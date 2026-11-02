#!/usr/bin/env python3
"""How far behind its answers does Blnk's background save fall, and what does a
crash lose at a given load?

The first probe, blnk-probe-queued.py, sent as fast as Blnk answered between two
balances, and every crash lost thousands of transfers Blnk had answered 201
QUEUED: the API answered about 850 a second and saved about 350. This one sends
at a fixed rate between many balances, and measures the gap as it goes: every
half second it counts the transfers answered 201 and the rows Blnk has saved for
them. The difference is what a crash at that moment would lose.

    python3 scripts/blnk-probe-rate.py --rate 300 --fault none
    python3 scripts/blnk-probe-rate.py --rate 300 --fault kill

--fault kill kills the API server halfway through and starts it again 2 seconds
later. At the end it sorts every transfer answered 201 the way the first probe
does, checks every balance against the transfers applied to it, and saves
everything to runs/blnk-probe/<run>.json.
"""
import argparse
import http.client
import json
import os
import random
import subprocess
import threading
import time
import urllib.error
import urllib.request
from decimal import Decimal

API = "http://127.0.0.1:5001"
THREADS = 8
FUND = 1000000  # cents put into each balance: 10,000.00, far more than a run moves


def call(method, path, body=None, timeout=5):
    """Sends one request to Blnk. Returns the HTTP status and the JSON answer, or
    None and the reason when no whole answer came back."""
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(API + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as err:
        status, raw = err.code, err.read()
    except (OSError, http.client.HTTPException) as err:
        return None, str(getattr(err, "reason", err))
    try:
        return status, json.loads(raw or b"null")
    except ValueError:
        return status, raw.decode(errors="replace")


def elapsed():
    """Seconds since the sending began."""
    return time.monotonic() - T0


def at(second):
    """Sleeps until the given second after the sending began."""
    time.sleep(max(0.0, second - elapsed()))


def docker(*args):
    """Runs one docker command and prints when it started and how it ended."""
    started = elapsed()
    result = subprocess.run(["docker", *args], capture_output=True, text=True)
    print(f"{started:6.2f} s  docker {' '.join(args)}: exit {result.returncode} {result.stderr.strip()}")


def psql(sql):
    """Runs one query in Blnk's Postgres and returns its rows, columns split on |."""
    out = subprocess.run(["docker", "exec", "blnk-postgres", "psql", "-U", "postgres", "-d", "blnk",
                          "-At", "-c", sql], capture_output=True, text=True, check=True).stdout
    return [line.split("|") for line in out.splitlines() if line]


def wait_healthy(limit):
    """Waits up to limit seconds for /health to answer 200, and says whether it did."""
    started = time.monotonic()
    while time.monotonic() - started < limit:
        if call("GET", "/health", timeout=2)[0] == 200:
            return True
        time.sleep(0.5)
    return False


def setup(count):
    """Makes the ledger and count balances, and funds each one from @World."""
    ledger = call("POST", "/ledgers", {"name": RUN})[1]["ledger_id"]
    balances = [call("POST", "/balances", {"ledger_id": ledger, "currency": "USD", "precision": 100})[1]["balance_id"]
                for _ in range(count)]
    for i, balance in enumerate(balances):
        call("POST", "/transactions", {
            "precise_amount": FUND, "precision": 100, "currency": "USD", "reference": f"{RUN}-fund-{i}",
            "description": "fund", "source": "@World", "destination": balance, "allow_overdraft": True,
        })
    funded = 0
    for _ in range(120):
        funded = int(psql(f"select count(*) from blnk.transactions where reference like '{RUN}-fund-%' "
                          "and right(reference, 2) = '_q' and status = 'APPLIED'")[0][0])
        if funded == count:
            return ledger, balances
        time.sleep(0.5)
    raise SystemExit(f"only {funded} of {count} balances were funded within 60 s")


def sender(thread, balances, rate, seconds, records, lock):
    """Sends transfers number thread, thread + 8, thread + 16 and so on, each one
    when the schedule says, so the threads together send rate a second. Each moves
    1 cent between two balances picked at random."""
    picker = random.Random(thread)
    k = thread
    while k / rate < seconds:
        at(k / rate)
        source, destination = picker.sample(range(len(balances)), 2)
        reference = f"{RUN}-{k}"
        sent = elapsed()
        status, body = call("POST", "/transactions", {
            "precise_amount": 1, "precision": 100, "currency": "USD", "reference": reference,
            "description": "probe", "source": balances[source], "destination": balances[destination],
        })
        record = {"reference": reference, "source": source, "destination": destination,
                  "http": status, "sent": round(sent, 3), "answered": round(elapsed(), 3)}
        if isinstance(body, dict):
            record["status"] = body.get("status")
        else:
            record["error"] = body
        with lock:
            records.append(record)
        k += THREADS


def saved_count():
    """How many of the run's transfers Blnk has saved: rows under their own
    reference, not the funding and not the queue copies."""
    return int(psql(f"select count(*) from blnk.transactions where reference like '{RUN}-%' "
                    f"and reference not like '{RUN}-fund-%' and right(reference, 2) <> '_q'")[0][0])


def monitor(records, lock, timeline, stop):
    """Every half second, notes how many transfers were answered 201 and how many
    Blnk has saved. The answers are counted first, so a save that lands between
    the two counts can make the gap a little smaller than it was."""
    while not stop.is_set():
        with lock:
            answered = sum(1 for record in records if record["http"] == 201)
        timeline.append((round(elapsed(), 2), answered, saved_count()))
        stop.wait(0.5)


def kill_halfway(seconds):
    """Kills the API server halfway through the sending and starts it 2 s later."""
    at(seconds / 2)
    docker("kill", "blnk-server")
    at(seconds / 2 + 2)
    docker("start", "blnk-server")


def rows_of_run():
    """Reads the run's transfers from Postgres: the row Blnk saved when it queued a
    transfer, and the queue copy, <reference>_q, the worker applied."""
    saved, applied = {}, {}
    for reference, status in psql(
            f"select reference, status from blnk.transactions where reference like '{RUN}-%' "
            f"and reference not like '{RUN}-fund-%'"):
        if reference.endswith("_q"):
            applied[reference[:-2]] = status
        else:
            saved[reference] = status
    return saved, applied


def wait_for_worker():
    """Waits until the number of queue copies has stopped growing for 6 s, at most 3 min."""
    last, steady, started = -1, 0, time.monotonic()
    while time.monotonic() - started < 180:
        count = int(psql(f"select count(*) from blnk.transactions where reference like '{RUN}-%' "
                         f"and reference not like '{RUN}-fund-%' and right(reference, 2) = '_q'")[0][0])
        steady = steady + 1 if count == last else 0
        if steady >= 3:
            return
        last = count
        time.sleep(2)


def report(title, records, saved, applied, balances):
    """Sorts the transfers answered 201 as the first probe does, and checks every
    balance against the transfers applied to it."""
    groups = {"applied": [], "applied-direct": [], "pending": [], "lost": [], "other": []}
    expected = [FUND] * len(balances)
    for record in records:
        reference = record["reference"]
        done = applied.get(reference) == "APPLIED" or saved.get(reference) == "APPLIED"
        if done:
            expected[record["source"]] -= 1
            expected[record["destination"]] += 1
        if record["http"] != 201:
            continue
        if applied.get(reference) == "APPLIED":
            groups["applied"].append(record)
        elif saved.get(reference) == "APPLIED":
            groups["applied-direct"].append(record)
        elif reference in applied:
            groups["other"].append(record)
        elif saved.get(reference) == "QUEUED":
            groups["pending"].append(record)
        elif reference in saved:
            groups["other"].append(record)
        else:
            groups["lost"].append(record)
    print(f"\n{title}")
    print(f"  of {sum(len(g) for g in groups.values())} transfers answered 201 QUEUED: "
          + ", ".join(f"{name} {len(group)}" for name, group in groups.items()))
    if groups["lost"]:
        answered = [record["answered"] for record in groups["lost"]]
        print(f"  the lost ones were answered between {min(answered)} s and {max(answered)} s")
    unanswered = [record for record in records if record["http"] != 201]
    landed = sum(1 for record in unanswered if record["reference"] in saved or record["reference"] in applied)
    print(f"  {len(unanswered)} got no 201; {landed} of them were stored anyway")
    ids = "', '".join(balances)
    actual = dict(psql(f"select balance_id, balance from blnk.balances where balance_id in ('{ids}')"))
    wrong = [i for i, balance in enumerate(balances) if Decimal(actual.get(balance, "NaN")) != expected[i]]
    total = sum(Decimal(actual[balance]) for balance in balances)
    print(f"  balances that differ from the transfers applied to them: {len(wrong)} of {len(balances)}; "
          f"all together {total} (must be {FUND * len(balances)})")
    for i in wrong[:5]:
        print(f"    balance {i}: {actual.get(balances[i])}, its applied transfers say {expected[i]}")
    return groups


def show_timeline(timeline, records, seconds, fault):
    """Prints how far the saves were behind the answers, every 2 s, and the worst."""
    print("  time    answered 201    saved    behind")
    for t, answered, saved in timeline[::4]:
        print(f"  {t:5.1f} s  {answered:12d}  {saved:7d}  {answered - saved:8d}")
    worst = max(timeline, key=lambda point: point[1] - point[2])
    print(f"  most behind: {worst[1] - worst[2]} transfers, at {worst[0]} s")
    if fault == "kill":
        before = [point for point in timeline if point[0] <= seconds / 2]
        if before:
            print(f"  behind at the last count before the kill, {before[-1][0]} s: {before[-1][1] - before[-1][2]}")
    waits = sorted(record["answered"] - record["sent"] for record in records if record["http"] == 201)
    if waits:
        print(f"  time to answer 201: median {waits[len(waits) // 2] * 1000:.0f} ms, "
              f"99th percentile {waits[int(len(waits) * 0.99)] * 1000:.0f} ms, slowest {waits[-1] * 1000:.0f} ms")


def main():
    global RUN, T0
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--rate", type=float, required=True, help="transfers a second, all threads together")
    parser.add_argument("--balances", type=int, default=20, help="balances the transfers move between")
    parser.add_argument("--seconds", type=float, default=20.0, help="how long to send")
    parser.add_argument("--fault", choices=["none", "kill"], default="none")
    args = parser.parse_args()

    RUN = f"rate-{args.fault}-{int(args.rate)}-{int(time.time())}"
    if not wait_healthy(30):
        raise SystemExit("Blnk's /health doesn't answer 200; is deploy/blnk.yaml up?")
    ledger, balances = setup(args.balances)
    print(f"run {RUN}: {args.balances} balances with {FUND} cents each; "
          f"sending {args.rate:g} a second for {args.seconds:g} s, fault {args.fault}")

    records, lock, timeline, stop = [], threading.Lock(), [], threading.Event()
    T0 = time.monotonic()
    senders = [threading.Thread(target=sender, args=(i, balances, args.rate, args.seconds, records, lock))
               for i in range(THREADS)]
    watcher = threading.Thread(target=monitor, args=(records, lock, timeline, stop))
    others = [threading.Thread(target=kill_halfway, args=(args.seconds,))] if args.fault == "kill" else []
    for thread in senders + others + [watcher]:
        thread.start()
    for thread in senders + others:
        thread.join()
    sent = len(records)
    print(f"{elapsed():6.2f} s  stopped sending: {sent} sent, {sent / args.seconds:.0f} a second, "
          f"{sum(1 for record in records if record['http'] == 201)} answered 201")
    # Keep counting until the saves catch up, or for 30 s when a crash means they never will.
    while elapsed() < args.seconds + 30 and (not timeline or timeline[-1][2] < timeline[-1][1]):
        time.sleep(0.5)
    stop.set()
    watcher.join()
    show_timeline(timeline, records, args.seconds, args.fault)

    if not wait_healthy(60):
        raise SystemExit("the server didn't come back within 60 s; see docker logs blnk-server")
    wait_for_worker()
    saved, applied = rows_of_run()
    groups = report("after the worker caught up:", records, saved, applied, balances)
    if groups["pending"]:
        print("\nsome are pending: waiting until they are 2 minutes old, then asking for queue recovery")
        time.sleep(max(0.0, args.seconds + 125 - elapsed()))
        status, body = call("POST", "/transactions/recover?threshold=2m", timeout=60)
        print(f"  POST /transactions/recover?threshold=2m: HTTP {status}, {body}")
        time.sleep(10)
        wait_for_worker()
        saved, applied = rows_of_run()
        report("after queue recovery:", records, saved, applied, balances)

    os.makedirs("runs/blnk-probe", exist_ok=True)
    path = f"runs/blnk-probe/{RUN}.json"
    with open(path, "w") as out:
        json.dump({"run": RUN, "rate": args.rate, "seconds": args.seconds, "fault": args.fault,
                   "ledger": ledger, "balances": balances, "records": records, "timeline": timeline,
                   "saved": saved, "applied": applied}, out)
    print(f"saved every request, count and row to {path}")


if __name__ == "__main__":
    main()
