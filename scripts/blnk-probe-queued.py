#!/usr/bin/env python3
"""Does Blnk keep every transaction it answered 201 QUEUED, when its API server stops?

Blnk's docs say it saves a queued transaction before it answers QUEUED (blnk-docs,
src/content/guides/concurrency.mdx L23). In v0.15.7 the API answers first, and a
background goroutine saves the transaction and queues it afterwards
(api/transactions.go L186-195, transaction_queue.go L121-131 and L157-184). A
shutdown waits for open HTTP requests only (cmd/server.go L322-336), then closes
the database pool (L412-420). This probe checks that from outside, changing
nothing in Blnk:

1. It makes a ledger with balances A and B and puts 10,000.00 into A.
2. 8 threads queue 0.01 transfers from A to B for 12 seconds, up to about 800 a
   second, each with a reference of its own.
3. Meanwhile it stops the API server and starts it again; --fault picks how.
4. Once the worker has caught up, it reads the transactions table straight from
   Postgres and sorts every transfer Blnk answered 201:
     applied  its queue copy, <reference>_q, was applied
     pending  it was saved but not applied yet, which queue recovery should fix
     lost     Blnk answered 201 QUEUED, but no row has its reference
   If any are pending, it waits for the 2 minutes queue recovery requires, asks
   Blnk to recover them, and sorts again.

--fault:
  stop      docker stop: a clean shutdown, the way a deploy restarts the server
  kill      docker kill: a crash
  pg-stall  Postgres freezes for 3 s, and 2 s in the server is stopped, the way an
            orchestrator restarts a server whose /health fails (Blnk's /health
            answers 503 while the database doesn't answer)

Run it on the machine that runs deploy/blnk.yaml, from the driftlab folder:

    python3 scripts/blnk-probe-queued.py --fault stop

It prints a summary and saves every request and the final rows to
runs/blnk-probe/<run>.json.
"""
import argparse
import http.client
import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.request

API = "http://127.0.0.1:5001"
THREADS = 8
PAUSE = 0.008     # seconds each thread waits after an answer
SEND_FOR = 12.0   # seconds of sending
FUND = 1000000    # cents put into A: 10,000.00, far more than the run moves


def call(method, path, body=None, timeout=5):
    """Sends one request to Blnk. Returns the HTTP status and the JSON answer, or
    None and the reason when no whole answer came back, as while the server is
    down or when it dies mid-answer."""
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
        status, _ = call("GET", "/health", timeout=2)
        if status == 200:
            return True
        time.sleep(0.5)
    return False


def setup():
    """Makes the ledger and balances A and B, and funds A from @World."""
    ledger = call("POST", "/ledgers", {"name": RUN})[1]["ledger_id"]
    a = call("POST", "/balances", {"ledger_id": ledger, "currency": "USD", "precision": 100})[1]["balance_id"]
    b = call("POST", "/balances", {"ledger_id": ledger, "currency": "USD", "precision": 100})[1]["balance_id"]
    call("POST", "/transactions", {
        "precise_amount": FUND, "precision": 100, "currency": "USD", "reference": f"{RUN}-fund",
        "description": "fund", "source": "@World", "destination": a, "allow_overdraft": True,
    })
    for _ in range(60):
        rows = psql(f"select status from blnk.transactions where reference = '{RUN}-fund_q'")
        if rows and rows[0][0] == "APPLIED":
            return ledger, a, b
        time.sleep(0.5)
    raise SystemExit("A was not funded within 30 s")


def sender(thread, a, b, records, lock, stop):
    """Queues 0.01 transfers from A to B until told to stop, recording each answer."""
    n = 0
    while not stop.is_set():
        n += 1
        reference = f"{RUN}-{thread}-{n}"
        sent = elapsed()
        status, body = call("POST", "/transactions", {
            "precise_amount": 1, "precision": 100, "currency": "USD", "reference": reference,
            "description": "probe", "source": a, "destination": b,
        })
        record = {"reference": reference, "http": status, "sent": round(sent, 3),
                  "answered": round(elapsed(), 3)}
        if isinstance(body, dict):
            record["status"] = body.get("status")
            record["id"] = body.get("transaction_id")
        else:
            record["error"] = body
        with lock:
            records.append(record)
        # A short pause keeps the 8 threads near 800 transfers a second, well under
        # what the machine's free ports allow, as every request opens a connection.
        time.sleep(0.05 if status is None else PAUSE)


def faults(kind):
    """Starts the fault's steps, each on a thread of its own, and returns the threads."""
    def stop_then_start(second, how):
        at(second)
        docker(*how)
        at(second + 2)
        docker("start", "blnk-server")

    def freeze_postgres():
        at(4.0)
        docker("pause", "blnk-postgres")
        at(7.0)
        docker("unpause", "blnk-postgres")

    if kind == "stop":
        steps = [lambda: stop_then_start(5.0, ["stop", "-t", "10", "blnk-server"])]
    elif kind == "kill":
        steps = [lambda: stop_then_start(5.0, ["kill", "blnk-server"])]
    else:
        steps = [freeze_postgres, lambda: stop_then_start(6.0, ["stop", "-t", "3", "blnk-server"])]
    threads = [threading.Thread(target=step) for step in steps]
    for thread in threads:
        thread.start()
    return threads


def rows_of_run():
    """Reads every transaction of this run from Postgres: the row Blnk saved when it
    queued a transfer, and the queue copy, <reference>_q, the worker applied."""
    saved, applied = {}, {}
    for reference, status in psql(
            f"select reference, status from blnk.transactions where reference like '{RUN}-%'"):
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
                         "and right(reference, 2) = '_q'")[0][0])
        steady = steady + 1 if count == last else 0
        if steady >= 3:
            return
        last = count
        time.sleep(2)


def sort_answers(records, saved, applied):
    """Sorts the transfers Blnk answered 201. Besides applied, pending and lost,
    applied-direct counts transfers applied under their own reference with no
    QUEUED row: when the background save fails, Blnk still queues the original
    (transaction_queue.go L173-182). other counts rows that ended neither
    applied nor QUEUED, such as rejected ones."""
    groups = {"applied": [], "applied-direct": [], "pending": [], "lost": [], "other": []}
    for record in records:
        if record["http"] != 201:
            continue
        reference = record["reference"]
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
    return groups


def report(title, records, saved, applied, a, b):
    """Prints the sorted answers and checks the money against the applied transfers."""
    groups = sort_answers(records, saved, applied)
    print(f"\n{title}")
    print(f"  of {sum(len(g) for g in groups.values())} transfers answered 201 QUEUED: "
          + ", ".join(f"{name} {len(group)}" for name, group in groups.items()))
    for record in groups["lost"][:10]:
        print(f"  lost: {record['reference']}, sent at {record['sent']} s, answered at {record['answered']} s")
    if groups["lost"]:
        answered = [record["answered"] for record in groups["lost"]]
        print(f"  the lost ones were answered between {min(answered)} s and {max(answered)} s")
    unanswered = [record for record in records if record["http"] != 201]
    landed = [record for record in unanswered if record["reference"] in saved or record["reference"] in applied]
    print(f"  {len(unanswered)} got no 201 (an error, or no answer while the server was down); "
          f"{len(landed)} of them were stored anyway")
    moved = sum(1 for reference in set(saved) | set(applied)
                if reference.startswith(f"{RUN}-") and not reference.endswith("-fund")
                and (applied.get(reference) == "APPLIED" or saved.get(reference) == "APPLIED"))
    balance_a = call("GET", f"/balances/{a}")[1]["balance"]
    balance_b = call("GET", f"/balances/{b}")[1]["balance"]
    print(f"  B = {balance_b} cents, and {moved} transfers of 1 cent were applied (must match); "
          f"A + B = {balance_a + balance_b} (must be {FUND})")
    return groups


def main():
    global RUN, T0
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--fault", choices=["stop", "kill", "pg-stall"], default="stop")
    fault = parser.parse_args().fault

    RUN = f"probe-{fault}-{int(time.time())}"
    if not wait_healthy(30):
        raise SystemExit("Blnk's /health doesn't answer 200; is deploy/blnk.yaml up?")
    ledger, a, b = setup()
    print(f"run {RUN}: ledger {ledger}, A {a}, B {b}, A funded with {FUND} cents")

    records, lock, stop = [], threading.Lock(), threading.Event()
    T0 = time.monotonic()
    started_at = time.time()
    senders = [threading.Thread(target=sender, args=(i, a, b, records, lock, stop)) for i in range(THREADS)]
    for thread in senders:
        thread.start()
    fault_threads = faults(fault)
    at(SEND_FOR)
    stop.set()
    for thread in senders + fault_threads:
        thread.join()
    print(f"{elapsed():6.2f} s  stopped sending: {len(records)} transfers sent, "
          f"{sum(1 for r in records if r['http'] == 201)} answered 201")
    if not wait_healthy(60):
        raise SystemExit("the server didn't come back within 60 s; see docker logs blnk-server")

    wait_for_worker()
    saved, applied = rows_of_run()
    groups = report("after the worker caught up:", records, saved, applied, a, b)

    if groups["pending"]:
        # Recovery only takes transactions queued at least 2 minutes ago, and the
        # last ones were queued 12 s in, so it waits until 140 s after the start.
        print("\nsome are pending: waiting until they are 2 minutes old, then asking for queue recovery")
        time.sleep(max(0.0, 140 - (time.time() - started_at)))
        status, body = call("POST", "/transactions/recover?threshold=2m", timeout=60)
        print(f"  POST /transactions/recover?threshold=2m: HTTP {status}, {body}")
        time.sleep(10)
        wait_for_worker()
        saved, applied = rows_of_run()
        report("after queue recovery:", records, saved, applied, a, b)

    logs = subprocess.run(["docker", "logs", "--since", str(int(started_at)), "blnk-server"],
                          capture_output=True, text=True)
    problems = [line for line in (logs.stdout + logs.stderr).splitlines()
                if "level=error" in line or "level=fatal" in line or "panic" in line]
    print(f"\nthe server logged {len(problems)} errors during the run; the last 5:")
    for line in problems[-5:]:
        print(f"  {line[:220]}")

    os.makedirs("runs/blnk-probe", exist_ok=True)
    path = f"runs/blnk-probe/{RUN}.json"
    with open(path, "w") as out:
        json.dump({"run": RUN, "fault": fault, "ledger": ledger, "a": a, "b": b, "records": records,
                   "saved": saved, "applied": applied}, out)
    print(f"saved every request and row to {path}")


if __name__ == "__main__":
    main()
