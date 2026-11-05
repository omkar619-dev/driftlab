#!/usr/bin/env python3
"""Does Blnk's worker build another batch after its first one already worked, when
the transfer came through the hot lane?

processTransaction in cmd/workers.go (v0.15.7, L66-L69) runs the normal batch
attempt and then, for a task from the hot queue, the hot-lane attempt as well,
whose result replaces the first one's. So if the first batch applied the
transfer, the second builds another batch around it, fails on the transfer's own
reference, and the worker goes on to the single path, which refuses it as a
duplicate. Nothing is applied twice; the work is wasted.

Blnk sends a pair of balances to the hot lane only after 3 failures to get the
pair's balance locks, and only once the normal queue holds nothing more for the
pair. So this probe works on one pair, A to B, in three steps:

1. send as fast as Blnk answers until Redis says the pair is no longer normal,
   for at most --flood-seconds;
2. wait until the worker has applied everything and gone quiet;
3. send --rate transfers a second for --seconds; with the hot lane on, all of
   them should take it.

Then it reads the worker's log since step 3. A batch that fails on a reference
already used names the task it started from. A task's own transaction ID is in
Postgres only if the task applied its transfer itself, so a task whose ID is
there under the very reference its failed batch names had already applied that
transfer when it built the batch. It counts the batches that worked, and the
transfers applied one at a time, in Postgres. With the hot lane off, the same
run is the control: the pair stays normal.

    docker compose -f deploy/blnk.yaml -f deploy/blnk-hotlane.yaml up -d
    python3 scripts/blnk-probe-hotlane.py
"""
import argparse
import http.client
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from decimal import Decimal

API = "http://127.0.0.1:5001"
THREADS = 8
FUND = 1000000  # cents put into A: 10,000.00, far more than a run moves
# A batch that failed open because a reference was already used, as logrus writes it.
FAILED = re.compile(r"failed open.*?batch_size=(\d+).*?reference (\S+) has already been used.*?transaction_id=(\S+)")


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


def psql(sql):
    """Runs one query in Blnk's Postgres and returns its rows, columns split on |."""
    out = subprocess.run(["docker", "exec", "blnk-postgres", "psql", "-U", "postgres", "-d", "blnk",
                          "-At", "-c", sql], capture_output=True, text=True, check=True).stdout
    return [line.split("|") for line in out.splitlines() if line]


def redis_get(key):
    """Reads one key from Blnk's Redis, or '' when it isn't there."""
    return subprocess.run(["docker", "exec", "blnk-redis", "redis-cli", "--raw", "GET", key],
                          capture_output=True, text=True, check=True).stdout.strip()


def log_lines(container, since):
    """A container's log lines since a time or for a span (docker logs --since),
    standard output and standard error together."""
    result = subprocess.run(["docker", "logs", "--since", since, container],
                            capture_output=True, text=True, check=True)
    return (result.stdout + result.stderr).splitlines()


def pair_state(a, b):
    """The pair's state in Redis, and how many lock failures Blnk has counted for it.
    Blnk only writes these keys when the hot lane is on."""
    key = f"{a}|{b}|USD"
    return (redis_get(f"blnk:hot_pairs:state:{key}") or "normal",
            redis_get(f"blnk:hot_pairs:contention:{key}") or "0")


def setup():
    """Makes the ledger and balances A and B, and funds A from @World."""
    ledger = call("POST", "/ledgers", {"name": RUN})[1]["ledger_id"]
    a, b = (call("POST", "/balances", {"ledger_id": ledger, "currency": "USD", "precision": 100})[1]["balance_id"]
            for _ in range(2))
    call("POST", "/transactions", {
        "precise_amount": FUND, "precision": 100, "currency": "USD", "reference": f"{RUN}-fund",
        "description": "fund", "source": "@World", "destination": a, "allow_overdraft": True,
    })
    for _ in range(120):
        if psql(f"select 1 from blnk.transactions where reference like '{RUN}-fund%' and status = 'APPLIED'"):
            return a, b
        time.sleep(0.5)
    raise SystemExit("A wasn't funded within 60 s")


def transfer(a, b, reference):
    """Queues one 0.01 transfer from A to B and returns the HTTP status."""
    return call("POST", "/transactions", {
        "precise_amount": 1, "precision": 100, "currency": "USD", "reference": reference,
        "description": "probe", "source": a, "destination": b,
    })[0]


def flood(thread, a, b, stop, statuses, lock):
    """Step 1: sends from A to B as fast as Blnk answers, until told to stop."""
    n = 0
    while not stop.is_set():
        n += 1
        status = transfer(a, b, f"{RUN}-1-{thread}-{n}")
        with lock:
            statuses[status] += 1
        time.sleep(0.008)


def steady(thread, a, b, rate, seconds, start, statuses, lock):
    """Step 3: sends transfer number thread, thread + 8 and so on, each one when the
    schedule says, so the threads together send rate a second."""
    k = thread
    while k / rate < seconds:
        time.sleep(max(0.0, start + k / rate - time.monotonic()))
        status = transfer(a, b, f"{RUN}-3-{k}")
        with lock:
            statuses[status] += 1
        k += THREADS


def pending(a, b, lane):
    """Transfers from A to B saved as QUEUED in a lane and not applied yet: the count
    Blnk itself checks before it moves a pair to the hot lane."""
    return int(psql(
        f"select count(*) from blnk.transactions t where t.status = 'QUEUED' and t.source = '{a}' "
        f"and t.destination = '{b}' and coalesce(t.meta_data->>'queue_lane', 'normal') = '{lane}' "
        "and not exists (select 1 from blnk.transactions c where c.parent_transaction = t.transaction_id)")[0][0])


def saved(step):
    """How many of a step's transfers Blnk has saved under their own reference."""
    return int(psql(f"select count(*) from blnk.transactions where reference like '{RUN}-{step}-%' "
                    "and right(reference, 2) <> '_q'")[0][0])


def wait_idle(a, b, step, answered, limit=900):
    """Waits until Blnk has saved every transfer of the step it answered 201, none of
    the pair's transfers is waiting, and the worker's log has been quiet for 5 s."""
    started, shown = time.monotonic(), 0.0
    while time.monotonic() - started < limit:
        stored, normal, hot = saved(step), pending(a, b, "normal"), pending(a, b, "hot")
        quiet = not log_lines("blnk-worker", "5s")
        if stored >= answered and normal == hot == 0 and quiet:
            return time.monotonic() - started
        if time.monotonic() - shown >= 10:
            print(f"  {time.monotonic() - started:4.0f} s: saved {stored} of {answered}, waiting in the "
                  f"normal lane {normal}, in the hot lane {hot}, worker {'quiet' if quiet else 'busy'}")
            shown = time.monotonic()
        time.sleep(2)
    raise SystemExit(f"the worker didn't finish within {limit} s")


def applied_in(step):
    """How the worker applied a step's transfers: the size of each batch, and how many
    it applied one at a time. Its log can't tell: it says "Transaction ... processed
    successfully" whenever ProcessQueuedTransaction succeeds, which in v0.15.7 includes
    a batch that worked on its second try (cmd/workers.go L79 and L140), and in
    https://github.com/blnkfinance/blnk/pull/423 is every task that worked. Blnk's
    journal stamps every row with the time its database transaction began, and the
    connection that wrote it, so the transfers one batch applied share a stamp. A
    batch has at least 2 (transaction_coalescing.go, v0.15.7, L81), so a transfer with
    a stamp of its own was applied by the single path."""
    sizes = [int(row[0]) for row in psql(
        "select count(*) from blnk.transactions t join blnk.transaction_journal j "
        "on j.transaction_id = t.transaction_id and j.action_type = 'INSERT' "
        f"where t.reference like '{RUN}-{step}-%' and t.status = 'APPLIED' "
        "group by j.timestamp, j.backend_pid")]
    return sorted(n for n in sizes if n > 1), sizes.count(1)


def read_worker(since):
    """Reads the worker's log since step 3: the batches that failed on a step-3
    reference already used, and the step-3 transfers the single path refused as
    already used."""
    failed, refused = [], 0
    for line in log_lines("blnk-worker", since):
        if "failed open" in line and "already been used" in line:
            match = FAILED.search(line)
            if match and match.group(2).startswith(f"{RUN}-3-"):
                failed.append({"batch_size": int(match.group(1)), "reference": match.group(2),
                               "task": match.group(3)})
        elif 'msg="transaction validation failed"' in line and f"reference {RUN}-3-" in line:
            refused += 1
    return failed, refused


def own_rows(ids):
    """For each of these transaction IDs that Postgres has a row for: the row's
    reference, and how many transactions were written in the same database
    transaction, which for a batch is its size. Blnk's journal stamps every row
    with the time its database transaction began, and the connection that wrote it."""
    found, ids = {}, sorted(ids)
    for i in range(0, len(ids), 500):
        values = ",".join(f"('{x}')" for x in ids[i:i + 500])
        for task, reference, size in psql(
                "select v.id, t.reference, (select count(*) from blnk.transaction_journal k "
                "where k.action_type = 'INSERT' and k.timestamp = j.timestamp and k.backend_pid = j.backend_pid) "
                f"from (values {values}) as v(id) join blnk.transactions t on t.transaction_id = v.id "
                "join blnk.transaction_journal j on j.transaction_id = v.id and j.action_type = 'INSERT'"):
            found[task] = (reference, int(size))
    return found


def main():
    global RUN
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--rate", type=float, default=100, help="step 3: transfers a second")
    parser.add_argument("--seconds", type=float, default=10, help="step 3: how long to send")
    parser.add_argument("--flood-seconds", type=float, default=30, help="step 1: the longest it sends")
    args = parser.parse_args()

    RUN = f"hotlane-{int(time.time())}"
    if call("GET", "/health", timeout=2)[0] != 200:
        raise SystemExit("Blnk's /health doesn't answer 200; is deploy/blnk.yaml up?")
    a, b = setup()
    print(f"run {RUN}: A {a}, B {b}, A funded with {FUND} cents")
    lock = threading.Lock()

    statuses1, stop = Counter(), threading.Event()
    threads = [threading.Thread(target=flood, args=(i, a, b, stop, statuses1, lock)) for i in range(THREADS)]
    start = time.monotonic()
    for thread in threads:
        thread.start()
    state, failures = "normal", "0"
    while time.monotonic() - start < args.flood_seconds and state == "normal":
        time.sleep(0.5)
        state, failures = pair_state(a, b)
    stop.set()
    for thread in threads:
        thread.join()
    print(f"step 1: sent {sum(statuses1.values())} in {time.monotonic() - start:.1f} s, "
          f"{statuses1[201]} answered 201; pair {state}, lock failures counted {failures}")

    waited = wait_idle(a, b, 1, statuses1[201])
    state, failures = pair_state(a, b)
    print(f"step 2: all applied and the worker quiet after {waited:.0f} s; "
          f"pair {state}, lock failures counted {failures}")

    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))
    statuses3 = Counter()
    start = time.monotonic()
    threads = [threading.Thread(target=steady, args=(i, a, b, args.rate, args.seconds, start, statuses3, lock))
               for i in range(THREADS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print(f"step 3: sent {sum(statuses3.values())} in {time.monotonic() - start:.1f} s, "
          f"{statuses3[201]} answered 201")
    waited = wait_idle(a, b, 3, statuses3[201])
    state, failures = pair_state(a, b)
    print(f"  all applied and the worker quiet {waited:.0f} s later; pair {state}")

    lanes = dict(psql(f"select coalesce(meta_data->>'queue_lane', 'normal'), count(*) from blnk.transactions "
                      f"where reference like '{RUN}-3-%' and right(reference, 2) <> '_q' group by 1"))
    promoted = sum(1 for line in log_lines("blnk-server", since)
                   if "Promoted hot pair to hot queue" in line and a in line)
    worked, singles = applied_in(3)
    failed, refused = read_worker(since)
    per_task = Counter(entry["task"] for entry in failed)
    rows = own_rows(per_task)
    again = {task: row for task, row in rows.items()
             if any(entry["task"] == task and entry["reference"] == row[0] for entry in failed)}
    print("\nstep 3's transfers by lane: " + ", ".join(f"{lane} {n}" for lane, n in sorted(lanes.items())))
    print(f"'Promoted hot pair to hot queue' for this pair in the server's log: {promoted}")
    sizes = f", sizes {sorted(worked)}" if len(worked) <= 12 else ""
    print(f"the worker applied them in {len(worked)} batches ({sum(worked)} transactions{sizes}) "
          f"and {singles} one at a time")
    print(f"batches that failed on a step-3 reference already used: {len(failed)}, "
          f"started from {len(per_task)} tasks")
    print(f"  tasks that had applied their own transfer, then failed a batch on that transfer's "
          f"reference: {len(again)}")
    for task, (reference, size) in sorted(again.items(), key=lambda item: item[1][0]):
        print(f"    {reference}: applied in a batch of {size}, then failed batches of "
              f"{[entry['batch_size'] for entry in failed if entry['task'] == task]}")
    print(f"  failed batches per task, for the other tasks: "
          f"{dict(sorted(Counter(n for task, n in per_task.items() if task not in again).items()))}; "
          f"of those, tasks that applied their own transfer after a batch failed on another "
          f"transfer's reference: {len(rows) - len(again)}")
    print(f"step-3 transfers the single path then refused as already used: {refused}")

    applied = int(psql(f"select count(distinct regexp_replace(reference, '_q$', '')) from blnk.transactions "
                       f"where (reference like '{RUN}-1-%' or reference like '{RUN}-3-%') "
                       "and status = 'APPLIED'")[0][0])
    balances = dict(psql(f"select balance_id, balance from blnk.balances where balance_id in ('{a}', '{b}')"))
    right = Decimal(balances[b]) == applied and Decimal(balances[a]) == FUND - applied
    print(f"money: {applied} transfers applied; B {balances[b]} (must be {applied}), "
          f"A {balances[a]} (must be {FUND - applied}): {'right' if right else 'WRONG'}")

    os.makedirs("runs/blnk-probe", exist_ok=True)
    path = f"runs/blnk-probe/{RUN}.json"
    with open(path, "w") as out:
        json.dump({"run": RUN, "a": a, "b": b, "args": vars(args), "step1": dict(statuses1),
                   "step3": dict(statuses3), "lanes": lanes, "promoted": promoted, "worked": worked,
                   "singles": singles, "failed": failed, "own_rows": rows, "again": sorted(again),
                   "refused": refused,
                   "applied": applied, "balances": balances}, out)
    print(f"saved to {path}")


if __name__ == "__main__":
    main()
