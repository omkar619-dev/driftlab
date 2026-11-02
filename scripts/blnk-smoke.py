#!/usr/bin/env python3
"""A first check of a fresh Blnk, by hand, before driftlab drives it.

It makes one ledger and two balances, A and B, moves 100.00 from @World into A
and then 30.00 from A to B, both through the queue, and prints what Blnk answers
at each step. At the end A, B and @World must add up to zero: a ledger moves
money, it never makes or loses any.

Run it on the machine that runs deploy/blnk.yaml:

    python3 scripts/blnk-smoke.py
"""
import json
import sys
import time
import urllib.error
import urllib.request

API = "http://127.0.0.1:5001"
RUN = f"smoke-{int(time.time())}"  # makes every reference unique to this run


def call(method, path, body=None):
    """Sends one request to Blnk and returns the HTTP status and the JSON answer."""
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(API + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"null")
    except urllib.error.URLError as err:
        sys.exit(f"{method} {path}: no answer from {API}: {err.reason}")


def expect(status, body, want, what):
    """Stops the script when Blnk answers something other than the expected status."""
    if status != want:
        sys.exit(f"{what}: expected HTTP {want}, got {status}: {body}")
    return body


def queue_transfer(name, source, destination, cents, overdraft=False):
    """Queues one transfer of cents/100 USD and prints Blnk's first answer."""
    reference = f"{RUN}-{name}"
    status, body = call("POST", "/transactions", {
        "precise_amount": cents,
        "precision": 100,
        "currency": "USD",
        "reference": reference,
        "description": name,
        "source": source,
        "destination": destination,
        "allow_overdraft": overdraft,
    })
    expect(status, body, 201, f"queue {name}")
    print(f"queued {name}: HTTP {status}, status {body['status']}, id {body['transaction_id']}")
    return reference


def wait_for(reference):
    """Waits up to 15 s for the worker, then prints the record under the reference
    and the queue copy the worker applies, whose reference ends in _q."""
    for _ in range(30):
        copy_status, copy = call("GET", f"/transactions/reference/{reference}_q")
        if copy_status == 200 and copy.get("status") in ("APPLIED", "REJECTED"):
            break
        time.sleep(0.5)
    status, original = call("GET", f"/transactions/reference/{reference}")
    print(f"  {reference}: HTTP {status}, status {original.get('status') if status == 200 else original}")
    print(f"  {reference}_q: HTTP {copy_status}, status {copy.get('status') if copy_status == 200 else copy}")


def show(name, path):
    """Prints one balance and returns its amount in cents."""
    status, body = call("GET", path)
    expect(status, body, 200, f"read {name}")
    print(f"{name}: balance {body['balance']}, credits {body['credit_balance']}, "
          f"debits {body['debit_balance']}, version {body.get('version')}")
    return int(body["balance"])


def amount(path):
    """Returns a balance in cents, or 0 when it doesn't exist yet."""
    status, body = call("GET", path)
    return int(body["balance"]) if status == 200 else 0


# @World is where money enters the ledger from outside, so it may go below zero.
# Blnk keeps one per currency for every ledger and run, so this run's check uses
# how much it changed.
WORLD = "/balances/indicator/@World/currency/USD"

status, body = call("GET", "/health")
print(f"health: HTTP {status}, {body}")
world_before = amount(WORLD)

ledger = expect(*call("POST", "/ledgers", {"name": RUN}), 201, "create ledger")["ledger_id"]
a = expect(*call("POST", "/balances", {"ledger_id": ledger, "currency": "USD", "precision": 100}),
           201, "create balance A")["balance_id"]
b = expect(*call("POST", "/balances", {"ledger_id": ledger, "currency": "USD", "precision": 100}),
           201, "create balance B")["balance_id"]
print(f"ledger {ledger}, A {a}, B {b}")

fund = queue_transfer("fund", "@World", a, 10000, overdraft=True)
wait_for(fund)
move = queue_transfer("move", a, b, 3000)
wait_for(move)

total = show("A", f"/balances/{a}") + show("B", f"/balances/{b}") + show("@World", WORLD) - world_before
print(f"A + B + this run's change in @World = {total} (must be 0)")
