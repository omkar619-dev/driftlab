#!/usr/bin/env python3
"""Blnk answers POST /transactions with 201 QUEUED before the transaction is saved.
If the API server stops in between, the transaction is gone: no row, no error.

Start Blnk with the repo's docker-compose.yaml, then run:

    python3 repro.py                          # kills the container named "server"
    python3 repro.py --container blnk-server

It makes balances A and B, funds A, and for 10 seconds sends 0.01 transfers from
A to B from 8 threads, each with a reference of its own. 5 seconds in, it kills
the API server and starts it again 2 seconds later. Then it looks up every
transfer Blnk answered 201: GET /transactions/reference/<reference> answers 404
for each one that was never saved.
"""
import argparse
import http.client
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request

API = "http://127.0.0.1:5001"


def call(method, path, body=None):
    """Returns the HTTP status and the JSON answer, or None while the server is down."""
    request = urllib.request.Request(API + path, method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as err:
        return err.code, None
    except (OSError, http.client.HTTPException, ValueError):
        return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", default="server", help="the Blnk API server's container")
    container = parser.parse_args().container
    run = f"repro-{int(time.time())}"

    ledger = call("POST", "/ledgers", {"name": run})[1]["ledger_id"]
    a, b = (call("POST", "/balances", {"ledger_id": ledger, "currency": "USD", "precision": 100})[1]["balance_id"]
            for _ in range(2))
    call("POST", "/transactions", {"precise_amount": 1000000, "precision": 100, "currency": "USD",
                                   "reference": f"{run}-fund", "description": "fund",
                                   "source": "@World", "destination": a, "allow_overdraft": True})
    while call("GET", f"/balances/{a}")[1]["balance"] < 1000000:
        time.sleep(0.5)

    answered, lock, stop, start = [], threading.Lock(), threading.Event(), time.monotonic()

    def send(thread):
        n = 0
        while not stop.is_set():
            n += 1
            reference = f"{run}-{thread}-{n}"
            status, _ = call("POST", "/transactions", {
                "precise_amount": 1, "precision": 100, "currency": "USD", "reference": reference,
                "description": "repro", "source": a, "destination": b})
            if status == 201:
                with lock:
                    answered.append((reference, time.monotonic() - start))
            time.sleep(0.01 if status else 0.1)

    # Daemon threads, so a failed docker command ends the script instead of leaving it sending.
    threads = [threading.Thread(target=send, args=(i,), daemon=True) for i in range(8)]
    for thread in threads:
        thread.start()
    time.sleep(5)
    subprocess.run(["docker", "kill", container], check=True, capture_output=True)
    killed = time.monotonic() - start
    time.sleep(2)
    subprocess.run(["docker", "start", container], check=True, capture_output=True)
    time.sleep(3)
    stop.set()
    for thread in threads:
        thread.join()
    while call("GET", "/health")[0] != 200:
        time.sleep(0.5)
    time.sleep(5)

    lost = [(reference, at) for reference, at in answered
            if call("GET", f"/transactions/reference/{reference}")[0] == 404]
    print(f"Blnk answered 201 QUEUED to {len(answered)} transfers; the server was killed at {killed:.2f} s")
    print(f"never saved (GET /transactions/reference/<reference> is 404): {len(lost)}")
    if lost:
        print(f"those were answered between {min(at for _, at in lost):.2f} s and {max(at for _, at in lost):.2f} s")


main()
