#!/usr/bin/env python3
"""Sorts a probe run's transfers again against Blnk's database as it is now.

The probe sorted the transfers Blnk answered 201 QUEUED a few minutes after each
run. This sorts them again from the probe's saved file, to see whether any lost
one turned up after all and whether the pending ones were applied since. For
each group it prints when its transfers were answered, and for the pg-stall run
how many were answered while Postgres was frozen.

    python3 scripts/blnk-probe-recheck.py runs/blnk-probe/probe-*.json
"""
import json
import subprocess
import sys
from decimal import Decimal


def psql(sql):
    """Runs one query in Blnk's Postgres and returns its rows, columns split on |."""
    out = subprocess.run(["docker", "exec", "blnk-postgres", "psql", "-U", "postgres", "-d", "blnk",
                          "-At", "-c", sql], capture_output=True, text=True, check=True).stdout
    return [line.split("|") for line in out.splitlines() if line]


def recheck(path):
    """Prints one run's groups as the database stands now."""
    with open(path) as file:
        run = json.load(file)
    name = run["run"]
    saved, applied = {}, {}
    for reference, status in psql(
            f"select reference, status from blnk.transactions where reference like '{name}-%'"):
        if reference.endswith("_q"):
            applied[reference[:-2]] = status
        else:
            saved[reference] = status

    groups = {"applied": [], "applied-direct": [], "pending": [], "lost": [], "other": []}
    for record in run["records"]:
        if record["http"] != 201:
            continue
        reference = record["reference"]
        if applied.get(reference) == "APPLIED":
            group = "applied"
        elif saved.get(reference) == "APPLIED":
            group = "applied-direct"
        elif reference in applied:
            group = "other"
        elif saved.get(reference) == "QUEUED":
            group = "pending"
        elif reference in saved:
            group = "other"
        else:
            group = "lost"
        groups[group].append(record)

    print(f"{name}: of {sum(len(g) for g in groups.values())} answered 201 QUEUED, now "
          + ", ".join(f"{group} {len(records)}" for group, records in groups.items()))
    for group, records in groups.items():
        if records:
            answered = [record["answered"] for record in records]
            print(f"  {group}: answered between {min(answered)} s and {max(answered)} s")
    if run["fault"] == "pg-stall":
        frozen = sum(1 for record in run["records"]
                     if record["http"] == 201 and 4.0 <= record["answered"] <= 7.0)
        print(f"  answered 201 while Postgres was frozen, 4.0 to 7.0 s: {frozen}")

    known = {record["reference"] for record in run["records"]} | {f"{name}-fund"}
    strangers = [reference for reference in set(saved) | set(applied) if reference not in known]
    balances = dict(psql(f"select balance_id, balance from blnk.balances "
                         f"where balance_id in ('{run['a']}', '{run['b']}')"))
    moved = sum(1 for record in run["records"] if applied.get(record["reference"]) == "APPLIED"
                or saved.get(record["reference"]) == "APPLIED")
    print(f"  B = {balances.get(run['b'])} cents, {moved} transfers of 1 cent applied (must match); "
          f"A + B = {Decimal(balances[run['a']]) + Decimal(balances[run['b']])}; "
          f"rows no request of the run explains: {len(strangers)}")


for path in sys.argv[1:]:
    recheck(path)
if len(sys.argv) < 2:
    sys.exit("give it the probe's files: runs/blnk-probe/probe-*.json")
