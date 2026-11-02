#!/usr/bin/env bash
# Why Blnk's balance UPDATE blocks transaction INSERTs, in plain Postgres 16.
#
# A throwaway container with two tables shaped like blnk.balances (a unique
# index on (indicator, currency)) and blnk.transactions (a foreign key to a
# balance). Each try holds an UPDATE of the balance open for 3 s and, 1 s in,
# inserts a transaction that points at it, waiting at most 1 s for the row lock.
set -u
name=pg-lockdemo

docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=demo postgres:16 >/dev/null || exit 1
trap 'docker stop "$name" >/dev/null' EXIT
# The image restarts Postgres once after its first-time setup; wait past that.
until docker logs "$name" 2>&1 | grep -q "PostgreSQL init process complete"; do sleep 1; done
until docker exec "$name" pg_isready -U postgres -q; do sleep 1; done

docker exec -i "$name" psql -U postgres -q -v ON_ERROR_STOP=1 <<'SQL'
create table balances (
    balance_id text primary key,
    indicator  text,
    currency   text,
    balance    numeric,
    unique (indicator, currency)
);
create table transactions (
    id     serial primary key,
    source text references balances (balance_id)
);
insert into balances values ('bln_1', null, 'USD', 0);
create function keep_new() returns trigger language plpgsql as $$ begin return new; end $$;
SQL

try() {
    docker exec "$name" psql -U postgres -q -c \
        "begin; update balances set $2 where balance_id = 'bln_1'; select pg_sleep(3); rollback;" >/dev/null &
    sleep 1
    if docker exec "$name" psql -U postgres -q -v ON_ERROR_STOP=1 -c \
        "set lock_timeout = '1s'; insert into transactions (source) values ('bln_1');" >/dev/null 2>&1; then
        result="insert went through"
    else
        result="insert waited, lock timeout"
    fi
    wait
    printf '%-22s %-48s %s\n' "$1" "set $2" "$result"
}

try "no trigger" "balance = balance + 1"
try "no trigger" "balance = balance + 1, currency = currency"
docker exec "$name" psql -U postgres -q -c \
    "create trigger keep_new before update on balances for each row execute function keep_new();"
try "BEFORE UPDATE trigger" "balance = balance + 1"
try "BEFORE UPDATE trigger" "balance = balance + 1, currency = currency"
