-- Read-side projection tables

create table if not exists account_summary (
    account_id  uuid        primary key,
    owner_id    uuid        not null,
    status      varchar(20) not null,
    currency    varchar(3)  not null,
    created_at  timestamptz not null,
    updated_at  timestamptz not null
);

create table if not exists account_balances (
    account_id    uuid           primary key,
    balance       numeric(20, 6) not null default 0,
    currency      varchar(3)     not null,
    last_event_id uuid           null,
    updated_at    timestamptz    not null
);

create table if not exists transaction_history (
    id           uuid           primary key,
    account_id   uuid           not null,
    amount       numeric(20, 6) not null,
    direction    varchar(6)     not null check (direction in ('CREDIT', 'DEBIT')),
    counterpart  uuid           null,
    reference    varchar(255)   null,
    occurred_at  timestamptz    not null
);

create table if not exists event_log (
    id           uuid        primary key,
    account_id   uuid        not null,
    event_type   varchar(100) not null,
    payload      jsonb       not null,
    occurred_at  timestamptz not null
);

create index if not exists idx_transaction_history_account on transaction_history (account_id, occurred_at desc);
create index if not exists idx_event_log_account           on event_log (account_id, occurred_at asc);
create index if not exists idx_account_summary_owner       on account_summary (owner_id);

