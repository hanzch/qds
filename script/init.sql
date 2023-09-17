CREATE DATABASE quant;

CREATE TABLE quant.codes
(
    `code` String,
    `name` String,
    `sdt` String,
    `status` Bool,
    `edt` Nullable(String)
)
ENGINE = ReplacingMergeTree
PRIMARY KEY code
ORDER BY code
SETTINGS index_granularity = 8192;

CREATE TABLE quant.ts_day
(
    `date` Date,
    `code` String,
    `open` Decimal(9, 2),
    `high` Decimal(9, 2),
    `low` Decimal(9, 2),
    `close` Decimal(9, 2),
    `volume` UInt64,
    `amount` UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY code
PRIMARY KEY date
ORDER BY (date, code)
SETTINGS index_granularity = 8192;

CREATE TABLE quant.ts_min
(
    `date` DateTime,
    `code` String,
    `open` Decimal(9, 2),
    `high` Decimal(9, 2),
    `low` Decimal(9, 2),
    `close` Decimal(9, 2),
    `volume` Decimal(18, 2),
    `amount` Decimal(18, 2)
)
ENGINE = ReplacingMergeTree
PARTITION BY code
PRIMARY KEY date
ORDER BY (date, code)
SETTINGS index_granularity = 8192;


CREATE TABLE quant.ts_adj
(

    `date` Date,
    `code` String,
    `num` Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY code
PRIMARY KEY date
ORDER BY (date, code)
SETTINGS index_granularity = 8192;


CREATE TABLE quant.users
(
    `username` String,
    `password` String,
    `name` String,
    `email` String,
    `disabled` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
PRIMARY KEY username
ORDER BY username
SETTINGS index_granularity = 8192;
