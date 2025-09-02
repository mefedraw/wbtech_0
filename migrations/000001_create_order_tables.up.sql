CREATE TABLE orders
(
    order_uid          TEXT PRIMARY KEY,
    track_number       TEXT,
    locale             TEXT,
    internal_signature TEXT,
    customer_id        TEXT,
    delivery_service   TEXT,
    shardkey           TEXT,
    sm_id              INTEGER,
    date_created       TIMESTAMP,
    oof_shard          TEXT
);

CREATE TABLE DELIVERY
(
    id        SERIAL PRIMARY KEY,
    order_uid TEXT REFERENCES orders (order_uid) ON DELETE CASCADE,
    name      TEXT,
    phone     TEXT,
    zip       TEXT,
    city      TEXT,
    address   TEXT,
    region    TEXT,
    email     TEXT
);

CREATE TABLE PAYMENT
(
    id            SERIAL PRIMARY KEY,
    order_uid     TEXT REFERENCES orders (order_uid) ON DELETE CASCADE,
    transaction   TEXT,
    request_id    TEXT,
    currency      TEXT,
    provider      TEXT,
    amount        INTEGER,
    payment_dt    INTEGER,
    bank          TEXT,
    delivery_cost INTEGER,
    goods_total   INTEGER,
    custom_fee    TEXT
);

CREATE TABLE items
(
    id           UUID PRIMARY KEY,
    order_uid    TEXT REFERENCES orders (order_uid) ON DELETE CASCADE,
    chrt_id      BIGINT,
    track_number TEXT,
    price        INTEGER,
    rid          TEXT,
    name         TEXT,
    sale         INTEGER,
    size         TEXT,
    total_price  INTEGER,
    nm_id        BIGINT,
    brand        TEXT,
    status       INTEGER
);