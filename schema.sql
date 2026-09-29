CREATE TABLE IF NOT EXISTS orders (
    event_id   UUID PRIMARY KEY,
    order_id   TEXT NOT NULL,
    user_id    TEXT NOT NULL,
    item       TEXT NOT NULL,
    quantity   INTEGER NOT NULL CHECK (quantity > 0),
    created_at TIMESTAMPTZ NOT NULL
);
