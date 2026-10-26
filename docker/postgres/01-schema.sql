CREATE TABLE IF NOT EXISTS orders
(
    order_number   UUID         PRIMARY KEY,
    customer_id    UUID         NOT NULL,
    status         VARCHAR(32)  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items
(
    id           UUID           PRIMARY KEY,
    order_number UUID           NOT NULL REFERENCES orders (order_number) ON DELETE CASCADE,
    item_name    VARCHAR(255)   NOT NULL,
    quantity     INT            NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0)
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_number ON order_items (order_number);
