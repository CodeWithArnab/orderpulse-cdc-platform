CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('CREATED', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
    amount NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);

ALTER TABLE orders REPLICA IDENTITY FULL;
