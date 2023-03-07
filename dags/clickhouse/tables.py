CH_CREATE_ORDER_TABLE = """CREATE TABLE IF NOT EXISTS orders_datamart(
    `order_id` UInt16,
    `customer_id` UInt16,
    `order_total_usd` String,
    `make` String,
    `model` String,
    `delivery_city` String,
    `delivery_company` String,
    `delivery_address` String,
    `created_at` Datetime('UTC'),
    `customer_name` String,
    `timestamp` Datetime('UTC') DEFAULT NOW()
)
ENGINE = MergeTree
ORDER BY (order_id, created_at, timestamp)
"""
