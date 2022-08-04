CREATE_BFRO_TABLE = """CREATE TABLE IF NOT EXISTS BFRO(
    `YEAR` UInt8,
    `SEASON` String,
    `STATE` String,
    `COUNTY` String,
    `NEAREST_TOWN` String,
    `NEAREST_ROAD` String,
    `REPORT_NUMBER` UInt16,
    `REPORT_CLASS` String,
    `MONTH` String,
    `DATE` String,
    `timestamp` Datetime('UTC') DEFAULT NOW()
)
ENGINE = MergeTree
ORDER BY (REPORT_NUMBER, timestamp)
"""
