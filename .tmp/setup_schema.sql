CREATE TABLE IF NOT EXISTS option_chain_meta (
    conid BIGINT PRIMARY KEY,
    trade_date DATE NOT NULL,
    underlying_symbol TEXT NOT NULL,
    expiry TIMESTAMPTZ NOT NULL,
    strike NUMERIC(10, 2) NOT NULL,
    "right" TEXT NOT NULL,
    dte INT NOT NULL,
    delta DOUBLE PRECISION,
    bid NUMERIC(10, 2) NOT NULL,
    ask NUMERIC(10, 2) NOT NULL,
    mid NUMERIC(10, 2) NOT NULL,
    open_interest BIGINT NOT NULL,
    volume BIGINT NOT NULL,
    min_tick NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS bars1m_option (
    ts_end TIMESTAMPTZ NOT NULL,
    conid BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    expiry TIMESTAMPTZ NOT NULL,
    strike NUMERIC(10, 2) NOT NULL,
    "right" TEXT NOT NULL,
    bid NUMERIC(10, 2) NOT NULL,
    ask NUMERIC(10, 2) NOT NULL,
    mid NUMERIC(10, 2) NOT NULL,
    volume BIGINT,
    open_interest BIGINT,
    PRIMARY KEY (conid, ts_end)
);
