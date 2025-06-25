CREATE TABLE IF NOT EXISTS user_balances (
    user_addr   BYTEA       NOT NULL,
    update_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "GLMR" NUMERIC[] DEFAULT '{}'::NUMERIC[],
    "xcDOT" NUMERIC[] DEFAULT '{}'::NUMERIC[],
    "FRAX" NUMERIC[] DEFAULT '{}'::NUMERIC[],
    "xcUSDC" NUMERIC[] DEFAULT '{}'::NUMERIC[],
    "xcUSDT" NUMERIC[] DEFAULT '{}'::NUMERIC[],
    "ETH_wh" NUMERIC[] DEFAULT '{}'::NUMERIC[],
    "BTC_wh" NUMERIC[] DEFAULT '{}'::NUMERIC[],
    "USDC_wh" NUMERIC[] DEFAULT '{}'::NUMERIC[],
    PRIMARY KEY (user_addr)
);
