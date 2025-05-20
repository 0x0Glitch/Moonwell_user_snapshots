-- Moonwell Database Setup Script

-- Create the moonwell database if it doesn't exist
-- Note: This command needs to be run as a PostgreSQL superuser
-- CREATE DATABASE moonwell;

-- Connect to the database
-- \c moonwell

-- Drop table if it exists to start fresh
DROP TABLE IF EXISTS public.moonwell_user_balances;

-- Create the main table for user balances
CREATE TABLE public.moonwell_user_balances (
    user_addr VARCHAR(42) PRIMARY KEY,
    update_time TIMESTAMP WITH TIME ZONE
);

-- Add token columns for each token in data/tokens.json
-- Each token gets a column with an array type that stores [mToken, borrow]
ALTER TABLE public.moonwell_user_balances ADD COLUMN "DAI" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "USDC" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "USDbC" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "WETH" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "cbETH" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "wstETH" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "rETH" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "weETH" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "AERO" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "cbBTC" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "EURC" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "wrsETH" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "WELL" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "USDS" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "tBTC" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "LBTC" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "VIRTUAL" NUMERIC[] DEFAULT ARRAY[0, 0];
ALTER TABLE public.moonwell_user_balances ADD COLUMN "MORPHO" NUMERIC[] DEFAULT ARRAY[0, 0];

-- Create index for faster queries
CREATE INDEX idx_user_addr ON public.moonwell_user_balances(user_addr);
CREATE INDEX idx_update_time ON public.moonwell_user_balances(update_time);

-- Grant permissions (if needed)
-- GRANT ALL PRIVILEGES ON TABLE public.moonwell_user_balances TO your_user;

-- Display the table structure
\d public.moonwell_user_balances; 