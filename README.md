# Moonwell User Balance Snapshot Tool

This tool fetches user balances from Moonwell markets on Base blockchain and generates snapshots.

## Key Features

- Fetches account data using the `getAccountSnapshot` method for Moonwell markets
- Uses Multicall3 contract to batch RPC requests for 10-20x faster performance
- Graceful fallback to individual RPC calls when multicall fails
- Tracks supply positions (mToken balances) and underlying token values
- Properly identifies users with actual activity
- Concurrent processing with configurable concurrency limit to avoid RPC rate limits
- Configurable batch size for processing users in batches
- Periodic snapshots with configurable interval
- Stores data in PostgreSQL database
- Performance monitoring and ETA display

## Configuration

The following environment variables can be used to configure the tool:

- `RPC_URL`: URL of the Ethereum RPC endpoint (must be set before running)
- `PG_DSN`: PostgreSQL connection string (must be set before running)
- `FETCH_INTERVAL_HOURS`: Hours between snapshots (default: "2")
- `WORKER_CONCURRENCY`: Maximum number of concurrent RPC requests (default: 10)
- `BATCH_SIZE`: Number of users to process in each batch (default: 100)
- `USE_MULTICALL`: Whether to use multicall (default: "true", set to "false" to always use individual calls)

### Recommended Settings to Avoid Rate Limits

To prevent exceeding rate limits when making RPC calls to the Ethereum node, use these recommended settings:

1. **Concurrency Control**
   ```
   WORKER_CONCURRENCY=10
   ```
   This limits the number of concurrent token requests.

2. **Batch Size**
   ```
   BATCH_SIZE=100
   ```
   This controls how many users are processed in each Multicall batch.

3. **Snapshot Interval**
   ```
   FETCH_INTERVAL_HOURS=2
   ```
   This determines how frequently the application fetches data in hours.

4. **Multicall Control**
   ```
   USE_MULTICALL=true
   ```
   Enable multicall for faster performance. The system will automatically fall back to individual calls if multicall fails.

5. **Additional Recommendations**
   - Use a premium RPC provider like Alchemy or Infura with higher rate limits
   - The code implements strategic delays to avoid rate limits
   - Consider running during off-peak hours for better performance

## Performance Improvements

The latest version includes significant performance improvements:

- **Multicall Implementation**: Batches RPC calls to reduce the number of requests by 10-20x
- **Automatic Fallback**: Falls back to individual calls if multicall fails
- **Batch Database Operations**: Uses database transactions for faster data storage
- **Optimized Concurrency**: Default workers increased from 3 to 10
- **Improved Batch Size**: Default batch size increased from 10 to 100
- **Progress Tracking**: Shows ETA and processing rate in users/second
- **Reduced Delays**: Smarter rate limiting with shorter delays

These optimizations reduce processing time for 4000 users from 12+ hours to approximately 30-60 minutes (depending on your RPC provider limits).

## Troubleshooting

If you encounter the error `multicall failed: execution reverted`, it likely means:

1. Your RPC provider has an issue with the multicall contract
2. Rate limiting is being applied to your connection
3. The multicall contract on the Base chain may be experiencing issues

Solutions:
- The tool will automatically fall back to individual RPC calls
- Set `USE_MULTICALL=false` to disable multicall completely
- Try a different RPC provider
- Reduce batch size to 50 to reduce the load

## Project Structure

```
moonwell-snapshots/
├── cmd/
│   ├── main/                 # Main application
│   │   ├── main.go           # Main application logic
│   │   └── mtoken_bindings.go # Ethereum contract bindings
│   ├── db_setup/             # Database setup utility
│   │   └── main.go           # Creates tables and schema
│   ├── clear_and_reset/      # Reset database utility
│   │   └── main.go           # Clears data and adds user addresses
│   └── db_check/             # Database checking utility
│       └── main.go           # Verifies database integrity
├── data/
│   ├── tokens.json           # Token configuration
│   └── users.txt             # User addresses to track
├── abi/
│   ├── MToken.json           # ABI for Moonwell MToken contract
│   └── Multicall3.json       # ABI for Multicall3 contract
├── db/
│   └── 0001_init.sql         # SQL for initial schema
├── scripts/
│   ├── install.sh            # Dependency installation script
│   ├── db_setup.sql          # Database setup script
│   └── gen_bindings.sh       # Script to generate contract bindings
├── run.sh                    # Main application runner
├── reset_db.sh               # Database reset script
└── check_db.sh               # Database check script
```

## Data Files

- `data/tokens.json`: Contains information about Moonwell market tokens, including address and decimals
- `data/users.txt`: Contains user addresses to monitor, one per line

## Setup and Usage

### First-time Setup

1. Install dependencies using the installation script:

```bash
./scripts/install.sh
```

2. Set up the database:

```bash
./reset_db.sh
```

This will create the database schema and initialize the tables with users from `data/users.txt`.

3. Check the database to make sure everything is set up correctly:

```bash
./check_db.sh
```

### Running the Application

Run the main application to fetch and store user balances:

```bash
./run.sh
```

## Database Schema

The tool uses a PostgreSQL database with the following schema:

```sql
CREATE TABLE public.moonwell_user_balances (
    user_addr TEXT PRIMARY KEY,
    update_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Dynamic columns for each token from tokens.json
);
```

Each token has a dynamic column that stores an array with two values:
1. mToken balance
2. Underlying token balance

## How Multicall Works

Multicall is a smart contract that allows bundling multiple Ethereum read calls into a single request. This implementation:

1. Collects getAccountSnapshot calls for all users for a single token
2. Batches them into one RPC request
3. Processes all results in parallel
4. Significantly reduces the number of RPC calls needed

## Recent Improvements

- Implemented Multicall3 for 10-20x faster performance
- Increased default concurrency from 3 to 10
- Increased default batch size from 10 to 100
- Added batch database operations for faster storage
- Added ETA and performance statistics 
- Fixed bug that was incorrectly showing users as having borrowed when they hadn't
- Updated storage to only track mToken and underlying balances
