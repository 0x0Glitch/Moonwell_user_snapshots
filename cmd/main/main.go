package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/lib/pq"
)

// Token represents a Moonwell market token
type Token struct {
        Symbol     string        `json:"symbol"`
        MTokenAddr string        `json:"mTokenAddr"`
        Decimals   int           `json:"decimals"`
        Contract   *MTokenCaller // Not part of JSON
}

// Snapshot represents a user's balance for a specific token
type Snapshot struct {
        User              common.Address
        MTokenBalance     *big.Int
        BorrowBalance     *big.Int // Kept for activity detection, but not stored in DB or displayed
        UnderlyingBalance *big.Int
}

// Global variables
var (
        // Configuration
        ethRPC           string // BASE RPC URL
        pgConnStr        string
        snapshotInterval time.Duration

        // Rate limiting configuration optimized for 30 req/sec
        maxConcurrency = 5                         
        batchSize      = 50                       // Keep the batch size at 100 for database efficiency
        subBatchSizes  = []int{10, 10, 10, 10, 10} // Five sub-batches of 20 each
        tokenDelay     = 1000 * time.Millisecond  
        subBatchDelay  = 1 * time.Second          

        // Database connection
        db *sql.DB

        // Logging
        errorLogger *log.Logger
        infoLogger  *log.Logger
        logFile     *os.File

        // Context for operations
        rootCtx    context.Context
        cancelFunc context.CancelFunc

        // Ethereum client
        client *ethclient.Client

        // User snapshots and mutex
        userSnapshots      map[string]map[string]map[string]string
        userSnapshotsMutex sync.Mutex

        // Stats tracking - removed unused variables

        // Checkpoint variables
        lastProcessedIndex int
        checkpointFile     = "data/checkpoint.txt"

        // Multi3 contract address removed - not used in current implementation

        // SQL queries for user updates
        createActiveUsersSQL = `
                BEGIN;

DROP TABLE IF EXISTS user_metrics.active_users CASCADE;

CREATE TABLE user_metrics.active_users AS
SELECT *
FROM   user_metrics.user_metrics
WHERE
      COALESCE( (dai      ->> 0)::numeric , 0) > 470459000000::numeric
   OR COALESCE( (usdc     ->> 0)::numeric , 0) > 465000000000::numeric
   OR COALESCE( (us_db_c  ->> 0)::numeric , 0) > 454000000000::numeric
   OR COALESCE( (weth     ->> 0)::numeric , 0) >    308824000::numeric
   OR COALESCE( (cb_eth   ->> 0)::numeric , 0) >    287455000::numeric
   OR COALESCE( (wst_eth  ->> 0)::numeric , 0) >    262360000::numeric
   OR COALESCE( (r_eth    ->> 0)::numeric , 0) >    279797000::numeric
   OR COALESCE( (we_eth   ->> 0)::numeric , 0) >    295808000::numeric
   OR COALESCE( (aero     ->> 0)::numeric , 0) > 1094127974000::numeric
   OR COALESCE( (cb_btc   ->> 0)::numeric , 0) > 499000000000::numeric
   OR COALESCE( (eurc     ->> 0)::numeric , 0) > 481000000000::numeric
   OR COALESCE( (wrs_eth  ->> 0)::numeric , 0) >    302970000::numeric
   OR COALESCE( (well     ->> 0)::numeric , 0) > 22548107060000::numeric
   OR COALESCE( (usds     ->> 0)::numeric , 0) >        0.495::numeric
   OR COALESCE( (t_btc    ->> 0)::numeric , 0) >        50::numeric
   OR COALESCE( (lbtc     ->> 0)::numeric , 0) > 498800000000::numeric
   OR COALESCE( (virtual  ->> 0)::numeric , 0) > 882564730000::numeric
   OR COALESCE( (morpho   ->> 0)::numeric , 0) > 400000000000::numeric;

   COMMIT;

        `
        getUserAddressesSQL = `
                SELECT user_address
                FROM user_metrics.active_users;
        `
)

func setupLogging() error {
	// Create logs directory if it doesn't exist
	logsDir := "logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return fmt.Errorf("failed to create logs directory: %w", err)
	}

	// Create log file with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	logFileName := filepath.Join(logsDir, fmt.Sprintf("moonwell_snapshot_%s.log", timestamp))
	
	var err error
	logFile, err = os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// Create multi-writer to write to both file and console
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	
	// Setup loggers
	infoLogger = log.New(multiWriter, "[INFO] ", log.LstdFlags|log.Lshortfile)
	errorLogger = log.New(multiWriter, "[ERROR] ", log.LstdFlags|log.Lshortfile)
	
	// Set default logger to use our multi-writer
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	
	infoLogger.Printf("Logging initialized - writing to: %s", logFileName)
	return nil
}

func logError(format string, args ...interface{}) {
	if errorLogger != nil {
		errorLogger.Printf(format, args...)
	} else {
		log.Printf("[ERROR] "+format, args...)
	}
}

func logInfo(format string, args ...interface{}) {
	if infoLogger != nil {
		infoLogger.Printf(format, args...)
	} else {
		log.Printf("[INFO] "+format, args...)
	}
}

func init() {
	// Setup logging first
	if err := setupLogging(); err != nil {
		log.Fatalf("Failed to setup logging: %v", err)
	}
	
	// Load configuration from environment variables with new names
	ethRPC = getEnv("RPC_URL", "https://api-BASE.n.dwellir.com/f83cced1-1793-4198-be0c-4e997f8c5c32")
	pgConnStr = getEnv("PG_DSN", "postgresql://username:password@hostname:port/database")

        // Parse snapshot interval (convert hours to duration)
        intervalHoursStr := getEnv("FETCH_INTERVAL_HOURS", "2")
        intervalHours, err := strconv.ParseFloat(intervalHoursStr, 64)
        if err != nil {
                logError("Invalid fetch interval hours: %v, using default 2h", err)
                intervalHours = 2
        }
        snapshotInterval = time.Duration(intervalHours * float64(time.Hour))

        // Parse concurrency
        concurrencyStr := getEnv("WORKER_CONCURRENCY", "5")
        concurrency, err := strconv.Atoi(concurrencyStr)
        if err != nil || concurrency < 1 {
                logError("Invalid worker concurrency: %v, using default 5", err)
                concurrency = 5
        }
        maxConcurrency = concurrency

        // Set batch size default to 100
        batchSizeStr := getEnv("BATCH_SIZE", "100")
        batch, err := strconv.Atoi(batchSizeStr)
        if err != nil || batch < 1 {
                logError("Invalid batch size: %v, using default 40", err)
                batch = 40
        }
        batchSize = batch

        logInfo("Configuration: RPC=%s, Concurrency=%d, BatchSize=%d, Interval=%s",
                ethRPC, maxConcurrency, batchSize, snapshotInterval)

        // Initialize snapshots map
        userSnapshots = make(map[string]map[string]map[string]string)
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
        value := os.Getenv(key)
        if value == "" {
                return defaultValue
        }
        return value
}

// isValidTokenSymbol validates token symbol to prevent SQL injection
func isValidTokenSymbol(symbol string) bool {
        // Only allow alphanumeric characters, underscores, and hyphens
        // Tokens should not contain special SQL characters
        for _, ch := range symbol {
                if !((ch >= 'a' && ch <= 'z') || 
                     (ch >= 'A' && ch <= 'Z') || 
                     (ch >= '0' && ch <= '9') || 
                     ch == '_' || ch == '-') {
                        return false
                }
        }
        return len(symbol) > 0 && len(symbol) <= 50
}

// loadTokens loads token data from a JSON file
func loadTokens(filePath string) ([]Token, error) {
        data, err := os.ReadFile(filePath)
        if err != nil {
                return nil, fmt.Errorf("error reading token file: %w", err)
        }

        var tokens []Token
        if err := json.Unmarshal(data, &tokens); err != nil {
                return nil, fmt.Errorf("error parsing token JSON: %w", err)
        }

        return tokens, nil
}

// loadUsers loads user addresses from a text file
func loadUsers(filePath string) ([]common.Address, error) {
        data, err := os.ReadFile(filePath)
        if err != nil {
                return nil, fmt.Errorf("error reading users file: %w", err)
        }

        lines := strings.Split(string(data), "\n")
        var users []common.Address
        for _, line := range lines {
                line = strings.TrimSpace(line)
                if line == "" || strings.HasPrefix(line, "#") {
                        continue
                }
                users = append(users, common.HexToAddress(line))
        }

        return users, nil
}

// initTokenContracts initializes token contracts
func initTokenContracts(tokens []Token, client *ethclient.Client) ([]Token, error) {
        for i := range tokens {
                addr := common.HexToAddress(tokens[i].MTokenAddr)
                contract, err := NewMTokenCaller(addr, client)
                if err != nil {
                        return nil, fmt.Errorf("error creating contract for %s: %w", tokens[i].Symbol, err)
                }
                tokens[i].Contract = contract
        }
        return tokens, nil
}

// hasUserActivity checks if a user has any activity for a token
func hasUserActivity(mTokenBalance, borrowBalance *big.Int) bool {
        // User has activity if either mToken balance or borrow balance is non-zero
        return mTokenBalance.Cmp(big.NewInt(0)) > 0 || borrowBalance.Cmp(big.NewInt(0)) > 0
}

// fetchUserBalance fetches a user's balance for a token using getAccountSnapshot
func fetchUserBalance(ctx context.Context, token Token, userAddr common.Address) (*Snapshot, error) {
        // Check if token contract is nil
        if token.Contract == nil {
                return nil, fmt.Errorf("token contract is nil for %s", token.Symbol)
        }
        
        // Call getAccountSnapshot
        errorCode, mTokenBalance, borrowBalance, _, err := token.Contract.GetAccountSnapshot(&bind.CallOpts{
                Context: ctx,
        }, userAddr)

        if err != nil {
                return nil, fmt.Errorf("error calling getAccountSnapshot: %w", err)
        }

        // Check error code (0 means no error)
        if errorCode == nil {
                return nil, fmt.Errorf("getAccountSnapshot returned nil error code")
        }
        if errorCode.Cmp(big.NewInt(0)) != 0 {
                return nil, fmt.Errorf("getAccountSnapshot returned error code: %s", errorCode.String())
        }
        
        // Ensure balance values are not nil
        if mTokenBalance == nil {
                mTokenBalance = big.NewInt(0)
        }
        if borrowBalance == nil {
                borrowBalance = big.NewInt(0)
        }

        // Create snapshot with only mTokenBalance (index 1) and borrowBalance (index 2)
        snapshot := &Snapshot{
                User:              userAddr,
                MTokenBalance:     mTokenBalance,
                BorrowBalance:     borrowBalance,
                UnderlyingBalance: big.NewInt(0), // Set to zero as we're not calculating it
        }

        // If user has no tokens AND no borrowing, return a snapshot with zero balances
        // This is to correctly identify users who haven't interacted with the protocol
        if !hasUserActivity(mTokenBalance, borrowBalance) {
                snapshot.MTokenBalance = big.NewInt(0)
                snapshot.BorrowBalance = big.NewInt(0)
        }

        return snapshot, nil
}


// addToUserSnapshot adds a token balance to the user's snapshot for printing
// Pass timestamp to ensure all users in same batch have identical timestamp
func addToUserSnapshot(snapshot *Snapshot, tokenSymbol string, timestamp string) {
        userSnapshotsMutex.Lock()
        defer userSnapshotsMutex.Unlock()

        // Use provided timestamp to ensure consistency within a batch
        ts := timestamp
        if ts == "" {
                // Fallback if no timestamp provided
                ts = time.Now().Format(time.RFC3339)
        }

        // Initialize maps if needed
        if userSnapshots[ts] == nil {
                userSnapshots[ts] = make(map[string]map[string]string)
        }

        userAddr := snapshot.User.Hex()
        if userSnapshots[ts][userAddr] == nil {
                userSnapshots[ts][userAddr] = make(map[string]string)
        }

        // Store balance as string (mToken,borrow)
        userSnapshots[ts][userAddr][tokenSymbol] = fmt.Sprintf("%s,%s",
                snapshot.MTokenBalance.String(),
                snapshot.BorrowBalance.String())
}

// printUserSnapshots prints user snapshots in a readable format
func printUserSnapshots() {
        userSnapshotsMutex.Lock()
        defer userSnapshotsMutex.Unlock()

        // Get all timestamps and sort them
        var timestamps []string
        for ts := range userSnapshots {
                timestamps = append(timestamps, ts)
        }
        sort.Strings(timestamps)

        // Print only the latest snapshot (or all if needed)
        if len(timestamps) > 0 {
                // Get the latest timestamp
                latestTS := timestamps[len(timestamps)-1]
                users := userSnapshots[latestTS]
                
                fmt.Printf("=== Snapshot at %s ===\n\n", latestTS)

                // Print each user's balances
                for userAddr, tokens := range users {
                        for token, balanceStr := range tokens {
                                parts := strings.Split(balanceStr, ",")
                                if len(parts) != 2 {
                                        log.Printf("Warning: Invalid balance format for user %s, token %s: %s",
                                                userAddr, token, balanceStr)
                                        continue
                                }

                                fmt.Printf("User: %s\n  %s: %s mTokens, %s borrow\n\n",
                                        userAddr, token, parts[0], parts[1])
                        }
                }
                
                // Clear only the printed snapshot, keep others for potential debugging
                delete(userSnapshots, latestTS)
                
                // Clean up old snapshots (keep only last 5 for debugging)
                if len(timestamps) > 5 {
                        for i := 0; i < len(timestamps)-5; i++ {
                                delete(userSnapshots, timestamps[i])
                        }
                }
        }
}

// Snapshot represents user balances for a token
type BatchSnapshot struct {
        User       common.Address
        TokenData  map[string][2]*big.Int // Map of token symbol -> [mToken balance, underlying balance]
        UpdateTime time.Time
}

// FailedFetch represents a failed RPC call that needs retry
type FailedFetch struct {
        User  common.Address
        Token Token
        Error error
}

// fetchUserBalanceWithRetry fetches user balance with retry mechanism
func fetchUserBalanceWithRetry(ctx context.Context, token Token, userAddr common.Address, maxRetries int) (*Snapshot, error) {
        var lastErr error
        
        for attempt := 0; attempt <= maxRetries; attempt++ {
                if attempt > 0 {
                        // Wait 500ms before retry
                        time.Sleep(500 * time.Millisecond)
                        logInfo("Retrying user %s, token %s (attempt %d/%d)", userAddr.Hex(), token.Symbol, attempt, maxRetries)
                }
                
                snapshot, err := fetchUserBalance(ctx, token, userAddr)
                if err == nil {
                        if attempt > 0 {
                                logInfo("Retry successful for user %s, token %s", userAddr.Hex(), token.Symbol)
                        }
                        return snapshot, nil
                }
                
                lastErr = err
                logError("Attempt %d failed for user %s, token %s: %v", attempt+1, userAddr.Hex(), token.Symbol, err)
        }
        
        return nil, fmt.Errorf("all retry attempts failed for user %s, token %s: %w", userAddr.Hex(), token.Symbol, lastErr)
}

// processUserBatch processes a batch of users for all tokens and inserts them into the database
func processUserBatch(ctx context.Context, tokens []Token, users []common.Address, db *sql.DB) error {
        if db == nil {
                return fmt.Errorf("database connection is nil")
        }
        if len(users) == 0 || len(tokens) == 0 {
                return nil // Nothing to process
        }
        
        // Create a consistent timestamp for this batch
        batchTimestamp := time.Now().Format(time.RFC3339)
        
        // Create worker pool with semaphore for concurrency control
        var wg sync.WaitGroup
        semaphore := make(chan struct{}, maxConcurrency)
        failedFetches := make(chan FailedFetch, len(users)*len(tokens))

        // Create a mutex for safely updating the batch snapshots
        var dataLock sync.Mutex

        // Create a map to collect all token data for each user
        batchData := make(map[string]*BatchSnapshot)
        for _, user := range users {
                userAddr := strings.ToLower(user.Hex())
                batchData[userAddr] = &BatchSnapshot{
                        User:       user,
                        TokenData:  make(map[string][2]*big.Int),
                        UpdateTime: time.Now(),
                }
        }

        // Track tokens processed for adding delays
        tokenProcessed := 0

        // Process each token for each user
        for i, token := range tokens {
                // Add a delay between tokens to avoid overloading the RPC
                if tokenProcessed > 0 {
                        log.Printf("Processing token %d/%d: %s (waiting %s)", i+1, len(tokens), token.Symbol, tokenDelay)
                        time.Sleep(tokenDelay) // Using global tokenDelay (1s by default)
                } else {
                        log.Printf("Processing token %d/%d: %s", i+1, len(tokens), token.Symbol)
                }

                tokenProcessed++

                for _, user := range users {
                        wg.Add(1)

                        // Capture variables for goroutine
                        t := token
                        u := user

                        go func() {
                                defer wg.Done()

                                // Acquire semaphore
                                select {
                                case semaphore <- struct{}{}:
                                        defer func() { <-semaphore }()
                                case <-ctx.Done():
                                        failedFetches <- FailedFetch{User: u, Token: t, Error: fmt.Errorf("context cancelled")}
                                        return
                                }

                                // Check context before fetching
                                if ctx.Err() != nil {
                                        failedFetches <- FailedFetch{User: u, Token: t, Error: fmt.Errorf("context cancelled")}
                                        return
                                }

                                // Fetch user balance with retry (3 attempts total)
                                snapshot, err := fetchUserBalanceWithRetry(ctx, t, u, 2)
                                if err != nil {
                                        failedFetches <- FailedFetch{User: u, Token: t, Error: err}
                                        return
                                }

                                // Add to user snapshots for printing
                                addToUserSnapshot(snapshot, t.Symbol, batchTimestamp)

                                // Store the data in our batch map
                                dataLock.Lock()
                                userAddr := strings.ToLower(u.Hex())
                                if batchData[userAddr] != nil && snapshot != nil {
                                        if snapshot.MTokenBalance != nil && snapshot.BorrowBalance != nil {
                                                batchData[userAddr].TokenData[t.Symbol] = [2]*big.Int{snapshot.MTokenBalance, snapshot.BorrowBalance}
                                        } else {
                                                failedFetches <- FailedFetch{User: u, Token: t, Error: fmt.Errorf("nil balance values")}
                                        }
                                }
                                dataLock.Unlock()
                        }()
                }
        }

        // Wait for all goroutines to complete
        wg.Wait()
        close(failedFetches)

        // Collect failed fetches
        var failures []FailedFetch
        for failure := range failedFetches {
                failures = append(failures, failure)
        }

        // Log failed fetches (these users will be skipped for this batch)
        if len(failures) > 0 {
                logError("Failed to fetch data for %d user-token pairs after retries:", len(failures))
                for _, failure := range failures {
                        logError("  User %s, Token %s: %v", failure.User.Hex(), failure.Token.Symbol, failure.Error)
                        
                        // Remove failed user-token data from batch (don't insert 0,0 values)
                        dataLock.Lock()
                        userAddr := strings.ToLower(failure.User.Hex())
                        if batchData[userAddr] != nil {
                                delete(batchData[userAddr].TokenData, failure.Token.Symbol)
                        }
                        dataLock.Unlock()
                }
                logError("These users will be processed in the next cycle")
        }

        // Now that we have all the data, insert the complete batch
        successfulUsers := 0
        for _, snapshot := range batchData {
                if len(snapshot.TokenData) > 0 {
                        successfulUsers++
                }
        }
        log.Printf("Token data collected for %d/%d users (skipped %d users with failed fetches). Inserting batch...", 
                successfulUsers, len(users), len(users)-successfulUsers)
        if err := insertCompleteBatch(db, batchData, tokens); err != nil {
                return fmt.Errorf("error inserting complete batch: %w", err)
        }

        return nil
}

// processUserBatchInSubBatches processes a batch of users by dividing them into smaller sub-batches
func processUserBatchInSubBatches(ctx context.Context, tokens []Token, users []common.Address, db *sql.DB) error {
        totalUsers := len(users)

        // Using the global subBatchSizes variable (five batches of 20 each by default)
        logInfo("Dividing batch of %d users into %d sub-batches of sizes: %v",
                totalUsers, len(subBatchSizes), subBatchSizes)

        // Process sub-batches
        startIdx := 0
        for i, size := range subBatchSizes {
                // Calculate end index for this sub-batch, ensuring we don't go beyond the total users
                endIdx := startIdx + size
                if endIdx > totalUsers {
                        endIdx = totalUsers
                }

                // Skip if we've processed all users
                if startIdx >= totalUsers {
                        break
                }

                // Extract the sub-batch of users
                subBatch := users[startIdx:endIdx]
                logInfo("Processing sub-batch %d/%d with %d users (users %d to %d)",
                        i+1, len(subBatchSizes), len(subBatch), startIdx, endIdx-1)

                // Process this sub-batch
                if err := processUserBatch(ctx, tokens, subBatch, db); err != nil {
                        return fmt.Errorf("error processing sub-batch %d: %w", i+1, err)
                }

                // Add a delay between sub-batches to avoid rate limits
                if i < len(subBatchSizes)-1 && endIdx < totalUsers {
                        logInfo("Waiting %s before processing next sub-batch", subBatchDelay)
                        time.Sleep(subBatchDelay)
                }

                // Update start index for next sub-batch
                startIdx = endIdx
        }

        return nil
}

// insertCompleteBatch inserts a batch of users with their complete token data
func insertCompleteBatch(db *sql.DB, batchData map[string]*BatchSnapshot, tokens []Token) error {
        if db == nil {
                return fmt.Errorf("database connection is nil")
        }
        if len(batchData) == 0 {
                return nil // Nothing to insert
        }
        
        // Start a transaction with context
        ctx, cancel := context.WithTimeout(rootCtx, 60*time.Second)
        defer cancel()
        
        tx, err := db.BeginTx(ctx, nil)
        if err != nil {
                return fmt.Errorf("error starting transaction: %w", err)
        }
        defer tx.Rollback()

        // Create the dynamic SQL for inserting or updating users with all token values using upsert
        insertSQL := "INSERT INTO public.moonwell_user_balances (user_addr, update_time"
        valueSQL := "VALUES ($1, $2"

        // Add token columns to the SQL
        for i, token := range tokens {
                // Validate each token symbol
                if !isValidTokenSymbol(token.Symbol) {
                        return fmt.Errorf("invalid token symbol in batch: %s", token.Symbol)
                }
                insertSQL += fmt.Sprintf(`, "%s"`, token.Symbol)
                valueSQL += fmt.Sprintf(", $%d", i+3)
        }

        // Add ON CONFLICT clause to handle existing users
        updateSQL := ") " + valueSQL + ") ON CONFLICT (user_addr) DO UPDATE SET update_time = $2"
        for i, token := range tokens {
                updateSQL += fmt.Sprintf(`, "%s" = $%d`, token.Symbol, i+3)
        }

        finalSQL := insertSQL + updateSQL

        // Prepare statement with context
        stmt, err := tx.PrepareContext(ctx, finalSQL)
        if err != nil {
                return fmt.Errorf("error preparing statement: %w", err)
        }
        defer stmt.Close()

        // Process and insert each user
        successCount := 0

        for userAddr, snapshot := range batchData {
                // Create parameter list
                params := make([]interface{}, len(tokens)+2)
                params[0] = userAddr
                params[1] = snapshot.UpdateTime

                // Add all token values
                for i, token := range tokens {
                        // Default empty value
                        val := "{0,0}"

                        // If we have data for this token, format it as PostgreSQL array
                        if tokenVals, ok := snapshot.TokenData[token.Symbol]; ok {
                                // Format as {mTokenBalance,underlyingBalance}
                                val = fmt.Sprintf("{\"%s\",\"%s\"}",
                                        tokenVals[0].String(),
                                        tokenVals[1].String())
                        }

                        params[i+2] = val
                }

                // Execute insert/update with context
                _, err := stmt.ExecContext(ctx, params...)
                if err != nil {
                        return fmt.Errorf("error inserting/updating user %s: %w", userAddr, err)
                }

                successCount++
        }

        // Commit transaction
        if err := tx.Commit(); err != nil {
                return fmt.Errorf("error committing transaction: %w", err)
        }

        logInfo("Successfully inserted/updated %d users with complete token data", successCount)
        return nil
}

// saveCheckpoint saves the last processed index to file
func saveCheckpoint(index int) error {
        // Create data directory if it doesn't exist
        dir := "data"
        if _, err := os.Stat(dir); os.IsNotExist(err) {
                if err := os.Mkdir(dir, 0755); err != nil {
                        return fmt.Errorf("failed to create data directory: %w", err)
                }
        }

        // Save index to file
        indexStr := strconv.Itoa(index)
        if err := os.WriteFile(checkpointFile, []byte(indexStr), 0644); err != nil {
                return fmt.Errorf("failed to write checkpoint: %w", err)
        }

        logInfo("Checkpoint saved: last processed index %d", index)
        return nil
}

// loadCheckpoint loads the last processed index from file
func loadCheckpoint() (int, error) {
        // Check if checkpoint file exists
        if _, err := os.Stat(checkpointFile); os.IsNotExist(err) {
                logInfo("No checkpoint found, starting from beginning")
                return 0, nil
        }

        // Read checkpoint file
        data, err := os.ReadFile(checkpointFile)
        if err != nil {
                return 0, fmt.Errorf("failed to read checkpoint: %w", err)
        }

        // Parse index
        indexStr := strings.TrimSpace(string(data))
        index, err := strconv.Atoi(indexStr)
        if err != nil {
                return 0, fmt.Errorf("invalid checkpoint format: %w", err)
        }

        logInfo("Checkpoint loaded: resuming from index %d", index)
        return index, nil
}

// takeSnapshot takes a snapshot of all user balances
func takeSnapshot(ctx context.Context, tokens []Token, users []common.Address, db *sql.DB) error {
        totalUsers := len(users)
        processed := 0
        startTime := time.Now()

        // Start from last processed index
        startIndex := lastProcessedIndex
        if startIndex >= totalUsers {
                logInfo("All users have been processed. Resetting to beginning.")
                startIndex = 0
                lastProcessedIndex = 0
                if err := saveCheckpoint(lastProcessedIndex); err != nil {
                        log.Printf("Warning: Failed to save checkpoint: %v", err)
                }
        }

        logInfo("Starting from index %d of %d users", startIndex, totalUsers)

        // Process users in batches
        for i := startIndex; i < len(users); i += batchSize {
                end := i + batchSize
                if end > len(users) {
                        end = len(users)
                }

                batchStartTime := time.Now()
                batchUsers := users[i:end]
                logInfo("Processing batch of users %d to %d of %d (%.1f%% complete)",
                        i, end-1, totalUsers, float64(processed+i-startIndex)/float64(totalUsers)*100)

                // Process the batch in smaller sub-batches to avoid rate limits
                if err := processUserBatchInSubBatches(ctx, tokens, batchUsers, db); err != nil {
                        // Save the last successful index before returning error
                        lastProcessedIndex = i
                        if err := saveCheckpoint(lastProcessedIndex); err != nil {
                                log.Printf("Warning: Failed to save checkpoint: %v", err)
                        }
                        return err
                }

                // Update the checkpoint after each batch
                lastProcessedIndex = end
                if err := saveCheckpoint(lastProcessedIndex); err != nil {
                        log.Printf("Warning: Failed to save checkpoint: %v", err)
                }

                processed += len(batchUsers)
                batchTime := time.Since(batchStartTime)

                // Calculate ETA
                if processed > 0 {
                        elapsed := time.Since(startTime)
                        usersPerSec := float64(processed) / elapsed.Seconds()
                        remainingUsers := totalUsers - (startIndex + processed)
                        eta := time.Duration(float64(remainingUsers) / usersPerSec * float64(time.Second))

                        log.Printf("Batch of %d users processed in %s. ETA: %s",
                                len(batchUsers), batchTime.Round(time.Millisecond),
                                (time.Now().Add(eta)).Format("15:04:05"))
                }

                // Add a delay between main batches
                if end < len(users) {
                        log.Printf("Waiting between main batches...")
                        time.Sleep(subBatchDelay) // Using the same delay between main batches
                }
        }

        // Print results
        printUserSnapshots()

        totalTime := time.Since(startTime)
        log.Printf("Finished processing all %d users in %s", totalUsers, totalTime.Round(time.Second))

        // Calculate performance statistics
        usersPerSecond := float64(totalUsers) / totalTime.Seconds()
        log.Printf("Performance: %.2f users/second", usersPerSecond)

        return nil
}

// cleanupInactiveUsers removes users from the database that are no longer in the active users list
func cleanupInactiveUsers(activeUserAddresses []string) error {
        if len(activeUserAddresses) == 0 {
                log.Println("No active users found, skipping cleanup")
                return nil
        }

        log.Printf("Cleaning up inactive users from database...")

        // Create placeholders for the IN clause
        placeholders := make([]string, len(activeUserAddresses))
        for i := range activeUserAddresses {
                placeholders[i] = fmt.Sprintf("$%d", i+1)
        }

        // Build the cleanup query
        cleanupSQL := fmt.Sprintf(`
                DELETE FROM public.moonwell_user_balances 
                WHERE user_addr NOT IN (%s)
        `, strings.Join(placeholders, ","))

        // Convert strings to interface{} slice for query execution
        args := make([]interface{}, len(activeUserAddresses))
        for i, addr := range activeUserAddresses {
                args[i] = strings.ToLower(addr) // Ensure consistent case
        }

        // Execute cleanup
        ctx, cancel := context.WithTimeout(rootCtx, 30*time.Second)
        defer cancel()

        result, err := db.ExecContext(ctx, cleanupSQL, args...)
        if err != nil {
                return fmt.Errorf("error cleaning up inactive users: %w", err)
        }

        // Get the number of rows affected
        rowsAffected, err := result.RowsAffected()
        if err != nil {
                log.Printf("Warning: Could not get rows affected count: %v", err)
        } else {
                log.Printf("Cleanup completed: removed %d inactive users from database", rowsAffected)
        }

        return nil
}

// updateUsersFromSupabase updates the users.txt file with active users from the same database
func updateUsersFromSupabase() error {
        log.Println("=== Starting User List Update ===")
        
        // Create active users table
        _, err := db.Exec(createActiveUsersSQL)
        if err != nil {
                log.Printf("Error creating active_users table: %v", err)
                return err
        }
        
        log.Println("Fetching active user addresses...")
        rows, err := db.Query(getUserAddressesSQL)
        if err != nil {
                log.Printf("Error querying user addresses: %v", err)
                log.Println("Will continue with existing addresses in users.txt")
                return err
        }
        defer rows.Close()
        
        var addresses []string
        for rows.Next() {
                var address string
                if err := rows.Scan(&address); err != nil {
                        log.Printf("Error scanning address: %v", err)
                        log.Println("Will continue with existing addresses in users.txt")
                        return err
                }
                address = strings.TrimSpace(strings.ToLower(address))
                if address != "" && strings.HasPrefix(address, "0x") && len(address) == 42 {
                        addresses = append(addresses, address)
                } else {
                        log.Printf("Warning: Skipping invalid address: %s", address)
                }
        }
        
        if err := rows.Err(); err != nil {
                log.Printf("Error reading addresses: %v", err)
                log.Println("Will continue with existing addresses in users.txt")
                return err
        }
        
        log.Printf("Retrieved %d active user addresses", len(addresses))
        
        // Clean up inactive users from the database before updating users.txt
        if err := cleanupInactiveUsers(addresses); err != nil {
                log.Printf("Warning: Failed to cleanup inactive users: %v", err)
                // Continue despite cleanup failure
        }
        
        // Update users.txt with active addresses
        content := strings.Join(addresses, "\n")
        if len(addresses) > 0 {
                content += "\n"
        }
        
        if err := os.WriteFile("data/users.txt", []byte(content), 0644); err != nil {
                log.Printf("Error writing users.txt: %v", err)
                log.Println("Will continue with existing addresses in users.txt")
                return err
        }
        
        log.Printf("users.txt updated with %d addresses", len(addresses))
        
        log.Println("Resetting checkpoint to 0...")
        if err := os.WriteFile(checkpointFile, []byte("0"), 0644); err != nil {
                log.Printf("Error resetting checkpoint: %v", err)
        }
        
        // Set lastProcessedIndex to 0 in memory as well
        lastProcessedIndex = 0
        
        log.Println("User update completed successfully")
        return nil
}

func main() {
        // Connect to Ethereum node
        var err error
        client, err = ethclient.Dial(ethRPC)
        if err != nil {
                log.Fatalf("Failed to connect to Ethereum node: %v", err)
        }
        log.Println("Connected to Ethereum node")

        // Set parameters based on environment variables
        log.Printf("Using worker_concurrency=%d and batch_size=%d", maxConcurrency, batchSize)

        // Connect to database with better error handling
        log.Printf("Connecting to database using PG_DSN: %s", maskConnectionString(pgConnStr))
        db, err = sql.Open("postgres", pgConnStr)
        if err != nil {
                log.Fatalf("Failed to open database connection: %v", err)
        }
        defer db.Close()

        // Set connection pool parameters
        db.SetMaxOpenConns(25)
        db.SetMaxIdleConns(5)
        db.SetConnMaxLifetime(5 * time.Minute)

        // Create initial context for database ping before rootCtx is created
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        if err := db.PingContext(ctx); err != nil {
                log.Fatalf("Failed to ping database: %v\nPlease check your PG_DSN connection string and ensure PostgreSQL is running.", err)
        }
        log.Println("Successfully connected to database")

        // Load tokens
        tokens, err := loadTokens("data/tokens.json")
        if err != nil {
                log.Fatalf("Failed to load tokens: %v", err)
        }

        // Initialize token contracts
        tokens, err = initTokenContracts(tokens, client)
        if err != nil {
                log.Fatalf("Failed to initialize token contracts: %v", err)
        }

        // Load users
        users, err := loadUsers("data/users.txt")
        if err != nil {
                log.Fatalf("Failed to load users: %v", err)
        }

        // Load checkpoint to resume from last processed address
        index, err := loadCheckpoint()
        if err != nil {
                log.Printf("Warning: Failed to load checkpoint: %v. Starting from beginning.", err)
                lastProcessedIndex = 0
        } else {
                lastProcessedIndex = index
        }

        // Create context with cancellation
        rootCtx, cancelFunc = context.WithCancel(context.Background())
        defer cancelFunc()

        // Set up signal handling for graceful shutdown
        sigChan := make(chan os.Signal, 1)
        signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
        go func() {
                <-sigChan
                log.Println("Received shutdown signal, gracefully stopping...")
                cancelFunc()
        }()

        // Try to update users before initial snapshot
        log.Println("Attempting to update user list before initial snapshot...")
        if err := updateUsersFromSupabase(); err != nil {
                log.Printf("Failed to update users: %v", err)
                log.Println("Will continue with existing addresses in users.txt")
        }

        // Reload users (either updated or existing)
        users, err = loadUsers("data/users.txt")
        if err != nil {
                log.Fatalf("Failed to load users: %v", err)
        }

        // Reload checkpoint after user update
        if index, err := loadCheckpoint(); err != nil {
                log.Printf("Warning: Failed to load checkpoint: %v. Starting from beginning.", err)
                lastProcessedIndex = 0
        } else {
                lastProcessedIndex = index
        }

        // Take initial snapshot
        log.Printf("Starting snapshot of %d users with %d tokens...", len(users), len(tokens))
        startTime := time.Now()

        if err := takeSnapshot(rootCtx, tokens, users, db); err != nil {
                if rootCtx.Err() == context.Canceled {
                        log.Println("Snapshot canceled due to shutdown")
                        return
                }
                log.Fatalf("Failed to take snapshot: %v", err)
        }

        log.Printf("Initial snapshot completed in %s", time.Since(startTime).Round(time.Second))

        // Set up ticker for periodic snapshots
        ticker := time.NewTicker(snapshotInterval)
        defer ticker.Stop()

        log.Printf("Waiting %s until next snapshot", snapshotInterval)

        // Main loop runs every snapshotInterval
        for {
                select {
                case <-rootCtx.Done():
                        log.Println("Shutting down main loop...")
                        return
                case <-ticker.C:
                startTime := time.Now()
                log.Printf("Starting periodic snapshot...")

                // Try to update users from database before each periodic snapshot
                log.Println("Attempting to update user list before periodic snapshot...")
                if err := updateUsersFromSupabase(); err != nil {
                        log.Printf("Failed to update users: %v", err)
                        log.Printf("Will continue with existing addresses in users.txt")
                }

                // Load current users
                updatedUsers, err := loadUsers("data/users.txt")
                if err != nil {
                        log.Printf("Error loading users.txt: %v", err)
                        log.Printf("Will use previous user list")
                        // Continue with existing users list from previous cycle
                } else {
                        // Update the users variable with new addresses
                        users = updatedUsers
                        log.Printf("Processing %d users", len(users))
                }

                // Reload checkpoint to ensure we're using the latest value
                if index, err := loadCheckpoint(); err != nil {
                        log.Printf("Warning: Failed to load checkpoint: %v. Using current index: %d", err, lastProcessedIndex)
                } else {
                        lastProcessedIndex = index
                }

                if err := takeSnapshot(rootCtx, tokens, users, db); err != nil {
                        if rootCtx.Err() == context.Canceled {
                                log.Println("Snapshot canceled due to shutdown")
                                return
                        }
                        log.Printf("Failed to take snapshot: %v", err)
                } else {
                        log.Printf("Periodic snapshot completed in %s", time.Since(startTime).Round(time.Second))
                }

                log.Printf("Waiting %s until next snapshot", snapshotInterval)
                }
        }
}

// maskConnectionString masks password in connection string for logging
func maskConnectionString(connStr string) string {
        // Simple masking, not perfect but helps avoid logging passwords
        if strings.Contains(connStr, "password=") {
                re := regexp.MustCompile(`password=([^&\s]+)`)
                return re.ReplaceAllString(connStr, "password=*****")
        }

        // Try to mask password in URL format
        if strings.Contains(connStr, "@") {
                parts := strings.Split(connStr, "@")
                if len(parts) >= 2 && strings.Contains(parts[0], ":") {
                        authParts := strings.Split(parts[0], ":")
                        if len(authParts) >= 3 {
                                authParts[len(authParts)-1] = "*****"
                                parts[0] = strings.Join(authParts, ":")
                                return strings.Join(parts, "@")
                        }
                }
        }

        return strings.Replace(connStr, pgConnStr, "******", -1)
}