package main

import (
        "context"
        "database/sql"
        "encoding/json"
        "fmt"
        "log"
        "math/big"
        "os"
        "regexp"
        "strconv"
        "strings"
        "sync"
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
        maxConcurrency     = 5                         // Reduced from 10 to 5 workers
        batchSize          = 100                       // Keep the batch size at 100 for database efficiency
        subBatchSizes      = []int{20, 20, 20, 20, 20} // Five sub-batches of 20 each
        fetchIntervalHours = 6
        useMulticall       = true
        tokenDelay         = 1300 * time.Millisecond // Increased from 200ms to 1s
        subBatchDelay      = 1 * time.Second // Longer delay between sub-batches

        // Database connection
        db *sql.DB

        // Context for operations
        rootCtx    context.Context
        cancelFunc context.CancelFunc

        // Ethereum client
        client *ethclient.Client

        // User snapshots and mutex
        userSnapshots      map[string]map[string]map[string]string
        userSnapshotsMutex sync.Mutex

        // Stats tracking
        processedTokens int
        processedUsers  int

        // Checkpoint variables
        lastProcessedIndex int
        checkpointFile     = "data/checkpoint.txt"

        // Multi3 contract address (for batch calls)
        multi3 = common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")

        // SQL queries for user updates
        createActiveUsersSQL = `
                BEGIN;

DROP table user_metrics.active_users cascade;

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
        truncateBalancesSQL = `
                TRUNCATE TABLE public.moonwell_user_balances;
        `
)

func init() {
        // Load configuration from environment variables with new names
        ethRPC = getEnv("RPC_URL", "https://api-BASE.n.dwellir.com/f83cced1-1793-4198-be0c-4e997f8c5c32")
        pgConnStr = getEnv("PG_DSN", "postgresql://username:password@hostname:port/database")

        // Parse snapshot interval (convert hours to duration)
        intervalHoursStr := getEnv("FETCH_INTERVAL_HOURS", "2")
        intervalHours, err := strconv.ParseFloat(intervalHoursStr, 64)
        if err != nil {
                log.Printf("Invalid fetch interval hours: %v, using default 2h", err)
                intervalHours = 2
        }
        snapshotInterval = time.Duration(intervalHours * float64(time.Hour))

        // Parse concurrency
        concurrencyStr := getEnv("WORKER_CONCURRENCY", "5")
        concurrency, err := strconv.Atoi(concurrencyStr)
        if err != nil || concurrency < 1 {
                log.Printf("Invalid worker concurrency: %v, using default 5", err)
                concurrency = 5
        }
        maxConcurrency = concurrency

        // Set batch size default to 40
        batchSizeStr := getEnv("BATCH_SIZE", "40")
        batch, err := strconv.Atoi(batchSizeStr)
        if err != nil || batch < 1 {
                log.Printf("Invalid batch size: %v, using default 40", err)
                batch = 40
        }
        batchSize = batch

        log.Printf("Configuration: RPC=%s, Concurrency=%d, BatchSize=%d, Interval=%s",
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
        // Call getAccountSnapshot
        errorCode, mTokenBalance, borrowBalance, _, err := token.Contract.GetAccountSnapshot(&bind.CallOpts{
                Context: ctx,
        }, userAddr)

        if err != nil {
                return nil, fmt.Errorf("error calling getAccountSnapshot: %w", err)
        }

        // Check error code (0 means no error)
        if errorCode.Cmp(big.NewInt(0)) != 0 {
                return nil, fmt.Errorf("getAccountSnapshot returned error code: %s", errorCode.String())
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

// saveSnapshot saves a user's balance to the database
func saveSnapshot(db *sql.DB, snapshot *Snapshot, tokenSymbol string) error {
        // Use upsert to ensure one row per user address
        sqlQuery := fmt.Sprintf(`
                INSERT INTO public.moonwell_user_balances (user_addr, update_time, "%s")
                VALUES ($1, $2, ARRAY[$3, $4]::NUMERIC[])
                ON CONFLICT (user_addr)
                DO UPDATE SET
                    update_time = $2,
                    "%s" = ARRAY[$3, $4]::NUMERIC[]
        `, tokenSymbol, tokenSymbol)

        // Create a context with timeout for the operation
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        // Standardize address format - always use lowercase for storage
        // This ensures the ON CONFLICT clause works correctly regardless of case
        userAddr := strings.ToLower(snapshot.User.Hex())

        // Use ExecContext instead of Exec for better context handling
        _, err := db.ExecContext(ctx,
                sqlQuery,
                userAddr,
                time.Now(),
                snapshot.MTokenBalance.String(),
                snapshot.BorrowBalance.String()) // Store borrow balance (index 2) instead of underlying balance

        if err != nil {
                return fmt.Errorf("error saving snapshot to database: %w", err)
        }

        return nil
}

// batchSaveSnapshots saves multiple snapshots to the database in one transaction
func batchSaveSnapshots(db *sql.DB, snapshots []*Snapshot, tokenSymbol string) error {
        if len(snapshots) == 0 {
                return nil
        }

        // Start a transaction
        ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        defer cancel()

        tx, err := db.BeginTx(ctx, nil)
        if err != nil {
                return fmt.Errorf("failed to begin transaction: %w", err)
        }
        defer tx.Rollback() // Rollback if we don't commit

        // Prepare the statement
        sqlQuery := fmt.Sprintf(`
                INSERT INTO public.moonwell_user_balances (user_addr, update_time, "%s")
                VALUES ($1, $2, ARRAY[$3, $4]::NUMERIC[])
                ON CONFLICT (user_addr)
                DO UPDATE SET
                    update_time = $2,
                    "%s" = ARRAY[$3, $4]::NUMERIC[]
        `, tokenSymbol, tokenSymbol)

        stmt, err := tx.PrepareContext(ctx, sqlQuery)
        if err != nil {
                return fmt.Errorf("failed to prepare statement: %w", err)
        }
        defer stmt.Close()

        now := time.Now()

        // Execute for each snapshot
        for _, snapshot := range snapshots {
                userAddr := strings.ToLower(snapshot.User.Hex())
                _, err := stmt.ExecContext(ctx,
                        userAddr,
                        now,
                        snapshot.MTokenBalance.String(),
                        snapshot.BorrowBalance.String())

                if err != nil {
                        return fmt.Errorf("error executing batch statement for %s: %w", userAddr, err)
                }
        }

        // Commit the transaction
        if err := tx.Commit(); err != nil {
                return fmt.Errorf("failed to commit transaction: %w", err)
        }

        return nil
}

// addToUserSnapshot adds a token balance to the user's snapshot for printing
func addToUserSnapshot(snapshot *Snapshot, tokenSymbol string) {
        userSnapshotsMutex.Lock()
        defer userSnapshotsMutex.Unlock()

        // Get current timestamp as string
        ts := time.Now().Format(time.RFC3339)

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

        // Sort timestamps (not necessary for a single snapshot but good practice)
        for ts, users := range userSnapshots {
                fmt.Printf("=== Snapshot at %s ===\n\n", ts)

                // Print each user's balances
                for userAddr, tokens := range users {
                        for token, balanceStr := range tokens {
                                parts := strings.Split(balanceStr, ",")
                                if len(parts) != 2 {
                                        continue
                                }

                                fmt.Printf("User: %s\n  %s: %s mTokens, %s borrow\n\n",
                                        userAddr, token, parts[0], parts[1])
                        }
                }
        }

        // Clear snapshots after printing
        userSnapshots = make(map[string]map[string]map[string]string)
}

// Snapshot represents user balances for a token
type BatchSnapshot struct {
        User       common.Address
        TokenData  map[string][2]*big.Int // Map of token symbol -> [mToken balance, underlying balance]
        UpdateTime time.Time
}

// processUserBatch processes a batch of users for all tokens and inserts them into the database
func processUserBatch(ctx context.Context, tokens []Token, users []common.Address, db *sql.DB) error {
        // Create worker pool with semaphore for concurrency control
        var wg sync.WaitGroup
        semaphore := make(chan struct{}, maxConcurrency)
        errChan := make(chan error, len(users)*len(tokens))

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
                                semaphore <- struct{}{}
                                defer func() { <-semaphore }()

                                // Fetch user balance
                                snapshot, err := fetchUserBalance(ctx, t, u)
                                if err != nil {
                                        errChan <- fmt.Errorf("error fetching balance for user %s, token %s: %w",
                                                u.Hex(), t.Symbol, err)
                                        return
                                }

                                // Add to user snapshots for printing
                                addToUserSnapshot(snapshot, t.Symbol)

                                // Store the data in our batch map
                                dataLock.Lock()
                                userAddr := strings.ToLower(u.Hex())
                                if batchData[userAddr] != nil {
                                        batchData[userAddr].TokenData[t.Symbol] = [2]*big.Int{snapshot.MTokenBalance, snapshot.BorrowBalance}
                                }
                                dataLock.Unlock()
                        }()
                }
        }

        // Wait for all goroutines to complete
        wg.Wait()
        close(errChan)

        // Check for errors
        var errors []string
        for err := range errChan {
                errors = append(errors, err.Error())
        }

        if len(errors) > 0 {
                for _, err := range errors {
                        log.Printf("Warning: %v", err)
                }
        }

        // Now that we have all the data, insert the complete batch
        log.Printf("All token data collected for %d users. Inserting complete batch...", len(users))
        if err := insertCompleteBatch(db, batchData, tokens); err != nil {
                return fmt.Errorf("error inserting complete batch: %w", err)
        }

        return nil
}

// processUserBatchInSubBatches processes a batch of users by dividing them into smaller sub-batches
func processUserBatchInSubBatches(ctx context.Context, tokens []Token, users []common.Address, db *sql.DB) error {
        totalUsers := len(users)

        // Using the global subBatchSizes variable (five batches of 20 each by default)
        log.Printf("Dividing batch of %d users into %d sub-batches of sizes: %v",
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
                log.Printf("Processing sub-batch %d/%d with %d users (users %d to %d)",
                        i+1, len(subBatchSizes), len(subBatch), startIdx, endIdx-1)

                // Process this sub-batch
                if err := processUserBatch(ctx, tokens, subBatch, db); err != nil {
                        return fmt.Errorf("error processing sub-batch %d: %w", i+1, err)
                }

                // Add a delay between sub-batches to avoid rate limits
                if i < len(subBatchSizes)-1 && endIdx < totalUsers {
                        log.Printf("Waiting %s before processing next sub-batch", subBatchDelay)
                        time.Sleep(subBatchDelay)
                }

                // Update start index for next sub-batch
                startIdx = endIdx
        }

        return nil
}

// insertCompleteBatch inserts a batch of users with their complete token data
func insertCompleteBatch(db *sql.DB, batchData map[string]*BatchSnapshot, tokens []Token) error {
        // Start a transaction
        tx, err := db.Begin()
        if err != nil {
                return fmt.Errorf("error starting transaction: %w", err)
        }
        defer tx.Rollback()

        // Create the dynamic SQL for inserting or updating users with all token values using upsert
        insertSQL := "INSERT INTO public.moonwell_user_balances (user_addr, update_time"
        valueSQL := "VALUES ($1, $2"

        // Add token columns to the SQL
        for i, token := range tokens {
                insertSQL += fmt.Sprintf(`, "%s"`, token.Symbol)
                valueSQL += fmt.Sprintf(", $%d", i+3)
        }

        // Add ON CONFLICT clause to handle existing users
        updateSQL := ") " + valueSQL + ") ON CONFLICT (user_addr) DO UPDATE SET update_time = $2"
        for i, token := range tokens {
                updateSQL += fmt.Sprintf(`, "%s" = $%d`, token.Symbol, i+3)
        }

        finalSQL := insertSQL + updateSQL

        // Prepare statement
        stmt, err := tx.Prepare(finalSQL)
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

                // Execute insert/update
                _, err := stmt.Exec(params...)
                if err != nil {
                        return fmt.Errorf("error inserting/updating user %s: %w", userAddr, err)
                }

                successCount++
        }

        // Commit transaction
        if err := tx.Commit(); err != nil {
                return fmt.Errorf("error committing transaction: %w", err)
        }

        log.Printf("Successfully inserted/updated %d users with complete token data", successCount)
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

        log.Printf("Checkpoint saved: last processed index %d", index)
        return nil
}

// loadCheckpoint loads the last processed index from file
func loadCheckpoint() (int, error) {
        // Check if checkpoint file exists
        if _, err := os.Stat(checkpointFile); os.IsNotExist(err) {
                log.Printf("No checkpoint found, starting from beginning")
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

        log.Printf("Checkpoint loaded: resuming from index %d", index)
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
                log.Printf("All users have been processed. Resetting to beginning.")
                startIndex = 0
                lastProcessedIndex = 0
                if err := saveCheckpoint(lastProcessedIndex); err != nil {
                        log.Printf("Warning: Failed to save checkpoint: %v", err)
                }
        }

        log.Printf("Starting from index %d of %d users", startIndex, totalUsers)

        // Process users in batches
        for i := startIndex; i < len(users); i += batchSize {
                end := i + batchSize
                if end > len(users) {
                        end = len(users)
                }

                batchStartTime := time.Now()
                batchUsers := users[i:end]
                log.Printf("Processing batch of users %d to %d of %d (%.1f%% complete)",
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

// updateUsersFromSupabase updates the users.txt file with active users from the same database
func updateUsersFromSupabase() error {
        log.Println("=== Starting User List Update ===")
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
        log.Println("Truncating moonwell_user_balances table...")
        if _, err := db.Exec(truncateBalancesSQL); err != nil {
                log.Printf("Error truncating moonwell_user_balances: %v", err)
        }
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
        client, err := ethclient.Dial(ethRPC)
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

        // Try to ping the database with timeout
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

        // Create context
        ctx = context.Background()

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

        if err := takeSnapshot(ctx, tokens, users, db); err != nil {
                log.Fatalf("Failed to take snapshot: %v", err)
        }

        log.Printf("Initial snapshot completed in %s", time.Since(startTime).Round(time.Second))

        // Set up ticker for periodic snapshots
        ticker := time.NewTicker(snapshotInterval)
        defer ticker.Stop()

        log.Printf("Waiting %s until next snapshot", snapshotInterval)

        // Main loop runs every snapshotInterval
        for range ticker.C {
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

                if err := takeSnapshot(ctx, tokens, users, db); err != nil {
                        log.Printf("Failed to take snapshot: %v", err)
                } else {
                        log.Printf("Periodic snapshot completed in %s", time.Since(startTime).Round(time.Second))
                }

                log.Printf("Waiting %s until next snapshot", snapshotInterval)
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