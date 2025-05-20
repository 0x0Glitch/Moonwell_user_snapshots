package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	// Get database connection string from environment
	pgDSN := os.Getenv("PG_DSN")
	if pgDSN == "" {
		// Use the provided connection string if PG_DSN is not set
		pgDSN = "postgresql://postgres.kapgloenrwhsmqcvwjyh:India@hindustan1@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"
	}

	log.Printf("Connecting to database...")
	
	// Connect to database
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
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
		log.Fatalf("Failed to ping database: %v\nPlease check your connection string and ensure PostgreSQL is running.", err)
	}
	log.Println("Successfully connected to database")

	// Check moonwell_user_balances table
	var tableExists bool
	tableName := "moonwell_user_balances"
	
	err = db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' AND table_name = $1
		)
	`, tableName).Scan(&tableExists)
	if err != nil {
		log.Fatalf("Error checking if table exists: %v", err)
	}

	if !tableExists {
		log.Fatalf("Table %s does not exist", tableName)
	}
	
	log.Printf("Table %s exists", tableName)

	// Count rows in the table
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM public." + tableName).Scan(&count)
	if err != nil {
		log.Fatalf("Error counting rows: %v", err)
	}
	
	log.Printf("Table %s has %d rows", tableName, count)

	// Check for duplicates
	rows, err := db.Query(`
		SELECT user_addr, COUNT(*) 
		FROM public.` + tableName + ` 
		GROUP BY user_addr 
		HAVING COUNT(*) > 1
	`)
	if err != nil {
		log.Fatalf("Error checking for duplicates: %v", err)
	}
	defer rows.Close()

	duplicates := false
	fmt.Println("\nChecking for duplicate user_addr entries:")
	for rows.Next() {
		var addr string
		var count int
		if err := rows.Scan(&addr, &count); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		fmt.Printf("User %s has %d entries (DUPLICATE DETECTED)\n", addr, count)
		duplicates = true
	}

	if !duplicates {
		fmt.Println("No duplicates found - each user address appears only once!")
	}

	// Show sample data
	fmt.Println("\nSample data from the table:")
	dataRows, err := db.Query("SELECT user_addr, update_time FROM public." + tableName + " LIMIT 5")
	if err != nil {
		log.Fatalf("Error querying sample data: %v", err)
	}
	defer dataRows.Close()

	fmt.Println("user_addr                                      | update_time")
	fmt.Println("------------------------------------------------|------------------------")
	for dataRows.Next() {
		var addr string
		var updateTime time.Time
		if err := dataRows.Scan(&addr, &updateTime); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		fmt.Printf("%-48s | %s\n", addr, updateTime.Format(time.RFC3339))
	}

	// Show a sample of non-zero token balances if any exist
	fmt.Println("\nSample of non-zero token balances:")

	// First, get some token column names
	tokenColsQuery := `
		SELECT column_name
		FROM information_schema.columns 
		WHERE table_name = $1
		AND column_name NOT IN ('user_addr', 'update_time')
		LIMIT 5
	`
	tokenColRows, err := db.Query(tokenColsQuery, tableName)
	if err != nil {
		fmt.Printf("Error getting token columns: %v\n", err)
		return
	}
	defer tokenColRows.Close()

	var tokenCols []string
	for tokenColRows.Next() {
		var colName string
		if err := tokenColRows.Scan(&colName); err != nil {
			fmt.Printf("Error scanning column name: %v\n", err)
			continue
		}
		tokenCols = append(tokenCols, colName)
	}

	// Now show some users with their balances for these tokens
	fmt.Println("user_addr                                      | token    | balance")
	fmt.Println("------------------------------------------------|----------|------------------")

	for _, tokenCol := range tokenCols {
		// Query for non-zero balances for this token
		query := fmt.Sprintf(`
			SELECT user_addr, '%s' as token, "%s" as balance
			FROM public.%s
			WHERE "%s" IS NOT NULL AND "%s" <> '{0,0}'::numeric[]
			LIMIT 3
		`, tokenCol, tokenCol, tableName, tokenCol, tokenCol)
		
		tokenBalRows, err := db.Query(query)
		if err != nil {
			fmt.Printf("Error querying %s balances: %v\n", tokenCol, err)
			continue
		}
		
		for tokenBalRows.Next() {
			var addr string
			var token string
			var balance []string
			if err := tokenBalRows.Scan(&addr, &token, &balance); err != nil {
				fmt.Printf("Error scanning row: %v\n", err)
				continue
			}
			fmt.Printf("%-48s | %-8s | %v\n", addr, token, balance)
		}
		tokenBalRows.Close()
	}

	// Check for case-insensitive duplicates
	fmt.Println("\nChecking for case-insensitive duplicate addresses:")
	
	// Query to find duplicate addresses ignoring case
	caseDupsQuery := `
		SELECT LOWER(user_addr) as lower_addr, COUNT(*) 
		FROM public.` + tableName + ` 
		GROUP BY LOWER(user_addr)
		HAVING COUNT(*) > 1
	`
	
	caseDupsRows, err := db.Query(caseDupsQuery)
	if err != nil {
		fmt.Printf("Error checking for case-insensitive duplicates: %v\n", err)
	} else {
		defer caseDupsRows.Close()
		
		caseDuplicates := false
		for caseDupsRows.Next() {
			var addr string
			var count int
			if err := caseDupsRows.Scan(&addr, &count); err != nil {
				fmt.Printf("Error scanning row: %v\n", err)
				continue
			}
			caseDuplicates = true
			fmt.Printf("Address %s has %d entries with different case formats\n", addr, count)
		}
		
		if !caseDuplicates {
			fmt.Println("No case-insensitive duplicates found!")
		} else {
			fmt.Println("\nFixing case-insensitive duplicates...")
			// Fix duplicates by standardizing to lowercase and keeping most recent
			fixQuery := `
				WITH ranked_addrs AS (
					SELECT 
						user_addr,
						update_time,
						ROW_NUMBER() OVER (PARTITION BY LOWER(user_addr) ORDER BY update_time DESC) as rn
					FROM public.` + tableName + `
				)
				DELETE FROM public.` + tableName + `
				WHERE user_addr IN (
					SELECT user_addr FROM ranked_addrs WHERE rn > 1
				)
			`
			
			_, err := db.Exec(fixQuery)
			if err != nil {
				fmt.Printf("Error fixing duplicates: %v\n", err)
			} else {
				fmt.Println("Duplicates eliminated - kept most recent entry for each address")
			}
			
			// Standardize all addresses to lowercase
			fmt.Println("Standardizing remaining addresses to lowercase...")
			
			// Get a list of columns in the table for proper column selection
			columnsQuery := `
				SELECT column_name
				FROM information_schema.columns
				WHERE table_name = $1
				ORDER BY ordinal_position
			`
			
			columnRows, err := db.Query(columnsQuery, tableName)
			if err != nil {
				fmt.Printf("Error getting column list: %v\n", err)
				return
			}
			defer columnRows.Close()
			
			var columns []string
			for columnRows.Next() {
				var colName string
				if err := columnRows.Scan(&colName); err != nil {
					fmt.Printf("Error scanning column name: %v\n", err)
					continue
				}
				columns = append(columns, colName)
			}
			
			// Create the base SQL for normalizing addresses
			normalizeSQL := `
				UPDATE public.` + tableName + `
				SET user_addr = LOWER(user_addr)
				WHERE user_addr <> LOWER(user_addr)
			`
			
			_, err = db.Exec(normalizeSQL)
			if err != nil {
				fmt.Printf("Error normalizing addresses: %v\n", err)
			} else {
				fmt.Println("All addresses standardized to lowercase")
			}
		}
	}

	log.Println("Database check complete!")
} 