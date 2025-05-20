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

// Token represents a Moonwell market token
type Token struct {
	Symbol     string `json:"symbol"`
	MTokenAddr string `json:"mTokenAddr"`
	Decimals   int    `json:"decimals"`
}

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

	// Check if table exists
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
		log.Printf("Table %s does not exist. Looking for moonwell_user_balance (without 's')...", tableName)
		
		// Check for table without 's'
		err = db.QueryRow(`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' AND table_name = 'moonwell_user_balance'
			)
		`).Scan(&tableExists)
		if err != nil {
			log.Fatalf("Error checking if table exists: %v", err)
		}
		
		if tableExists {
			tableName = "moonwell_user_balance"
			log.Printf("Found table %s", tableName)
		} else {
			log.Fatalf("Neither moonwell_user_balances nor moonwell_user_balance exist. Please create the table first.")
		}
	}

	// Clear existing data
	log.Printf("Clearing existing data from table %s...", tableName)
	_, err = db.Exec(fmt.Sprintf("DELETE FROM public.%s", tableName))
	if err != nil {
		log.Fatalf("Error clearing data: %v", err)
	}
	log.Printf("Existing data cleared successfully")
	log.Printf("Database reset completed successfully.")
} 