package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func main() {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "users"
	)

	// Use dots for table names (BigQuery standard format)
	tableName := projectID + "." + datasetID + "." + tableID

	fmt.Println("=== Testing ALTER COLUMN DROP NOT NULL with BigQuery Emulator ===")

	// Create BigQuery Emulator server
	fmt.Println("\n1. Creating BigQuery Emulator server...")
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		log.Fatalf("Failed to create BQE server: %v", err)
	}

	// Load initial data
	fmt.Println("\n2. Loading initial project and dataset...")
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(datasetID),
			),
		),
	); err != nil {
		log.Fatalf("Failed to load initial data: %v", err)
	}

	if err := bqServer.SetProject(projectID); err != nil {
		log.Fatalf("Failed to set project: %v", err)
	}

	// Create test server
	testServer := bqServer.TestServer()
	defer testServer.Close()

	// Create BigQuery client
	fmt.Println("\n3. Creating BigQuery client...")
	client, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}
	defer client.Close()

	// Create initial table with NOT NULL constraint
	fmt.Println("\n4. Creating initial table with NOT NULL constraint...")
	createTableSQL := `
CREATE TABLE ` + "`" + tableName + "`" + ` (
    id INT64 NOT NULL,
    name STRING NOT NULL,
    email STRING
)`
	job, err := client.Query(createTableSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for table creation: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Table creation failed: %v", err)
	}
	fmt.Println("✓ Table created successfully with NOT NULL constraints")

	// Insert test data
	fmt.Println("\n5. Inserting test data...")
	insertSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, email) 
VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')`
	job, err = client.Query(insertSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for insert: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Insert failed: %v", err)
	}
	fmt.Println("✓ Data inserted successfully")

	// Execute ALTER COLUMN DROP NOT NULL using BigQuery client
	fmt.Println("\n6. Executing ALTER COLUMN DROP NOT NULL via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` ALTER COLUMN ` + "`" + `name` + "`" + ` DROP NOT NULL`
	fmt.Printf("Executing: %s\n", alterSQL)
	job, err = client.Query(alterSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to execute ALTER TABLE: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for ALTER TABLE: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("ALTER TABLE failed: %v", err)
	}
	fmt.Println("✓ NOT NULL constraint dropped successfully via BigQuery client")

	// Verify the NOT NULL constraint was dropped by inserting NULL values
	fmt.Println("\n7. Verifying NOT NULL constraint was dropped...")
	insertNullSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, email) 
VALUES (3, NULL, 'charlie@example.com')`
	job, err = client.Query(insertNullSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to insert NULL value - constraint may not have been dropped: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for insert with NULL: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Insert with NULL failed: %v", err)
	}
	fmt.Println("✓ NULL value inserted successfully - NOT NULL constraint was dropped")

	// Query the table to verify the data
	fmt.Println("\n8. Verifying data with NULL values...")
	querySQL := `SELECT id, name, email FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query table: %v", err)
	}

	fmt.Println("Data from table with dropped NOT NULL constraint:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		fmt.Printf("  ID: %v, Name: %v, Email: %v\n", row[0], row[1], row[2])
	}

	// Insert another row with NULL to further verify
	fmt.Println("\n9. Inserting another row with NULL to further verify...")
	insertAnotherNullSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, email) 
VALUES (4, NULL, NULL)`
	job, err = client.Query(insertAnotherNullSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to insert another NULL value: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for insert with another NULL: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Insert with another NULL failed: %v", err)
	}
	fmt.Println("✓ Another NULL value inserted successfully")

	// Final verification
	fmt.Println("\n10. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query final data: %v", err)
	}

	fmt.Println("Final data from table with dropped NOT NULL constraint:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		fmt.Printf("  ID: %v, Name: %v, Email: %v\n", row[0], row[1], row[2])
	}

	fmt.Println("\n=== ALTER COLUMN DROP NOT NULL test completed successfully! ===")
}
