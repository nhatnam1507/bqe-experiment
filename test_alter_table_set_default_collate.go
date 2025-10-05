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

	fmt.Println("=== Testing ALTER TABLE SET DEFAULT COLLATE with BigQuery Emulator ===")

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

	// Create initial table
	fmt.Println("\n4. Creating initial table...")
	createTableSQL := `
CREATE TABLE ` + "`" + tableName + "`" + ` (
    id INT64,
    name STRING,
    description STRING
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
	fmt.Println("✓ Table created successfully")

	// Insert test data
	fmt.Println("\n5. Inserting test data...")
	insertSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, description) 
VALUES (1, 'Alice', 'A person named Alice'), (2, 'Bob', 'A person named Bob')`
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

	// Execute ALTER TABLE SET DEFAULT COLLATE using BigQuery client
	fmt.Println("\n6. Executing ALTER TABLE SET DEFAULT COLLATE via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` SET DEFAULT COLLATE 'und:ci'`
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
	fmt.Println("✓ Default collate set successfully via BigQuery client")

	// Verify the table still works by querying it
	fmt.Println("\n7. Verifying table still works after setting default collate...")
	querySQL := `SELECT id, name, description FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query table: %v", err)
	}

	fmt.Println("Data from table after setting default collate:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		fmt.Printf("  ID: %v, Name: %v, Description: %v\n", row[0], row[1], row[2])
	}

	// Insert new data to verify the table still accepts inserts
	fmt.Println("\n8. Inserting new data to verify table still accepts inserts...")
	insertNewSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, description) 
VALUES (3, 'Charlie', 'A person named Charlie')`
	job, err = client.Query(insertNewSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to insert new data: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for insert new data: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Insert new data failed: %v", err)
	}
	fmt.Println("✓ New data inserted successfully")

	// Test case-insensitive comparison (if supported by the collate)
	fmt.Println("\n9. Testing case-insensitive comparison...")
	caseInsensitiveQuerySQL := `SELECT id, name, description FROM ` + "`" + tableName + "`" + ` WHERE name = 'alice' ORDER BY id`
	it, err = client.Query(caseInsensitiveQuerySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query with case-insensitive comparison: %v", err)
	}

	fmt.Println("Data from case-insensitive query (name = 'alice'):")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		fmt.Printf("  ID: %v, Name: %v, Description: %v\n", row[0], row[1], row[2])
	}

	// Final verification
	fmt.Println("\n10. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query final data: %v", err)
	}

	fmt.Println("Final data from table with default collate:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		fmt.Printf("  ID: %v, Name: %v, Description: %v\n", row[0], row[1], row[2])
	}

	fmt.Println("\n=== ALTER TABLE SET DEFAULT COLLATE test completed successfully! ===")
}
