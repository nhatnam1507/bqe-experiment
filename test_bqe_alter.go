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

	fmt.Println("=== Testing ALTER TABLE ADD COLUMN with BigQuery Emulator ===")

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
    name STRING
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
INSERT INTO ` + "`" + tableName + "`" + ` (id, name) 
VALUES (1, 'Alice'), (2, 'Bob')`
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

	// Execute ALTER TABLE ADD COLUMN using BigQuery client
	fmt.Println("\n6. Executing ALTER TABLE ADD COLUMN via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` ADD COLUMN age INT64`
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
	fmt.Println("✓ Column added successfully via BigQuery client")

	// Verify the schema change by querying data
	fmt.Println("\n7. Verifying schema change...")
	querySQL := `SELECT * FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query data after alter: %v", err)
	}

	fmt.Println("Data after ALTER TABLE:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		if len(row) >= 3 {
			fmt.Printf("  ID: %v, Name: %v, Age: %v\n", row[0], row[1], row[2])
		} else {
			fmt.Printf("  ID: %v, Name: %v, Age: NULL\n", row[0], row[1])
		}
	}

	// Insert new data with the age column
	fmt.Println("\n8. Inserting new data with age column...")
	insertWithAgeSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, age) 
VALUES (3, 'Charlie', 25)`
	job, err = client.Query(insertWithAgeSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to insert data with age: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for insert with age: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Insert with age failed: %v", err)
	}
	fmt.Println("✓ New data inserted successfully")

	// Final verification
	fmt.Println("\n9. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query final data: %v", err)
	}

	fmt.Println("Final data:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		if len(row) >= 3 {
			fmt.Printf("  ID: %v, Name: %v, Age: %v\n", row[0], row[1], row[2])
		} else {
			fmt.Printf("  ID: %v, Name: %v, Age: NULL\n", row[0], row[1])
		}
	}

	fmt.Println("\n=== ALTER TABLE ADD COLUMN test completed successfully! ===")
}
