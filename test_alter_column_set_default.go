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

	fmt.Println("=== Testing ALTER COLUMN SET DEFAULT with BigQuery Emulator ===")

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
    age INT64,
    status STRING
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
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, age, status) 
VALUES (1, 'Alice', 25, 'active'), (2, 'Bob', 30, 'inactive')`
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

	// Execute ALTER COLUMN SET DEFAULT using BigQuery client
	fmt.Println("\n6. Executing ALTER COLUMN SET DEFAULT via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` ALTER COLUMN ` + "`" + `status` + "`" + ` SET DEFAULT 'pending'`
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
	fmt.Println("✓ Column default set successfully via BigQuery client")

	// Verify the table still works by querying it
	fmt.Println("\n7. Verifying table still works after setting column default...")
	querySQL := `SELECT id, name, age, status FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query table: %v", err)
	}

	fmt.Println("Data from table after setting column default:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		fmt.Printf("  ID: %v, Name: %v, Age: %v, Status: %v\n", row[0], row[1], row[2], row[3])
	}

	// Insert new data without specifying the status column to test default value
	fmt.Println("\n8. Inserting new data without specifying status to test default value...")
	insertNewSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, age) 
VALUES (3, 'Charlie', 35)`
	job, err = client.Query(insertNewSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to insert new data without status: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for insert new data without status: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Insert new data without status failed: %v", err)
	}
	fmt.Println("✓ New data inserted successfully without status (should use default)")

	// Test another column default update
	fmt.Println("\n9. Testing another column default update...")
	alterSQL2 := `ALTER TABLE ` + "`" + tableName + "`" + ` ALTER COLUMN ` + "`" + `age` + "`" + ` SET DEFAULT 0`
	fmt.Printf("Executing: %s\n", alterSQL2)
	job, err = client.Query(alterSQL2).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to execute second ALTER TABLE: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for second ALTER TABLE: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Second ALTER TABLE failed: %v", err)
	}
	fmt.Println("✓ Second column default set successfully")

	// Insert new data without specifying the age column to test default value
	fmt.Println("\n10. Inserting new data without specifying age to test default value...")
	insertNewSQL2 := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, status) 
VALUES (4, 'David', 'active')`
	job, err = client.Query(insertNewSQL2).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to insert new data without age: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for insert new data without age: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Insert new data without age failed: %v", err)
	}
	fmt.Println("✓ New data inserted successfully without age (should use default)")

	// Final verification
	fmt.Println("\n11. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query final data: %v", err)
	}

	fmt.Println("Final data from table with column defaults:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to read row: %v", err)
		}
		fmt.Printf("  ID: %v, Name: %v, Age: %v, Status: %v\n", row[0], row[1], row[2], row[3])
	}

	fmt.Println("\n=== ALTER COLUMN SET DEFAULT test completed successfully! ===")
}
