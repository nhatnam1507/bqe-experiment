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

	fmt.Println("=== Testing ALTER COLUMN DROP DEFAULT with BigQuery Emulator ===")

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

	// Create initial table with default values
	fmt.Println("\n4. Creating initial table with default values...")
	createTableSQL := `
CREATE TABLE ` + "`" + tableName + "`" + ` (
    id INT64,
    name STRING,
    age INT64 DEFAULT 25,
    status STRING DEFAULT 'active'
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
	fmt.Println("✓ Table created successfully with default values")

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

	// Insert data without specifying age and status to test default values
	fmt.Println("\n6. Inserting data without specifying age and status to test default values...")
	insertDefaultSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name) 
VALUES (3, 'Charlie')`
	job, err = client.Query(insertDefaultSQL).Run(ctx)
	if err != nil {
		log.Fatalf("Failed to insert data with defaults: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for insert with defaults: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Insert with defaults failed: %v", err)
	}
	fmt.Println("✓ Data inserted successfully with default values")

	// Verify the data with default values
	fmt.Println("\n7. Verifying data with default values...")
	querySQL := `SELECT id, name, age, status FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query table: %v", err)
	}

	fmt.Println("Data from table with default values:")
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

	// Execute ALTER COLUMN DROP DEFAULT using BigQuery client
	fmt.Println("\n8. Executing ALTER COLUMN DROP DEFAULT via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` ALTER COLUMN ` + "`" + `status` + "`" + ` DROP DEFAULT`
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
	fmt.Println("✓ Column default dropped successfully via BigQuery client")

	// Verify the table still works by querying it
	fmt.Println("\n9. Verifying table still works after dropping column default...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query table: %v", err)
	}

	fmt.Println("Data from table after dropping column default:")
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

	// Test another column default drop
	fmt.Println("\n10. Testing another column default drop...")
	alterSQL2 := `ALTER TABLE ` + "`" + tableName + "`" + ` ALTER COLUMN ` + "`" + `age` + "`" + ` DROP DEFAULT`
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
	fmt.Println("✓ Second column default dropped successfully")

	// Insert new data to verify the table still accepts inserts
	fmt.Println("\n11. Inserting new data to verify table still accepts inserts...")
	insertNewSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, age, status) 
VALUES (4, 'David', 40, 'pending')`
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

	// Final verification
	fmt.Println("\n12. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		log.Fatalf("Failed to query final data: %v", err)
	}

	fmt.Println("Final data from table with dropped column defaults:")
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

	fmt.Println("\n=== ALTER COLUMN DROP DEFAULT test completed successfully! ===")
}
