package testing

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestAlterColumnDropNotNull(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "users"
	)

	// Use dots for table names (BigQuery standard format)
	tableName := projectID + "." + datasetID + "." + tableID

	t.Log("=== Testing ALTER COLUMN DROP NOT NULL with BigQuery Emulator ===")

	// Create BigQuery Emulator server
	t.Log("1. Creating BigQuery Emulator server...")
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatalf("Failed to create BQE server: %v", err)
	}

	// Load initial data
	t.Log("2. Loading initial project and dataset...")
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(datasetID),
			),
		),
	); err != nil {
		t.Fatalf("Failed to load initial data: %v", err)
	}

	if err := bqServer.SetProject(projectID); err != nil {
		t.Fatalf("Failed to set project: %v", err)
	}

	// Create test server
	testServer := bqServer.TestServer()
	defer testServer.Close()

	// Create BigQuery client
	t.Log("3. Creating BigQuery client...")
	client, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("Failed to create BigQuery client: %v", err)
	}
	defer client.Close()

	// Create initial table with NOT NULL constraint
	t.Log("4. Creating initial table with NOT NULL constraint...")
	createTableSQL := `
CREATE TABLE ` + "`" + tableName + "`" + ` (
    id INT64 NOT NULL,
    name STRING NOT NULL,
    email STRING
)`
	job, err := client.Query(createTableSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for table creation: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("Table creation failed: %v", err)
	}
	t.Log("✓ Table created successfully with NOT NULL constraints")

	// Insert test data
	t.Log("5. Inserting test data...")
	insertSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, email) 
VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')`
	job, err = client.Query(insertSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for insert: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	t.Log("✓ Data inserted successfully")

	// Execute ALTER COLUMN DROP NOT NULL using BigQuery client
	t.Log("6. Executing ALTER COLUMN DROP NOT NULL via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` ALTER COLUMN ` + "`" + `name` + "`" + ` DROP NOT NULL`
	t.Logf("Executing: %s", alterSQL)
	job, err = client.Query(alterSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to execute ALTER TABLE: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for ALTER TABLE: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("ALTER TABLE failed: %v", err)
	}
	t.Log("✓ NOT NULL constraint dropped successfully via BigQuery client")

	// Verify the NOT NULL constraint was dropped by inserting NULL values
	t.Log("7. Verifying NOT NULL constraint was dropped...")
	insertNullSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, email) 
VALUES (3, NULL, 'charlie@example.com')`
	job, err = client.Query(insertNullSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to insert NULL value - constraint may not have been dropped: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for insert with NULL: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("Insert with NULL failed: %v", err)
	}
	t.Log("✓ NULL value inserted successfully - NOT NULL constraint was dropped")

	// Query the table to verify the data
	t.Log("8. Verifying data with NULL values...")
	querySQL := `SELECT id, name, email FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}

	t.Log("Data from table with dropped NOT NULL constraint:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to read row: %v", err)
		}
		t.Logf("  ID: %v, Name: %v, Email: %v", row[0], row[1], row[2])
	}

	// Insert another row with NULL to further verify
	t.Log("9. Inserting another row with NULL to further verify...")
	insertAnotherNullSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, email) 
VALUES (4, NULL, NULL)`
	job, err = client.Query(insertAnotherNullSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to insert another NULL value: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for insert with another NULL: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("Insert with another NULL failed: %v", err)
	}
	t.Log("✓ Another NULL value inserted successfully")

	// Final verification
	t.Log("10. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query final data: %v", err)
	}

	t.Log("Final data from table with dropped NOT NULL constraint:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to read row: %v", err)
		}
		t.Logf("  ID: %v, Name: %v, Email: %v", row[0], row[1], row[2])
	}

	t.Log("=== ALTER COLUMN DROP NOT NULL test completed successfully! ===")
}

