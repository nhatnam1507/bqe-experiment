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

func TestAlterTableRenameColumn(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "users"
	)

	// Use dots for table names (BigQuery standard format)
	tableName := projectID + "." + datasetID + "." + tableID

	t.Log("=== Testing ALTER TABLE RENAME COLUMN with BigQuery Emulator ===")

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

	// Create initial table
	t.Log("4. Creating initial table...")
	createTableSQL := `
CREATE TABLE ` + "`" + tableName + "`" + ` (
    id INT64,
    name STRING,
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
	t.Log("✓ Table created successfully")

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

	// Execute ALTER TABLE RENAME COLUMN using BigQuery client
	t.Log("6. Executing ALTER TABLE RENAME COLUMN via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` RENAME COLUMN ` + "`" + `name` + "`" + ` TO ` + "`" + `full_name` + "`"
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
	t.Log("✓ Column renamed successfully via BigQuery client")

	// Verify the column was renamed by querying with the new column name
	t.Log("7. Verifying column rename...")
	querySQL := `SELECT id, full_name, email FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query table with renamed column: %v", err)
	}

	t.Log("Data from table with renamed column:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to read row: %v", err)
		}
		t.Logf("  ID: %v, Full Name: %v, Email: %v", row[0], row[1], row[2])
	}

	// Verify the old column name no longer exists
	t.Log("8. Verifying old column name no longer exists...")
	oldQuerySQL := `SELECT id, name, email FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	_, err = client.Query(oldQuerySQL).Read(ctx)
	if err == nil {
		t.Fatalf("Old column name should not exist, but query succeeded")
	}
	t.Log("✓ Old column name correctly no longer exists")

	// Insert new data using the renamed column
	t.Log("9. Inserting new data using renamed column...")
	insertNewSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, full_name, email) 
VALUES (3, 'Charlie', 'charlie@example.com')`
	job, err = client.Query(insertNewSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to insert data with renamed column: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for insert with renamed column: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("Insert with renamed column failed: %v", err)
	}
	t.Log("✓ New data inserted successfully with renamed column")

	// Final verification
	t.Log("10. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query final data: %v", err)
	}

	t.Log("Final data from table with renamed column:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to read row: %v", err)
		}
		t.Logf("  ID: %v, Full Name: %v, Email: %v", row[0], row[1], row[2])
	}

	t.Log("=== ALTER TABLE RENAME COLUMN test completed successfully! ===")
}

