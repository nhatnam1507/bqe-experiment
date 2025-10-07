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

func TestAlterTableDropColumn(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "users"
	)

	// Use dots for table names (BigQuery standard format)
	tableName := projectID + "." + datasetID + "." + tableID

	t.Log("=== Testing ALTER TABLE DROP COLUMN with BigQuery Emulator ===")

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
    email STRING,
    age INT64
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
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, email, age) 
VALUES (1, 'Alice', 'alice@example.com', 25), (2, 'Bob', 'bob@example.com', 30)`
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

	// Execute ALTER TABLE DROP COLUMN using BigQuery client
	t.Log("6. Executing ALTER TABLE DROP COLUMN via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` DROP COLUMN ` + "`" + `age` + "`"
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
	t.Log("✓ Column dropped successfully via BigQuery client")

	// Verify the column was dropped by querying without the dropped column
	t.Log("7. Verifying column drop...")
	querySQL := `SELECT id, name, email FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query table without dropped column: %v", err)
	}

	t.Log("Data from table without dropped column:")
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

	// Verify the dropped column no longer exists
	t.Log("8. Verifying dropped column no longer exists...")
	oldQuerySQL := `SELECT id, name, email, age FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	_, err = client.Query(oldQuerySQL).Read(ctx)
	if err == nil {
		t.Fatalf("Dropped column should not exist, but query succeeded")
	}
	t.Log("✓ Dropped column correctly no longer exists")

	// Insert new data without the dropped column
	t.Log("9. Inserting new data without dropped column...")
	insertNewSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, email) 
VALUES (3, 'Charlie', 'charlie@example.com')`
	job, err = client.Query(insertNewSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to insert data without dropped column: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for insert without dropped column: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("Insert without dropped column failed: %v", err)
	}
	t.Log("✓ New data inserted successfully without dropped column")

	// Final verification
	t.Log("10. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query final data: %v", err)
	}

	t.Log("Final data from table without dropped column:")
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

	t.Log("=== ALTER TABLE DROP COLUMN test completed successfully! ===")
}

