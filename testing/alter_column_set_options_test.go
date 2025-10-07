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

func TestAlterColumnSetOptions(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "users"
	)

	// Use dots for table names (BigQuery standard format)
	tableName := projectID + "." + datasetID + "." + tableID

	t.Log("=== Testing ALTER COLUMN SET OPTIONS with BigQuery Emulator ===")

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
    description STRING
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
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, description) 
VALUES (1, 'Alice', 'Alice description'), (2, 'Bob', 'Bob description')`
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

	// Execute ALTER COLUMN SET OPTIONS using BigQuery client
	t.Log("6. Executing ALTER COLUMN SET OPTIONS via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` ALTER COLUMN ` + "`" + `description` + "`" + ` SET OPTIONS (description = 'User description field')`
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
	t.Log("✓ Column options set successfully via BigQuery client")

	// Verify the table still works by querying it
	t.Log("7. Verifying table still works after setting column options...")
	querySQL := `SELECT id, name, description FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}

	t.Log("Data from table with column options:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to read row: %v", err)
		}
		t.Logf("  ID: %v, Name: %v, Description: %v", row[0], row[1], row[2])
	}

	// Insert new data to verify the table still accepts inserts
	t.Log("8. Inserting new data to verify table still accepts inserts...")
	insertNewSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, description) 
VALUES (3, 'Charlie', 'Charlie description')`
	job, err = client.Query(insertNewSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to insert new data: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for insert new data: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("Insert new data failed: %v", err)
	}
	t.Log("✓ New data inserted successfully")

	// Final verification
	t.Log("9. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query final data: %v", err)
	}

	t.Log("Final data from table with column options:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to read row: %v", err)
		}
		t.Logf("  ID: %v, Name: %v, Description: %v", row[0], row[1], row[2])
	}

	t.Log("=== ALTER COLUMN SET OPTIONS test completed successfully! ===")
}
