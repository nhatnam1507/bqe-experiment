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

func TestAlterTableAddColumn(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "users"
	)

	// Use dots for table names (BigQuery standard format)
	tableName := projectID + "." + datasetID + "." + tableID

	t.Log("=== Testing ALTER TABLE ADD COLUMN with BigQuery Emulator ===")

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
    name STRING
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
INSERT INTO ` + "`" + tableName + "`" + ` (id, name) 
VALUES (1, 'Alice'), (2, 'Bob')`
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

	// Execute ALTER TABLE ADD COLUMN using BigQuery client
	t.Log("6. Executing ALTER TABLE ADD COLUMN via BigQuery client...")
	alterSQL := `ALTER TABLE ` + "`" + tableName + "`" + ` ADD COLUMN age INT64`
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
	t.Log("✓ Column added successfully via BigQuery client")

	// Verify the schema change by querying data
	t.Log("7. Verifying schema change...")
	querySQL := `SELECT * FROM ` + "`" + tableName + "`" + ` ORDER BY id`
	it, err := client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query data after alter: %v", err)
	}

	t.Log("Data after ALTER TABLE:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to read row: %v", err)
		}
		if len(row) >= 3 {
			t.Logf("  ID: %v, Name: %v, Age: %v", row[0], row[1], row[2])
		} else {
			t.Logf("  ID: %v, Name: %v, Age: NULL", row[0], row[1])
		}
	}

	// Insert new data with the age column
	t.Log("8. Inserting new data with age column...")
	insertWithAgeSQL := `
INSERT INTO ` + "`" + tableName + "`" + ` (id, name, age) 
VALUES (3, 'Charlie', 25)`
	job, err = client.Query(insertWithAgeSQL).Run(ctx)
	if err != nil {
		t.Fatalf("Failed to insert data with age: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for insert with age: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("Insert with age failed: %v", err)
	}
	t.Log("✓ New data inserted successfully")

	// Final verification
	t.Log("9. Final verification...")
	it, err = client.Query(querySQL).Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query final data: %v", err)
	}

	t.Log("Final data:")
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to read row: %v", err)
		}
		if len(row) >= 3 {
			t.Logf("  ID: %v, Name: %v, Age: %v", row[0], row[1], row[2])
		} else {
			t.Logf("  ID: %v, Name: %v, Age: NULL", row[0], row[1])
		}
	}

	t.Log("=== ALTER TABLE ADD COLUMN test completed successfully! ===")
}

