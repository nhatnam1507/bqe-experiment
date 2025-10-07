# BQE Testing Module

This module contains comprehensive test files for the BigQuery Emulator (BQE) project. These tests validate various ALTER TABLE operations and other BigQuery functionality.

## Test Files

- `alter_table_add_column_test.go` - Tests adding columns to existing tables
- `alter_table_drop_column_test.go` - Tests dropping columns from tables
- `alter_table_rename_column_test.go` - Tests renaming columns
- `alter_table_rename_to_test.go` - Tests renaming tables
- `alter_table_set_default_collate_test.go` - Tests setting default collation
- `alter_column_drop_default_test.go` - Tests dropping default values
- `alter_column_drop_not_null_test.go` - Tests dropping NOT NULL constraints
- `alter_column_set_data_type_test.go` - Tests changing column data types
- `alter_column_set_default_test.go` - Tests setting default values
- `alter_column_set_options_test.go` - Tests setting column options

## Running Tests

From the project root, run:
```bash
make test-all
```

Or from within the testing module:
```bash
cd testing
go test -v ./...
```

Or run specific tests:
```bash
cd testing
go test -v -run TestAlterTableAddColumn
```

## Module Dependencies

This module depends on:
- `github.com/goccy/bigquery-emulator` - The main BigQuery emulator
- `github.com/goccy/go-zetasqlite` - ZetaSQLite integration
- `cloud.google.com/go/bigquery` - Google Cloud BigQuery client library

The module references are handled by the workspace configuration in the root `go.work` file, which provides local replacements for the development versions of the emulator modules.
