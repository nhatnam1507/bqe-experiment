# BigQuery Emulator with ALTER TABLE Support

This repository contains a complete BigQuery Emulator system with enhanced ALTER TABLE functionality, built using a clean architecture with separation between the API layer and SQL engine.

## Repository Structure

```
bqe/
├── go-zetasqlite/          # SQL Engine (Git Submodule)
│   ├── internal/           # Core SQL processing logic
│   ├── README.md          # SQL engine documentation
│   └── ...
├── bigquery-emulator/      # HTTP API Layer (Git Submodule)
│   ├── server/            # REST API handlers
│   ├── internal/          # API layer logic
│   └── ...
├── ALTER_TABLE_COMPREHENSIVE_PLAN.md  # Implementation guide
├── test_bqe_alter.go      # Integration tests
├── Makefile              # Build and test commands
├── go.work               # Go workspace configuration
└── README.md             # This file
```

## Architecture

The system follows a clean separation of concerns:

- **`bigquery-emulator/`**: HTTP API compatibility layer that handles BigQuery REST API requests
- **`go-zetasqlite/`**: SQL processing engine that executes ZetaSQL queries using SQLite

## Features

### ✅ Implemented
- **ALTER TABLE ADD COLUMN**: Fully implemented and tested
- **BigQuery Compatibility**: Full REST API support
- **SQLite Backend**: Efficient local storage
- **ZetaSQL Support**: Google Standard SQL compatibility

### 🚧 Planned
- ALTER TABLE DROP COLUMN
- ALTER TABLE RENAME COLUMN
- ALTER TABLE RENAME TO
- ALTER COLUMN SET DATA TYPE
- And more ALTER operations...

## Quick Start

### Prerequisites
- Go 1.21+
- CGO enabled
- C++ compiler (for go-zetasql dependency)

### Setup
```bash
# Clone with submodules
git clone --recursive <repository-url>
cd bqe

# Initialize submodules (if not cloned recursively)
git submodule update --init --recursive

# Install dependencies
go mod download
```

### Testing
```bash
# Run all tests on main branch to verify merged features
make test-all
```

This single command will:
- Switch go-zetasqlite to main branch
- Run all test_*.go files
- Show which features are merged (✓) vs not merged (❌)

## Development

### Adding New ALTER Operations

1. Follow the [ALTER_TABLE_COMPREHENSIVE_PLAN.md](ALTER_TABLE_COMPREHENSIVE_PLAN.md) guide
2. Only modify files in `go-zetasqlite/` - the API layer will automatically work
3. Test with both SQLite execution and BigQuery client integration

### Key Files
- `go-zetasqlite/internal/stmt_action.go` - Core ALTER TABLE logic
- `go-zetasqlite/internal/spec.go` - Type definitions
- `go-zetasqlite/internal/analyzer.go` - SQL parsing and analysis
- `test_alter_add_column.go` - ALTER TABLE ADD COLUMN integration tests
- `Makefile` - Automated testing with branch management

## Submodules

This repository uses Git submodules to manage the two main components:

- **go-zetasqlite**: SQL processing engine
- **bigquery-emulator**: HTTP API layer

### Working with Submodules

```bash
# Update all submodules to latest
git submodule update --remote

# Update specific submodule
git submodule update --remote go-zetasqlite

# Commit changes in submodule
cd go-zetasqlite
git add .
git commit -m "Your changes"
git push

# Update main repository to point to new submodule commit
cd ..
git add go-zetasqlite
git commit -m "Update go-zetasqlite submodule"
```

## Contributing

1. Make changes in the appropriate submodule
2. Test thoroughly using `make test-bqe`
3. Follow the patterns established in the comprehensive plan
4. Update documentation as needed

## License

This project follows the same license as the upstream repositories.

## Acknowledgments

- Built on top of [go-zetasqlite](https://github.com/goccy/go-zetasqlite)
- Uses [bigquery-emulator](https://github.com/goccy/bigquery-emulator) as the API layer
- Implements Google Standard SQL (ZetaSQL) compatibility
