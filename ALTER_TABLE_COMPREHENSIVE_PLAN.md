# ALTER TABLE Implementation Guide

## Table of Contents

1. [Overview](#overview)
2. [Architecture Understanding](#architecture-understanding)
3. [Implementation Patterns](#implementation-patterns)
4. [Successfully Implemented: ADD COLUMN](#successfully-implemented-add-column)
5. [Implementation Checklist](#implementation-checklist)
6. [Code Templates](#code-templates)
7. [Critical Files Reference](#critical-files-reference)
8. [Testing Strategy](#testing-strategy)
9. [Common Pitfalls & Solutions](#common-pitfalls--solutions)
10. [Quick Reference](#quick-reference)

---

## Overview

This guide provides a complete reference for implementing ALTER TABLE operations in the BigQuery Emulator. It combines theoretical patterns with real-world implementation experience from successfully implementing `ALTER TABLE ADD COLUMN`.

### Key Principles

- **Clean Architecture**: Separation between API layer (`bigquery-emulator`) and SQL engine (`go-zetasqlite`)
- **Pattern Reuse**: Follow established patterns from existing SQL operations
- **Direct SQL Pattern**: Execute SQLite ALTER TABLE directly with catalog synchronization
- **Comprehensive Testing**: Test at multiple levels (unit, integration, BigQuery client)

---

## Architecture Understanding

### System Components

```
┌─────────────────────┐    ┌─────────────────────┐
│   bigquery-emulator │    │    go-zetasqlite    │
│   (HTTP API Layer)  │    │   (SQL Engine)      │
├─────────────────────┤    ├─────────────────────┤
│ • REST API Handler  │    │ • SQL Parser        │
│ • Request Routing   │───▶│ • Query Execution   │
│ • Response Format   │    │ • Catalog Management│
│ • Metadata Sync     │◀───│ • ZetaSQL Engine    │
└─────────────────────┘    └─────────────────────┘
```

### Why No bigquery-emulator Changes Needed

The `bigquery-emulator` is designed as a **thin HTTP API layer** that:

1. **Receives** BigQuery REST API requests
2. **Delegates** SQL execution to `go-zetasqlite`
3. **Syncs** catalog changes automatically
4. **Returns** BigQuery-formatted responses

**Key Evidence:**
```go
// bigquery-emulator/server/handler.go
response, jobErr := r.server.contentRepo.Query(
    ctx, tx, r.project.ID, "",
    job.Configuration.Query.Query,  // ← Raw SQL passed directly
    job.Configuration.Query.QueryParameters,
)

// Automatic catalog sync
if response.ChangedCatalog.Changed() {
    if err := syncCatalog(ctx, r.server, response.ChangedCatalog); err != nil {
        return nil, err
    }
}
```

---

## Implementation Patterns

### Pattern Analysis

| Pattern | Use Case | Example Operations |
|---------|----------|-------------------|
| **Simple Action** | No metadata changes | SET OPTIONS, SET DEFAULT COLLATE |
| **Direct SQL** | Schema modifications | ADD COLUMN, DROP COLUMN, RENAME COLUMN |
| **Spec-Based** | Complex operations | RENAME TABLE |
| **Multi-Statement** | Multiple operations | Multiple ALTERs in one statement |

### Recommended Approach: Direct SQL Pattern

For most ALTER TABLE operations, use the **Direct SQL Pattern**:

1. **Operation Detection**: String-based detection in `detectAlterOperation()`
2. **SQL Execution**: Execute SQLite ALTER TABLE directly
3. **Catalog Update**: Use `catalog.UpdateTableSpec()` for synchronization
4. **No Preparation**: Return `nil` from `Prepare()` method

---

## Successfully Implemented: ADD COLUMN

### Implementation Summary

✅ **Status**: Fully implemented and tested  
✅ **Pattern**: Direct SQL Pattern  
✅ **Files Modified**: 6 files in `go-zetasqlite/` only  
✅ **Testing**: Unit + Integration + BigQuery Client  

### Key Learnings

#### Architecture Patterns
- **Catalog is Parent**: `Catalog` manages `TableSpec` objects
- **TableSpec is Data**: Contains metadata, doesn't manage catalog
- **Proper Update**: Use `catalog.UpdateTableSpec(ctx, conn, spec)`

#### Critical Issues Solved
1. **Type Mapping**: Added `INTEGER` → `INT64` mapping
2. **Regex Parsing**: Handle backticks around column names
3. **Catalog Sync**: Use `resetCatalog()` instead of `Sync()`
4. **Tables Slice**: Update before calling `resetCatalog()`

#### Best Practices Established
- Follow existing patterns (DMLStmtAction, DropStmtAction)
- Always use catalog methods for updates
- Test with both SQLite and BigQuery client
- Add debug logging during development, remove before commit

---

## Implementation Checklist

### For New ALTER Operations

- [ ] **1. Add Operation Type**
  - [ ] Add to `AlterOperationType` enum in `spec.go`
  - [ ] Update `detectAlterOperation()` method
  - [ ] Update `updateCatalogForOperation()` switch

- [ ] **2. Implement Handler**
  - [ ] Create handler method (e.g., `handleDropColumn`)
  - [ ] Create SQL parser (e.g., `parseDropColumnFromSQL`)
  - [ ] Follow `handleAddColumn` pattern exactly

- [ ] **3. Update Analyzer**
  - [ ] Add operation detection logic
  - [ ] Test with sample SQL statements

- [ ] **4. Testing**
  - [ ] Unit test: SQL parsing and catalog updates
  - [ ] Integration test: Use `test_bqe_alter.go` pattern
  - [ ] BigQuery client test: End-to-end verification
  - [ ] Error test: Invalid SQL, missing columns

- [ ] **5. Documentation**
  - [ ] Update `README.md` status
  - [ ] Remove debug logging
  - [ ] Add to this guide if needed

---

## Code Templates

### Basic Handler Template

```go
func (a *AlterStmtAction) handleDropColumn(ctx context.Context, conn *Conn, spec *TableSpec) error {
    // 1. Parse SQL to extract parameters
    columnName := a.parseDropColumnFromSQL(a.formattedQuery)
    if columnName == "" {
        return fmt.Errorf("failed to parse DROP COLUMN statement")
    }
    
    // 2. Modify the TableSpec
    for i, col := range spec.Columns {
        if col.Name == columnName {
            spec.Columns = append(spec.Columns[:i], spec.Columns[i+1:]...)
            break
        }
    }
    
    // 3. Update catalog and persist changes
    return a.catalog.UpdateTableSpec(ctx, conn, spec)
}
```

### SQL Parser Template

```go
func (a *AlterStmtAction) parseDropColumnFromSQL(sql string) string {
    // Handle backticks around identifiers
    re := regexp.MustCompile("DROP COLUMN\\s+`([^`]+)`")
    matches := re.FindStringSubmatch(sql)
    if len(matches) >= 2 {
        return matches[1]
    }
    return ""
}
```

### Operation Detection Template

```go
func (a *Analyzer) detectAlterOperation(sql string) AlterOperationType {
    sqlUpper := strings.ToUpper(sql)
    
    switch {
    case strings.Contains(sqlUpper, "DROP COLUMN"):
        return AlterTableDropColumn
    // ... other cases
    default:
        return AlterTableSetOptions
    }
}
```

---

## Critical Files Reference

### Files to Modify for New Operations

| File | Purpose | Changes Required |
|------|---------|------------------|
| `spec.go` | Type definitions | Add to `AlterOperationType` enum |
| `stmt_action.go` | Core logic | Add handler + parser methods |
| `analyzer.go` | Operation detection | Update `detectAlterOperation()` |
| `formatter.go` | SQL generation | Add `FormatSQL` methods if needed |
| `README.md` | Documentation | Update feature status |

### Key Methods to Update

```go
// In stmt_action.go
func (a *AlterStmtAction) updateCatalogForOperation(ctx context.Context, conn *Conn) error {
    // Add new case
    case AlterTableDropColumn:
        return a.handleDropColumn(ctx, conn, existingSpec)
}

// In analyzer.go  
func (a *Analyzer) detectAlterOperation(sql string) AlterOperationType {
    // Add new detection
    case strings.Contains(sqlUpper, "DROP COLUMN"):
        return AlterTableDropColumn
}
```

---

## Testing Strategy

### 1. Unit Testing
```go
// Test SQL parsing
func TestParseDropColumnFromSQL(t *testing.T) {
    action := &AlterStmtAction{}
    result := action.parseDropColumnFromSQL("ALTER TABLE users DROP COLUMN `age`")
    assert.Equal(t, "age", result)
}
```

### 2. Integration Testing
```go
// Use test_bqe_alter.go pattern
func TestAlterTableDropColumn(t *testing.T) {
    // 1. Create table
    // 2. Insert data
    // 3. Execute ALTER TABLE DROP COLUMN
    // 4. Verify schema change
    // 5. Test data operations
}
```

### 3. BigQuery Client Testing
```go
// Test with actual BigQuery client
client, err := bigquery.NewClient(ctx, projectID)
if err != nil {
    t.Fatal(err)
}

query := client.Query("ALTER TABLE test.dataset1.users DROP COLUMN age")
job, err := query.Run(ctx)
```

---

## Common Pitfalls & Solutions

### 1. Regex Patterns
❌ **Problem**: Not handling backticks around identifiers  
✅ **Solution**: Use `DROP COLUMN\\s+\`([^`]+)\``

### 2. Type Mapping
❌ **Problem**: Missing BigQuery type mappings  
✅ **Solution**: Add all types to `mapBigQueryTypeToZetaSQLType()`

### 3. Catalog Updates
❌ **Problem**: Direct manipulation of catalog  
✅ **Solution**: Always use `catalog.UpdateTableSpec()`

### 4. Error Handling
❌ **Problem**: Generic error messages  
✅ **Solution**: Provide specific, actionable error messages

### 5. Testing
❌ **Problem**: Only testing SQLite execution  
✅ **Solution**: Test with BigQuery client for end-to-end verification

---

## Quick Reference

### Essential Commands

```bash
# Test the implementation
make test-bqe

# Run specific test
go run test_bqe_alter.go

# Check for linting errors
go vet ./...
```

### Key Patterns to Follow

1. **Always use `catalog.UpdateTableSpec()`** for catalog updates
2. **Handle backticks** in regex patterns for identifiers
3. **Test with BigQuery client** for end-to-end verification
4. **Follow `handleAddColumn` pattern** exactly for new handlers
5. **Add debug logging** during development, remove before commit

### Success Metrics

- ✅ SQLite ALTER TABLE executes successfully
- ✅ Catalog is updated with new schema
- ✅ BigQuery client can query the modified table
- ✅ No linting errors
- ✅ All tests pass

---

## Conclusion

This guide provides everything needed to implement ALTER TABLE operations efficiently and correctly. The patterns established with `ALTER TABLE ADD COLUMN` serve as a proven template for all future implementations.

**Remember**: Only modify `go-zetasqlite/` - the `bigquery-emulator` will automatically work with any new SQL features!
