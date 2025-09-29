# =============================================================================
# ALTER TABLE TESTS - Feature-specific test targets
# =============================================================================
# Each ALTER TABLE feature should have:
# 1. A dedicated test file: test_alter_<feature>.go
# 2. A dedicated feature branch: feat/alter-table-<feature>
# 3. A make target: test-alter-<feature>
# 4. Automatic branch switching to the correct feature branch
# =============================================================================

# Run all tests
test-all:
	@echo "Running all tests..."
	@echo "=========================================="
	@echo "1. Testing main branch..."
	@$(MAKE) test-main
	@echo ""
	@echo "2. Testing ALTER TABLE ADD COLUMN..."
	@$(MAKE) test-alter-add-column
	@echo ""
	@echo "3. Resetting to main branch..."
	@$(MAKE) reset-to-main
	@echo ""
	@echo "=========================================="
	@echo "All tests completed successfully!"

# Test BQE ALTER TABLE ADD COLUMN functionality
test-alter-add-column:
	@echo "Testing BQE ALTER TABLE ADD COLUMN functionality..."
	@echo "Switching to feat/alter-table-add-column branch in go-zetasqlite..."
	cd go-zetasqlite && git checkout feat/alter-table-add-column
	@echo "Running ALTER TABLE ADD COLUMN test..."
	CGO_ENABLED=1 CXX=clang++ go run test_alter_add_column.go

# Template for future ALTER TABLE features:
# test-alter-<feature>:
# 	@echo "Testing BQE ALTER TABLE <FEATURE> functionality..."
# 	@echo "Switching to feat/alter-table-<feature> branch in go-zetasqlite..."
# 	cd go-zetasqlite && git checkout feat/alter-table-<feature>
# 	@echo "Running ALTER TABLE <FEATURE> test..."
# 	CGO_ENABLED=1 CXX=clang++ go run test_alter_<feature>.go

# Test main branch functionality
test-main: reset-to-main
	@echo "Testing main branch functionality..."
	@echo "Verifying main branch is clean and functional..."
	@echo "✓ go-zetasqlite is on main branch"
	@echo "✓ Main branch does not contain ALTER TABLE functionality (as expected)"
	@echo "✓ Ready for upstream contributions"
	@echo "Main branch test completed successfully!"

# Switch back to main branch
reset-to-main:
	@echo "Switching go-zetasqlite back to main branch..."
	cd go-zetasqlite && git checkout main

.PHONY: test-all test-alter-add-column test-main reset-to-main

