# =============================================================================
# COMPREHENSIVE TESTING - Run all tests on main branch
# =============================================================================
# This target:
# 1. Switches go-zetasqlite to main branch
# 2. Runs all .go test files to verify which features are merged
# 3. Helps identify which features are available vs not merged
# =============================================================================

# Run all tests on main branch to verify merged features
test-all:
	@echo "Running comprehensive test suite on main branch..."
	@echo "=========================================="
	@echo "1. Switching to main branch..."
	cd go-zetasqlite && git checkout main
	@echo "✓ go-zetasqlite is now on main branch"
	@echo ""
	@echo "2. Running all available test files..."
	@echo "=========================================="
	@for test_file in test_*.go; do \
		if [ -f "$$test_file" ]; then \
			echo "Running $$test_file..."; \
			echo "----------------------------------------"; \
			CGO_ENABLED=1 CXX=clang++ go run "$$test_file" || echo "❌ $$test_file failed (feature not merged)"; \
			echo ""; \
		fi; \
	done
	@echo "=========================================="
	@echo "Test suite completed!"
	@echo "✓ Successful tests = features are merged"
	@echo "❌ Failed tests = features are not merged"

# =============================================================================
# FEATURE-SPECIFIC TESTING - Quick test individual features on their branches
# =============================================================================
# These targets:
# 1. Switch to the specific feature branch
# 2. Run the specific test file
# 3. Perfect for development and quick verification
# =============================================================================

# Test ALTER TABLE ADD COLUMN on feature branch
test-alter-table-add-column:
	@echo "Testing ALTER TABLE ADD COLUMN on feature branch..."
	@echo "Switching to feat/alter-table-add-column branch..."
	cd go-zetasqlite && git checkout feat/alter-table-add-column
	@echo "Running ALTER TABLE ADD COLUMN test..."
	CGO_ENABLED=1 CXX=clang++ go run test_alter_table_add_column.go

# Template for future features:
# test-alter-table-<feature>:
# 	@echo "Testing ALTER TABLE <FEATURE> on feature branch..."
# 	@echo "Switching to feat/alter-table-<feature> branch..."
# 	cd go-zetasqlite && git checkout feat/alter-table-<feature>
# 	@echo "Running ALTER TABLE <FEATURE> test..."
# 	CGO_ENABLED=1 CXX=clang++ go run test_alter_table_<feature>.go

.PHONY: test-all test-alter-table-add-column