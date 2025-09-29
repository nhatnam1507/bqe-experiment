# Test BQE ALTER TABLE functionality
test-bqe:
	@echo "Testing BQE ALTER TABLE functionality..."
	CGO_ENABLED=1 CXX=clang++ go run test_bqe_alter.go

# Test library integration
test-library:
	@echo "Testing library integration..."
	CGO_ENABLED=1 CXX=clang++ go run test_library_integration.go

.PHONY: test-bqe test-library

