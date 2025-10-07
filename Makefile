test-all:
	@echo "Running comprehensive test suite..."
	@echo "=========================================="
	@echo "Testing all available test files in testing module..."
	@echo ""
	@cd testing && \
	echo "Running go test with verbose output..."; \
	echo "----------------------------------------"; \
	if CGO_ENABLED=1 CXX=clang++ go test -v ./...; then \
		echo "✅ All tests PASSED"; \
	else \
		echo "❌ Some tests FAILED"; \
	fi; \
	echo "=========================================="

.PHONY: test-all