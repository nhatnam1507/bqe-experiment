test-all:
	@echo "Running comprehensive test suite..."
	@echo "=========================================="
	@echo "Testing all available test files..."
	@echo ""
	@failed_tests=""; \
	passed_tests=""; \
	total_tests=0; \
	for test_file in test_*.go; do \
		if [ -f "$$test_file" ]; then \
			total_tests=$$((total_tests + 1)); \
			echo "Running $$test_file..."; \
			echo "----------------------------------------"; \
			if CGO_ENABLED=1 CXX=clang++ go run "$$test_file" 2>/dev/null; then \
				echo "✅ $$test_file PASSED"; \
				passed_tests="$$passed_tests $$test_file"; \
			else \
				echo "❌ $$test_file FAILED"; \
				failed_tests="$$failed_tests $$test_file"; \
			fi; \
			echo ""; \
		fi; \
	done; \
	echo "=========================================="; \
	echo "TEST SUMMARY:"; \
	echo "Total tests: $$total_tests"; \
	if [ -n "$$passed_tests" ]; then \
		echo "✅ Passed tests:$$passed_tests"; \
	fi; \
	if [ -n "$$failed_tests" ]; then \
		echo "❌ Failed tests:$$failed_tests"; \
	else \
		echo "🎉 All tests passed!"; \
	fi; \
	echo "=========================================="

.PHONY: test-all