PACKAGES ?= $(shell go list ./... | grep -v /vendor/)
GOPROXY ?=
TESTS ?= .*
CLEANUP_SCRIPT ?= $(shell pwd)/../misc/cleanup-profraw.sh
COVERAGE_DIR ?= $(shell pwd)/../coverage

ifdef GOPROXY
PROXY := GOPROXY=${GOPROXY}
endif

build:
	go test -o smoke.test -c -race -v ./tests

# WORK_DIR=/tmp \
# NYDUS_BUILDER=/path/to/latest/nydus-image \
# NYDUS_NYDUSD=/path/to/latest/nydusd \
# NYDUS_NYDUSIFY=/path/to/latest/nydusify \
# SKIP_CASES=compressor=lz4_block,fs_version=5 \
# make test
test: build
	golangci-lint run --timeout=5m
	@if [ -n "$$LLVM_PROFILE_FILE" ] && [ -x "$(CLEANUP_SCRIPT)" ]; then \
		echo "Starting background profraw cleanup..."; \
		(while true; do sleep 30; bash "$(CLEANUP_SCRIPT)" "$(COVERAGE_DIR)" 2>/dev/null || true; done) & \
		CLEANUP_PID=$$!; \
		trap "kill $$CLEANUP_PID 2>/dev/null || true" EXIT; \
	fi
	$(CARGO_COV_FLAGS) \
		   sudo -E ./smoke.test -test.v -test.timeout 10m -test.parallel=16 -test.run=$(TESTS)
	@if [ -n "$$LLVM_PROFILE_FILE" ] && [ -x "$(CLEANUP_SCRIPT)" ]; then \
		echo "Running final profraw cleanup..."; \
		bash "$(CLEANUP_SCRIPT)" "$(COVERAGE_DIR)" || true; \
	fi

# PERFORMANCE_TEST_MODE=fs-version-5 \
# PERFORMANCE_TEST_IMAGE=wordpress:latest \
# make test-performance
test-performance: build
	@if [ -n "$$LLVM_PROFILE_FILE" ] && [ -x "$(CLEANUP_SCRIPT)" ]; then \
		echo "Starting background profraw cleanup..."; \
		(while true; do sleep 30; bash "$(CLEANUP_SCRIPT)" "$(COVERAGE_DIR)" 2>/dev/null || true; done) & \
		CLEANUP_PID=$$!; \
		trap "kill $$CLEANUP_PID 2>/dev/null || true" EXIT; \
	fi
	$(CARGO_COV_FLAGS) PERFORMANCE_TEST=True \
		   sudo -E ./smoke.test -test.v -test.timeout 10m -test.parallel=1 -test.run=TestPerformance
	@if [ -n "$$LLVM_PROFILE_FILE" ] && [ -x "$(CLEANUP_SCRIPT)" ]; then \
		echo "Running final profraw cleanup..."; \
		bash "$(CLEANUP_SCRIPT)" "$(COVERAGE_DIR)" || true; \
	fi

# BENCHMARK_TEST_IMAGE=wordpress:6.1.1 \
# BENCHMARK_MODE=fs-version-6 \
# make test-benchmark
test-benchmark: build
	@if [ -n "$$LLVM_PROFILE_FILE" ] && [ -x "$(CLEANUP_SCRIPT)" ]; then \
		echo "Starting background profraw cleanup..."; \
		(while true; do sleep 30; bash "$(CLEANUP_SCRIPT)" "$(COVERAGE_DIR)" 2>/dev/null || true; done) & \
		CLEANUP_PID=$$!; \
		trap "kill $$CLEANUP_PID 2>/dev/null || true" EXIT; \
	fi
	$(CARGO_COV_FLAGS) BENCHMARK_TEST=True \
		   sudo -E ./smoke.test -test.v -test.timeout 10m -test.parallel=1 -test.run=TestBenchmark
	@if [ -n "$$LLVM_PROFILE_FILE" ] && [ -x "$(CLEANUP_SCRIPT)" ]; then \
		echo "Running final profraw cleanup..."; \
		bash "$(CLEANUP_SCRIPT)" "$(COVERAGE_DIR)" || true; \
	fi

# WORK_DIR=/tmp \
# NYDUS_STABLE_VERSION=v2.2.3 \
# NYDUS_STABLE_VERSION_EXPORT=v2_2_3 \
# NYDUS_BUILDER=/path/to/latest/nydus-image \
# NYDUS_NYDUSD=/path/to/latest/nydusd \
# NYDUS_NYDUSIFY=/path/to/latest/nydusify \
# NYDUS_BUILDER_v0_1_0=/path/to/v0.1.0/nydus-image \
# NYDUS_NYDUSD_v0_1_0=/path/to/v0.1.0/nydusd \
# NYDUS_NYDUSIFY_v0_1_0=/path/to/v0.1.0/nydusify \
# NYDUS_BUILDER_$NYDUS_STABLE_VERSION_EXPORT=/path/to/$NYDUS_STABLE_VERSION/nydus-image \
# NYDUS_NYDUSD_$NYDUS_STABLE_VERSION_EXPORT=/path/to/$NYDUS_STABLE_VERSION/nydusd \
# NYDUS_NYDUSIFY_$NYDUS_STABLE_VERSION_EXPORT=/path/to/$NYDUS_STABLE_VERSION/nydusify \
# make test TESTS=TestCompatibility
test-compatibility: build
	$(CARGO_COV_FLAGS) make test TESTS=TestCompatibility

# SNAPSHOTTER_SYSTEM_SOCK=/run/containerd-nydus/system.sock
# SNAPSHOTTER=nydus
# TAKEOVER_TEST_IMAGE=wordpress
# NEW_NYDUSD_BINARY_PATH=target/release/nydusd
test-takeover: build
	@if [ -n "$$LLVM_PROFILE_FILE" ] && [ -x "$(CLEANUP_SCRIPT)" ]; then \
		echo "Starting background profraw cleanup..."; \
		(while true; do sleep 30; bash "$(CLEANUP_SCRIPT)" "$(COVERAGE_DIR)" 2>/dev/null || true; done) & \
		CLEANUP_PID=$$!; \
		trap "kill $$CLEANUP_PID 2>/dev/null || true" EXIT; \
	fi
	$(CARGO_COV_FLAGS) TAKEOVER_TEST=true \
		   sudo -E ./smoke.test -test.v -test.timeout 10m -test.parallel=1 -test.run=TestTakeover
	@if [ -n "$$LLVM_PROFILE_FILE" ] && [ -x "$(CLEANUP_SCRIPT)" ]; then \
		echo "Running final profraw cleanup..."; \
		bash "$(CLEANUP_SCRIPT)" "$(COVERAGE_DIR)" || true; \
	fi
