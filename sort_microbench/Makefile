.PHONY: opt clean release trace debug reldebug

opt: release

GENERATOR=
FORCE_COLOR=
WARNINGS_AS_ERRORS=
FORCE_WARN_UNUSED_FLAG=
DISABLE_UNITY_FLAG=
DISABLE_SANITIZER_FLAG=
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif
ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif

clean:
	rm -rf build

release:
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

trace:
	mkdir -p build/trace && \
	cd build/trace && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} -DTRACE -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

reldebug:
	mkdir -p build/reldebug && \
	cd build/reldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build . --config RelWithDebInfo

format:
	find src -iname *.hpp -o -iname *.cpp | xargs clang-format -i
