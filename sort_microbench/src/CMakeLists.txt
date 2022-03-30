add_subdirectory(duckdb)

add_library_unity(
  sort_microbench
  OBJECT
  sort_microbench.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:sort_microbench>
    PARENT_SCOPE)

add_executable(smb sort_microbench.cpp ${ALL_OBJECT_FILES})
