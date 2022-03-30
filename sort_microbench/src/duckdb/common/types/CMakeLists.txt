add_library_unity(
    duckdb_common_types
  OBJECT
  hugeint.cpp
  string_type.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types>
    PARENT_SCOPE)
