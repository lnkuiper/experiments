add_subdirectory(types)

add_library_unity(
  duckdb_common
  OBJECT
  assert.cpp
  constants.cpp
  exception.cpp
  types.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common>
    PARENT_SCOPE)
