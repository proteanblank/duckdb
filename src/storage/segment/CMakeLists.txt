add_library_unity(duckdb_storage_segment
                  OBJECT
                  numeric_segment.cpp
                  string_segment.cpp
        rle_segment.cpp
                  uncompressed_segment.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_storage_segment> PARENT_SCOPE)
