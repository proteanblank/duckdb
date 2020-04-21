#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace std;

namespace duckdb {
extern bool useLRESegment;
TEST_CASE("Test LRE", "[LRE]") {
	//	useLRESegment = true;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a varchar)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('42')"));
	useLRESegment = false;
}
} // namespace duckdb
