#include "test_helpers.h"

class GetDataTest : public OdbcTest {};

TEST_F(GetDataTest, GetStringAsWChar) {
    exec_direct(stmt->hstmt, "SELECT N'hello' AS val");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_EQ(get_string_col(stmt->hstmt, 1), "hello");
}

TEST_F(GetDataTest, GetIntAsSLong) {
    exec_direct(stmt->hstmt, "SELECT 12345 AS val");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 12345);
}

TEST_F(GetDataTest, GetFloatAsDouble) {
    exec_direct(stmt->hstmt, "SELECT CAST(3.14 AS FLOAT) AS val");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_NEAR(get_double_col(stmt->hstmt, 1), 3.14, 0.001);
}

TEST_F(GetDataTest, GetNull) {
    exec_direct(stmt->hstmt, "SELECT NULL AS val");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_TRUE(is_null_col(stmt->hstmt, 1));
}

TEST_F(GetDataTest, LargeString) {
    // 4000-char string
    std::string big(4000, 'X');
    exec_direct(stmt->hstmt, "SELECT REPLICATE('X', 4000) AS val");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    std::string result = get_string_col(stmt->hstmt, 1);
    EXPECT_EQ(result.size(), 4000u);
}
