#include "test_helpers.h"

class MetadataTest : public OdbcTest {};

TEST_F(MetadataTest, DescribeColW) {
    exec_direct(stmt->hstmt, "SELECT 1 AS my_col, N'hello' AS str_col");
    SQLWCHAR colName[256];
    SQLSMALLINT nameLen, dataType, decDigits, nullable;
    SQLULEN colSize;

    SQLRETURN rc = SQLDescribeColW(stmt->hstmt, 1, colName, 256, &nameLen,
        &dataType, &colSize, &decDigits, &nullable);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);
    EXPECT_EQ(from_utf16(colName, nameLen), "my_col");

    rc = SQLDescribeColW(stmt->hstmt, 2, colName, 256, &nameLen,
        &dataType, &colSize, &decDigits, &nullable);
    ASSERT_TRUE(SQL_SUCCEEDED(rc));
    EXPECT_EQ(from_utf16(colName, nameLen), "str_col");
}

TEST_F(MetadataTest, ColAttributeW) {
    exec_direct(stmt->hstmt, "SELECT 1 AS my_col");
    SQLWCHAR buf[256];
    SQLSMALLINT bufLen;
    SQLLEN numericAttr;

    // Get column name
    SQLRETURN rc = SQLColAttributeW(stmt->hstmt, 1, SQL_DESC_NAME,
        buf, sizeof(buf), &bufLen, &numericAttr);
    ASSERT_TRUE(SQL_SUCCEEDED(rc));
    EXPECT_EQ(from_utf16(buf, bufLen / sizeof(SQLWCHAR)), "my_col");
}

TEST_F(MetadataTest, TablesW) {
    // Create a table to find
    drop_table("test_meta_tables");
    exec_direct(stmt->hstmt, "CREATE TABLE test_meta_tables (id INT)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    auto wcat = to_utf16("master");
    auto wschema = to_utf16("dbo");
    auto wtable = to_utf16("test_meta_tables");
    auto wtype = to_utf16("TABLE");

    SQLRETURN rc = SQLTablesW(stmt->hstmt,
        (SQLWCHAR*)wcat.c_str(), (SQLSMALLINT)wcat.size(),
        (SQLWCHAR*)wschema.c_str(), (SQLSMALLINT)wschema.size(),
        (SQLWCHAR*)wtable.c_str(), (SQLSMALLINT)wtable.size(),
        (SQLWCHAR*)wtype.c_str(), (SQLSMALLINT)wtype.size());
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    int count = 0;
    while (SQLFetch(stmt->hstmt) == SQL_SUCCESS) count++;
    EXPECT_GE(count, 1);

    drop_table("test_meta_tables");
}

TEST_F(MetadataTest, ColumnsW) {
    drop_table("test_meta_cols");
    exec_direct(stmt->hstmt, "CREATE TABLE test_meta_cols (id INT, name NVARCHAR(100))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    auto wcat = to_utf16("master");
    auto wschema = to_utf16("dbo");
    auto wtable = to_utf16("test_meta_cols");

    SQLRETURN rc = SQLColumnsW(stmt->hstmt,
        (SQLWCHAR*)wcat.c_str(), (SQLSMALLINT)wcat.size(),
        (SQLWCHAR*)wschema.c_str(), (SQLSMALLINT)wschema.size(),
        (SQLWCHAR*)wtable.c_str(), (SQLSMALLINT)wtable.size(),
        nullptr, 0);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    int count = 0;
    while (SQLFetch(stmt->hstmt) == SQL_SUCCESS) count++;
    EXPECT_EQ(count, 2);

    drop_table("test_meta_cols");
}

TEST_F(MetadataTest, GetTypeInfoW) {
    SQLRETURN rc = SQLGetTypeInfoW(stmt->hstmt, SQL_ALL_TYPES);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    int count = 0;
    while (SQLFetch(stmt->hstmt) == SQL_SUCCESS) count++;
    EXPECT_GT(count, 0);
}
