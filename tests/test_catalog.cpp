#include "test_helpers.h"

class CatalogTest : public OdbcTest {};

TEST_F(CatalogTest, TablesW) {
    drop_table("test_cat_tbl");
    exec_direct(stmt->hstmt, "CREATE TABLE test_cat_tbl (id INT)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    SQLRETURN rc = SQLTablesW(stmt->hstmt, nullptr, 0, nullptr, 0, nullptr, 0, nullptr, 0);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    int count = 0;
    while (SQLFetch(stmt->hstmt) == SQL_SUCCESS) count++;
    EXPECT_GT(count, 0);
    drop_table("test_cat_tbl");
}

TEST_F(CatalogTest, ColumnsW) {
    drop_table("test_cat_cols");
    exec_direct(stmt->hstmt, "CREATE TABLE test_cat_cols (id INT, name NVARCHAR(50), val FLOAT)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    auto wtable = to_utf16("test_cat_cols");
    SQLRETURN rc = SQLColumnsW(stmt->hstmt, nullptr, 0, nullptr, 0,
        (SQLWCHAR*)wtable.c_str(), (SQLSMALLINT)wtable.size(), nullptr, 0);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    int count = 0;
    while (SQLFetch(stmt->hstmt) == SQL_SUCCESS) count++;
    EXPECT_EQ(count, 3);
    drop_table("test_cat_cols");
}

TEST_F(CatalogTest, PrimaryKeysW) {
    drop_table("test_cat_pk");
    exec_direct(stmt->hstmt, "CREATE TABLE test_cat_pk (id INT PRIMARY KEY, name NVARCHAR(50))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    auto wtable = to_utf16("test_cat_pk");
    SQLRETURN rc = SQLPrimaryKeysW(stmt->hstmt, nullptr, 0, nullptr, 0,
        (SQLWCHAR*)wtable.c_str(), (SQLSMALLINT)wtable.size());
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    int count = 0;
    while (SQLFetch(stmt->hstmt) == SQL_SUCCESS) count++;
    EXPECT_EQ(count, 1);
    drop_table("test_cat_pk");
}
