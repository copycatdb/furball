#include "test_helpers.h"

class ExecutionTest : public OdbcTest {};

TEST_F(ExecutionTest, SelectLiteral) {
    SQLRETURN rc = exec_direct(stmt->hstmt, "SELECT 42 AS val");
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);
    rc = SQLFetch(stmt->hstmt);
    ASSERT_TRUE(SQL_SUCCEEDED(rc));
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 42);
}

TEST_F(ExecutionTest, DDL_CreateDropTable) {
    drop_table("test_ddl");
    SQLRETURN rc = exec_direct(stmt->hstmt, "CREATE TABLE test_ddl (id INT)");
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);
    drop_table("test_ddl");
}

TEST_F(ExecutionTest, DML_InsertUpdateDelete) {
    drop_table("test_dml");
    exec_direct(stmt->hstmt, "CREATE TABLE test_dml (id INT, name NVARCHAR(50))");

    SQLRETURN rc = exec_direct(stmt->hstmt, "INSERT INTO test_dml VALUES (1, N'alice')");
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLLEN rowcount = 0;
    SQLRowCount(stmt->hstmt, &rowcount);
    EXPECT_EQ(rowcount, 1);

    exec_direct(stmt->hstmt, "UPDATE test_dml SET name = N'bob' WHERE id = 1");
    SQLRowCount(stmt->hstmt, &rowcount);
    EXPECT_EQ(rowcount, 1);

    exec_direct(stmt->hstmt, "DELETE FROM test_dml WHERE id = 1");
    SQLRowCount(stmt->hstmt, &rowcount);
    EXPECT_EQ(rowcount, 1);

    drop_table("test_dml");
}

TEST_F(ExecutionTest, PrepareAndExecute) {
    SQLRETURN rc = prepare(stmt->hstmt, "SELECT 99 AS val");
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);
    rc = SQLExecute(stmt->hstmt);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);
    rc = SQLFetch(stmt->hstmt);
    ASSERT_TRUE(SQL_SUCCEEDED(rc));
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 99);
}

TEST_F(ExecutionTest, NumResultCols) {
    exec_direct(stmt->hstmt, "SELECT 1 AS a, 2 AS b, 3 AS c");
    SQLSMALLINT cols = 0;
    SQLNumResultCols(stmt->hstmt, &cols);
    EXPECT_EQ(cols, 3);
}

TEST_F(ExecutionTest, RowCount) {
    drop_table("test_rc");
    exec_direct(stmt->hstmt, "CREATE TABLE test_rc (id INT)");
    exec_direct(stmt->hstmt, "INSERT INTO test_rc VALUES (1), (2), (3)");
    SQLLEN rowcount = 0;
    SQLRowCount(stmt->hstmt, &rowcount);
    EXPECT_EQ(rowcount, 3);
    drop_table("test_rc");
}

TEST_F(ExecutionTest, EmptyResultSet) {
    drop_table("test_empty");
    exec_direct(stmt->hstmt, "CREATE TABLE test_empty (id INT)");
    SQLRETURN rc = exec_direct(stmt->hstmt, "SELECT * FROM test_empty");
    ASSERT_TRUE(SQL_SUCCEEDED(rc));
    rc = SQLFetch(stmt->hstmt);
    EXPECT_EQ(rc, SQL_NO_DATA);
    drop_table("test_empty");
}

TEST_F(ExecutionTest, ReExecuteOnSameStmt) {
    exec_direct(stmt->hstmt, "SELECT 1");
    SQLFetch(stmt->hstmt);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 1);
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    exec_direct(stmt->hstmt, "SELECT 2");
    SQLFetch(stmt->hstmt);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 2);
}

TEST_F(ExecutionTest, FreeStmtClose) {
    exec_direct(stmt->hstmt, "SELECT 1");
    SQLRETURN rc = SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    EXPECT_TRUE(SQL_SUCCEEDED(rc));
    // Should be able to reuse
    rc = exec_direct(stmt->hstmt, "SELECT 2");
    EXPECT_TRUE(SQL_SUCCEEDED(rc));
}
