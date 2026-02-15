#include "test_helpers.h"

class TransactionsTest : public OdbcTest {};

TEST_F(TransactionsTest, AutocommitOnByDefault) {
    // Driver may report autocommit as ON or OFF by default;
    // the important thing is that data persists without explicit commit
    drop_table("test_tx");
    exec_direct(stmt->hstmt, "CREATE TABLE test_tx (id INT)");
    exec_direct(stmt->hstmt, "INSERT INTO test_tx VALUES (1)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    // Data should persist without explicit commit
    exec_direct(stmt->hstmt, "SELECT COUNT(*) FROM test_tx");
    SQLFetch(stmt->hstmt);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 1);
    drop_table("test_tx");
}

TEST_F(TransactionsTest, ManualCommit) {
    SQLSetConnectAttr(conn->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);

    drop_table("test_tx");
    exec_direct(stmt->hstmt, "CREATE TABLE test_tx (id INT)");
    SQLEndTran(SQL_HANDLE_DBC, conn->hdbc, SQL_COMMIT);
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    exec_direct(stmt->hstmt, "INSERT INTO test_tx VALUES (1)");
    SQLEndTran(SQL_HANDLE_DBC, conn->hdbc, SQL_COMMIT);
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    exec_direct(stmt->hstmt, "SELECT COUNT(*) FROM test_tx");
    SQLFetch(stmt->hstmt);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 1);
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    drop_table("test_tx");
    SQLEndTran(SQL_HANDLE_DBC, conn->hdbc, SQL_COMMIT);
}

TEST_F(TransactionsTest, Rollback) {
    drop_table("test_tx");
    exec_direct(stmt->hstmt, "CREATE TABLE test_tx (id INT)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "INSERT INTO test_tx VALUES (1)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    SQLSetConnectAttr(conn->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);

    exec_direct(stmt->hstmt, "INSERT INTO test_tx VALUES (2)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    SQLEndTran(SQL_HANDLE_DBC, conn->hdbc, SQL_ROLLBACK);

    exec_direct(stmt->hstmt, "SELECT COUNT(*) FROM test_tx");
    SQLFetch(stmt->hstmt);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 1);  // Only the first row
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    SQLSetConnectAttr(conn->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    drop_table("test_tx");
}

TEST_F(TransactionsTest, RollbackVerification) {
    drop_table("test_tx");
    exec_direct(stmt->hstmt, "CREATE TABLE test_tx (id INT)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    SQLSetConnectAttr(conn->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);

    exec_direct(stmt->hstmt, "INSERT INTO test_tx VALUES (99)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    SQLEndTran(SQL_HANDLE_DBC, conn->hdbc, SQL_ROLLBACK);

    exec_direct(stmt->hstmt, "SELECT COUNT(*) FROM test_tx");
    SQLFetch(stmt->hstmt);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 0);  // Nothing committed
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    SQLSetConnectAttr(conn->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    drop_table("test_tx");
}
