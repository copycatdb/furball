#include "test_helpers.h"

// a) Connection tests

TEST(Connection, AllocHandles) {
    SQLHENV henv;
    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    ASSERT_TRUE(SQL_SUCCEEDED(rc));
    rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    ASSERT_TRUE(SQL_SUCCEEDED(rc));

    SQLHDBC hdbc;
    rc = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    ASSERT_TRUE(SQL_SUCCEEDED(rc));

    SQLHSTMT hstmt;
    rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    // May fail if not connected - that's ok, just check env+dbc work
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

TEST(Connection, DriverConnect) {
    OdbcEnv env;
    OdbcConn conn(env.henv);
    ASSERT_TRUE(conn.connect()) << get_diag(SQL_HANDLE_DBC, conn.hdbc);
}

TEST(Connection, DisconnectAndFreeHandle) {
    OdbcEnv env;
    SQLHDBC hdbc;
    SQLAllocHandle(SQL_HANDLE_DBC, env.henv, &hdbc);
    SQLCHAR out[1024];
    SQLSMALLINT outlen;
    SQLRETURN rc = SQLDriverConnect(hdbc, nullptr, (SQLCHAR*)CONN_STR_UTF8,
        SQL_NTS, out, 1024, &outlen, SQL_DRIVER_NOPROMPT);
    ASSERT_TRUE(SQL_SUCCEEDED(rc));

    rc = SQLDisconnect(hdbc);
    EXPECT_TRUE(SQL_SUCCEEDED(rc));
    rc = SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    EXPECT_TRUE(SQL_SUCCEEDED(rc));
}

TEST(Connection, MultipleConnections) {
    OdbcEnv env;
    OdbcConn conn1(env.henv);
    OdbcConn conn2(env.henv);
    ASSERT_TRUE(conn1.connect());
    ASSERT_TRUE(conn2.connect());

    // Both should work independently
    OdbcStmt s1(conn1.hdbc), s2(conn2.hdbc);
    EXPECT_TRUE(SQL_SUCCEEDED(exec_direct(s1.hstmt, "SELECT 1")));
    EXPECT_TRUE(SQL_SUCCEEDED(exec_direct(s2.hstmt, "SELECT 2")));
}

TEST(Connection, AutocommitAttribute) {
    OdbcEnv env;
    OdbcConn conn(env.henv);
    ASSERT_TRUE(conn.connect());

    // Test setting autocommit OFF then ON
    SQLRETURN rc = SQLSetConnectAttr(conn.hdbc, SQL_ATTR_AUTOCOMMIT,
        (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    EXPECT_TRUE(SQL_SUCCEEDED(rc));

    rc = SQLSetConnectAttr(conn.hdbc, SQL_ATTR_AUTOCOMMIT,
        (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    EXPECT_TRUE(SQL_SUCCEEDED(rc));
}
