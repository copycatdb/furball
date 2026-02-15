#include "test_helpers.h"

class ErrorsTest : public OdbcTest {};

TEST_F(ErrorsTest, SyntaxError) {
    SQLRETURN rc = exec_direct(stmt->hstmt, "SELECTT 1");
    EXPECT_FALSE(SQL_SUCCEEDED(rc));
    std::string diag = get_diag(SQL_HANDLE_STMT, stmt->hstmt);
    EXPECT_FALSE(diag.empty());
}

TEST_F(ErrorsTest, TableNotFound) {
    SQLRETURN rc = exec_direct(stmt->hstmt, "SELECT * FROM nonexistent_table_xyz_999");
    EXPECT_FALSE(SQL_SUCCEEDED(rc));
    SQLWCHAR state[6];
    SQLINTEGER native;
    SQLWCHAR msg[1024];
    SQLSMALLINT len;
    SQLGetDiagRecW(SQL_HANDLE_STMT, stmt->hstmt, 1, state, &native, msg, 1024, &len);
    std::string sqlstate = from_utf16(state, 5);
    // S0002 or 42S02 for table not found
    EXPECT_TRUE(sqlstate == "42S02" || sqlstate == "S0002" || sqlstate.substr(0,2) == "42")
        << "Got SQLSTATE: " << sqlstate;
}

TEST_F(ErrorsTest, DuplicateKey) {
    drop_table("test_err");
    exec_direct(stmt->hstmt, "CREATE TABLE test_err (id INT PRIMARY KEY)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "INSERT INTO test_err VALUES (1)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    SQLRETURN rc = exec_direct(stmt->hstmt, "INSERT INTO test_err VALUES (1)");
    EXPECT_FALSE(SQL_SUCCEEDED(rc));

    SQLWCHAR state[6];
    SQLINTEGER native;
    SQLWCHAR msg[1024];
    SQLSMALLINT len;
    SQLGetDiagRecW(SQL_HANDLE_STMT, stmt->hstmt, 1, state, &native, msg, 1024, &len);
    std::string sqlstate = from_utf16(state, 5);
    EXPECT_EQ(sqlstate, "23000");

    drop_table("test_err");
}

TEST_F(ErrorsTest, NullConstraintViolation) {
    drop_table("test_err");
    exec_direct(stmt->hstmt, "CREATE TABLE test_err (id INT NOT NULL)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    SQLRETURN rc = exec_direct(stmt->hstmt, "INSERT INTO test_err VALUES (NULL)");
    EXPECT_FALSE(SQL_SUCCEEDED(rc));

    SQLWCHAR state[6];
    SQLINTEGER native;
    SQLWCHAR msg[1024];
    SQLSMALLINT len;
    SQLGetDiagRecW(SQL_HANDLE_STMT, stmt->hstmt, 1, state, &native, msg, 1024, &len);
    std::string sqlstate = from_utf16(state, 5);
    // 23000 or HY000 depending on driver
    EXPECT_TRUE(sqlstate == "23000" || sqlstate == "HY000") << "Got: " << sqlstate;

    drop_table("test_err");
}

TEST_F(ErrorsTest, VerifySqlstateCodes) {
    // 42000 for syntax errors
    SQLRETURN rc = exec_direct(stmt->hstmt, "THIS IS NOT SQL");
    EXPECT_FALSE(SQL_SUCCEEDED(rc));
    SQLWCHAR state[6];
    SQLINTEGER native;
    SQLWCHAR msg[1024];
    SQLSMALLINT len;
    SQLGetDiagRecW(SQL_HANDLE_STMT, stmt->hstmt, 1, state, &native, msg, 1024, &len);
    std::string sqlstate = from_utf16(state, 5);
    // Syntax errors are typically 42000
    EXPECT_TRUE(sqlstate.substr(0, 2) == "42") << "Got: " << sqlstate;
}
