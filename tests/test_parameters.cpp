#include "test_helpers.h"

class ParametersTest : public OdbcTest {
protected:
    void TearDown() override {
        drop_table("test_param");
        OdbcTest::TearDown();
    }
};

// pyodbc-style DAE string parameter
TEST_F(ParametersTest, StringParamDAE) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (name NVARCHAR(100))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_param VALUES (?)");

    SQLLEN dae_ind = SQL_DATA_AT_EXEC;
    SQLRETURN rc = SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
        SQL_WVARCHAR, 100, 0, (SQLPOINTER)1, 0, &dae_ind);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    rc = SQLExecute(stmt->hstmt);
    ASSERT_EQ(rc, SQL_NEED_DATA);

    SQLPOINTER token;
    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_EQ(rc, SQL_NEED_DATA);

    auto wstr = to_utf16("hello world");
    rc = SQLPutData(stmt->hstmt, (SQLPOINTER)wstr.c_str(), wstr.size() * sizeof(char16_t));
    ASSERT_TRUE(SQL_SUCCEEDED(rc));

    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    // Verify
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT name FROM test_param");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_EQ(get_string_col(stmt->hstmt, 1), "hello world");
}

// Direct int parameter
TEST_F(ParametersTest, IntParam) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (val INT)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_param VALUES (?)");

    SQLINTEGER val = 42;
    SQLLEN ind = sizeof(val);
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG,
        SQL_INTEGER, 0, 0, &val, 0, &ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT val FROM test_param");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 42);
}

// Direct double parameter
TEST_F(ParametersTest, FloatParam) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (val FLOAT)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_param VALUES (?)");

    double val = 3.14159;
    SQLLEN ind = sizeof(val);
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_DOUBLE,
        SQL_DOUBLE, 0, 0, &val, 0, &ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT val FROM test_param");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_NEAR(get_double_col(stmt->hstmt, 1), 3.14159, 0.00001);
}

// NULL parameter
TEST_F(ParametersTest, NullParam) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (val NVARCHAR(100))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_param VALUES (?)");

    SQLLEN ind = SQL_NULL_DATA;
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
        SQL_WVARCHAR, 100, 0, nullptr, 0, &ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT val FROM test_param");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_TRUE(is_null_col(stmt->hstmt, 1));
}

// Multiple params in one query
TEST_F(ParametersTest, MultipleParams) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (id INT, name NVARCHAR(100))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_param VALUES (?, ?)");

    SQLINTEGER id = 1;
    SQLLEN id_ind = sizeof(id);
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG,
        SQL_INTEGER, 0, 0, &id, 0, &id_ind);

    SQLLEN dae_ind = SQL_DATA_AT_EXEC;
    SQLBindParameter(stmt->hstmt, 2, SQL_PARAM_INPUT, SQL_C_WCHAR,
        SQL_WVARCHAR, 100, 0, (SQLPOINTER)2, 0, &dae_ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_EQ(rc, SQL_NEED_DATA);

    SQLPOINTER token;
    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_EQ(rc, SQL_NEED_DATA);

    auto wstr = to_utf16("alice");
    SQLPutData(stmt->hstmt, (SQLPOINTER)wstr.c_str(), wstr.size() * sizeof(char16_t));

    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT id, name FROM test_param");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_EQ(get_int_col(stmt->hstmt, 1), 1);
    EXPECT_EQ(get_string_col(stmt->hstmt, 2), "alice");
}

// Param in WHERE clause
TEST_F(ParametersTest, ParamInWhere) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (id INT, name NVARCHAR(50))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "INSERT INTO test_param VALUES (1, N'alice'), (2, N'bob')");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "SELECT name FROM test_param WHERE id = ?");

    SQLINTEGER id = 2;
    SQLLEN ind = sizeof(id);
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG,
        SQL_INTEGER, 0, 0, &id, 0, &ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_EQ(get_string_col(stmt->hstmt, 1), "bob");
}

// Binary param
TEST_F(ParametersTest, BinaryParam) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (val VARBINARY(100))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_param VALUES (?)");

    unsigned char data[] = {0xDE, 0xAD, 0xBE, 0xEF};
    SQLLEN ind = sizeof(data);
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_BINARY,
        SQL_VARBINARY, 100, 0, data, sizeof(data), &ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT val FROM test_param");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    SQLCHAR buf[100];
    SQLLEN out_ind;
    SQLGetData(stmt->hstmt, 1, SQL_C_BINARY, buf, sizeof(buf), &out_ind);
    EXPECT_EQ(out_ind, 4);
    EXPECT_EQ(buf[0], 0xDE);
}

// Datetime param as string via DAE
TEST_F(ParametersTest, DatetimeParamDAE) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (val DATETIME2)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_param VALUES (?)");

    SQLLEN dae_ind = SQL_DATA_AT_EXEC;
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
        SQL_WVARCHAR, 50, 0, (SQLPOINTER)1, 0, &dae_ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_EQ(rc, SQL_NEED_DATA);

    SQLPOINTER token;
    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_EQ(rc, SQL_NEED_DATA);

    auto wstr = to_utf16("2024-06-15 10:30:00");
    SQLPutData(stmt->hstmt, (SQLPOINTER)wstr.c_str(), wstr.size() * sizeof(char16_t));

    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT CAST(val AS NVARCHAR(50)) FROM test_param");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    std::string result = get_string_col(stmt->hstmt, 1);
    EXPECT_TRUE(result.find("2024-06-15") != std::string::npos);
}

// UUID param as string via DAE
TEST_F(ParametersTest, UuidParamDAE) {
    drop_table("test_param");
    exec_direct(stmt->hstmt, "CREATE TABLE test_param (val UNIQUEIDENTIFIER)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_param VALUES (?)");

    SQLLEN dae_ind = SQL_DATA_AT_EXEC;
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
        SQL_WVARCHAR, 36, 0, (SQLPOINTER)1, 0, &dae_ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_EQ(rc, SQL_NEED_DATA);

    SQLPOINTER token;
    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_EQ(rc, SQL_NEED_DATA);

    auto wstr = to_utf16("6F9619FF-8B86-D011-B42D-00CF4FC964FF");
    SQLPutData(stmt->hstmt, (SQLPOINTER)wstr.c_str(), wstr.size() * sizeof(char16_t));

    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT val FROM test_param");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    std::string result = get_string_col(stmt->hstmt, 1);
    // UUID case-insensitive comparison
    std::string upper_result = result;
    for (auto& c : upper_result) c = toupper(c);
    EXPECT_EQ(upper_result, "6F9619FF-8B86-D011-B42D-00CF4FC964FF");
}
