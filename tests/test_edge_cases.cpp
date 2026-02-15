#include "test_helpers.h"

class EdgeCasesTest : public OdbcTest {
protected:
    void TearDown() override {
        drop_table("test_edge");
        OdbcTest::TearDown();
    }
};

TEST_F(EdgeCasesTest, EmptyStringParam) {
    drop_table("test_edge");
    exec_direct(stmt->hstmt, "CREATE TABLE test_edge (val NVARCHAR(100))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    prepare(stmt->hstmt, "INSERT INTO test_edge VALUES (?)");
    SQLLEN dae_ind = SQL_DATA_AT_EXEC;
    SQLBindParameter(stmt->hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
        SQL_WVARCHAR, 100, 0, (SQLPOINTER)1, 0, &dae_ind);

    SQLRETURN rc = SQLExecute(stmt->hstmt);
    ASSERT_EQ(rc, SQL_NEED_DATA);
    SQLPOINTER token;
    SQLParamData(stmt->hstmt, &token);

    auto wstr = to_utf16("");
    SQLPutData(stmt->hstmt, (SQLPOINTER)wstr.c_str(), 0);
    rc = SQLParamData(stmt->hstmt, &token);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_STMT, stmt->hstmt);

    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT val FROM test_edge");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_EQ(get_string_col(stmt->hstmt, 1), "");
}

TEST_F(EdgeCasesTest, VeryLongString) {
    drop_table("test_edge");
    exec_direct(stmt->hstmt, "CREATE TABLE test_edge (val NVARCHAR(MAX))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    std::string big(4000, 'A');
    exec_direct(stmt->hstmt, "INSERT INTO test_edge VALUES (N'" + big + "')");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    exec_direct(stmt->hstmt, "SELECT val FROM test_edge");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    std::string result = get_string_col(stmt->hstmt, 1);
    EXPECT_EQ(result.size(), 4000u);
}

TEST_F(EdgeCasesTest, UnicodeString) {
    drop_table("test_edge");
    exec_direct(stmt->hstmt, "CREATE TABLE test_edge (val NVARCHAR(100))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    // Chinese chars + emoji via ExecDirect
    exec_direct(stmt->hstmt, u8"INSERT INTO test_edge VALUES (N'\u4F60\u597D')");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    exec_direct(stmt->hstmt, "SELECT val FROM test_edge");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    std::string result = get_string_col(stmt->hstmt, 1);
    EXPECT_EQ(result, u8"\u4F60\u597D");
}

TEST_F(EdgeCasesTest, NullInEveryColumn) {
    drop_table("test_edge");
    exec_direct(stmt->hstmt, "CREATE TABLE test_edge (a INT, b NVARCHAR(50), c FLOAT, d DATE)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "INSERT INTO test_edge VALUES (NULL, NULL, NULL, NULL)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    exec_direct(stmt->hstmt, "SELECT a, b, c, d FROM test_edge");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    EXPECT_TRUE(is_null_col(stmt->hstmt, 1));
    EXPECT_TRUE(is_null_col(stmt->hstmt, 2));
    EXPECT_TRUE(is_null_col(stmt->hstmt, 3));
    EXPECT_TRUE(is_null_col(stmt->hstmt, 4));
}

TEST_F(EdgeCasesTest, WideTable) {
    drop_table("test_edge");
    std::string ddl = "CREATE TABLE test_edge (";
    std::string insert = "INSERT INTO test_edge VALUES (";
    for (int i = 0; i < 25; i++) {
        if (i > 0) { ddl += ", "; insert += ", "; }
        ddl += "c" + std::to_string(i) + " INT";
        insert += std::to_string(i);
    }
    ddl += ")";
    insert += ")";

    exec_direct(stmt->hstmt, ddl);
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, insert);
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);

    exec_direct(stmt->hstmt, "SELECT * FROM test_edge");
    SQLSMALLINT cols = 0;
    SQLNumResultCols(stmt->hstmt, &cols);
    EXPECT_EQ(cols, 25);
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    for (int i = 0; i < 25; i++) {
        EXPECT_EQ(get_int_col(stmt->hstmt, i + 1), i);
    }
}
