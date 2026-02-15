#include "test_helpers.h"

class DataTypesTest : public OdbcTest {
protected:
    void TearDown() override {
        drop_table("test_dt");
        OdbcTest::TearDown();
    }

    // Helper: create table, insert via ExecDirect, select back
    void roundtrip_string(const std::string& sql_type, const std::string& insert_val,
                          const std::string& expected) {
        drop_table("test_dt");
        exec_direct(stmt->hstmt, "CREATE TABLE test_dt (val " + sql_type + ")");
        SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
        exec_direct(stmt->hstmt, "INSERT INTO test_dt VALUES (" + insert_val + ")");
        SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
        exec_direct(stmt->hstmt, "SELECT val FROM test_dt");
        ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
        std::string got = get_string_col(stmt->hstmt, 1);
        EXPECT_EQ(got, expected);
    }

    void roundtrip_int(const std::string& sql_type, const std::string& insert_val, int expected) {
        drop_table("test_dt");
        exec_direct(stmt->hstmt, "CREATE TABLE test_dt (val " + sql_type + ")");
        SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
        exec_direct(stmt->hstmt, "INSERT INTO test_dt VALUES (" + insert_val + ")");
        SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
        exec_direct(stmt->hstmt, "SELECT val FROM test_dt");
        ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
        EXPECT_EQ(get_int_col(stmt->hstmt, 1), expected);
    }

    void roundtrip_bigint(const std::string& sql_type, const std::string& insert_val, int64_t expected) {
        drop_table("test_dt");
        exec_direct(stmt->hstmt, "CREATE TABLE test_dt (val " + sql_type + ")");
        SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
        exec_direct(stmt->hstmt, "INSERT INTO test_dt VALUES (" + insert_val + ")");
        SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
        exec_direct(stmt->hstmt, "SELECT val FROM test_dt");
        ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
        EXPECT_EQ(get_bigint_col(stmt->hstmt, 1), expected);
    }

    void roundtrip_double(const std::string& sql_type, const std::string& insert_val, double expected, double tol = 0.001) {
        drop_table("test_dt");
        exec_direct(stmt->hstmt, "CREATE TABLE test_dt (val " + sql_type + ")");
        SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
        exec_direct(stmt->hstmt, "INSERT INTO test_dt VALUES (" + insert_val + ")");
        SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
        exec_direct(stmt->hstmt, "SELECT val FROM test_dt");
        ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
        EXPECT_NEAR(get_double_col(stmt->hstmt, 1), expected, tol);
    }
};

// Integer types
TEST_F(DataTypesTest, Int) { roundtrip_int("INT", "42", 42); }
TEST_F(DataTypesTest, BigInt) { roundtrip_bigint("BIGINT", "9223372036854775807", 9223372036854775807LL); }
TEST_F(DataTypesTest, SmallInt) { roundtrip_int("SMALLINT", "32767", 32767); }
TEST_F(DataTypesTest, TinyInt) { roundtrip_int("TINYINT", "255", 255); }

// Float types
TEST_F(DataTypesTest, Float) { roundtrip_double("FLOAT", "3.14159", 3.14159, 0.00001); }
TEST_F(DataTypesTest, Real) { roundtrip_double("REAL", "2.718", 2.718, 0.01); }

// Decimal types
TEST_F(DataTypesTest, Decimal) { roundtrip_string("DECIMAL(18,4)", "1234.5678", "1234.5678"); }
TEST_F(DataTypesTest, Numeric) { roundtrip_string("NUMERIC(10,2)", "99.99", "99.99"); }

// Bit
TEST_F(DataTypesTest, Bit) { roundtrip_int("BIT", "1", 1); }

// String types
TEST_F(DataTypesTest, Varchar) { roundtrip_string("VARCHAR(100)", "'hello world'", "hello world"); }
TEST_F(DataTypesTest, VarcharMax) { roundtrip_string("VARCHAR(MAX)", "'long text here'", "long text here"); }
TEST_F(DataTypesTest, NVarchar) { roundtrip_string("NVARCHAR(100)", "N'hello'", "hello"); }
TEST_F(DataTypesTest, NVarcharMax) { roundtrip_string("NVARCHAR(MAX)", "N'unicode text'", "unicode text"); }
TEST_F(DataTypesTest, Char) {
    // CHAR(10) pads with spaces
    roundtrip_string("CHAR(10)", "'abc'", "abc       ");
}
TEST_F(DataTypesTest, NChar) {
    roundtrip_string("NCHAR(10)", "N'abc'", "abc       ");
}

// Date/time types
TEST_F(DataTypesTest, Date) { roundtrip_string("DATE", "'2024-01-15'", "2024-01-15"); }
TEST_F(DataTypesTest, Time) { roundtrip_string("TIME", "'13:45:30'", "13:45:30.000"); }
TEST_F(DataTypesTest, DateTime) { roundtrip_string("DATETIME", "'2024-01-15 13:45:30'", "2024-01-15 13:45:30.000"); }
TEST_F(DataTypesTest, DateTime2) { roundtrip_string("DATETIME2", "'2024-01-15 13:45:30.1234567'", "2024-01-15 13:45:30.123"); }
TEST_F(DataTypesTest, SmallDateTime) { roundtrip_string("SMALLDATETIME", "'2024-01-15 13:45:00'", "2024-01-15 13:45:00.000"); }
TEST_F(DataTypesTest, DateTimeOffset) {
    roundtrip_string("DATETIMEOFFSET", "'2024-01-15 13:45:30 +05:30'", "2024-01-15 08:15:30.000 +05:30");
}

// Binary types
TEST_F(DataTypesTest, Binary) {
    drop_table("test_dt");
    exec_direct(stmt->hstmt, "CREATE TABLE test_dt (val BINARY(4))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "INSERT INTO test_dt VALUES (0xDEADBEEF)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT val FROM test_dt");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    SQLCHAR buf[16];
    SQLLEN ind;
    SQLGetData(stmt->hstmt, 1, SQL_C_BINARY, buf, sizeof(buf), &ind);
    EXPECT_EQ(ind, 4);
    EXPECT_EQ(buf[0], 0xDE);
    EXPECT_EQ(buf[1], 0xAD);
    EXPECT_EQ(buf[2], 0xBE);
    EXPECT_EQ(buf[3], 0xEF);
}

TEST_F(DataTypesTest, VarBinary) {
    drop_table("test_dt");
    exec_direct(stmt->hstmt, "CREATE TABLE test_dt (val VARBINARY(100))");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "INSERT INTO test_dt VALUES (0xCAFE)");
    SQLFreeStmt(stmt->hstmt, SQL_CLOSE);
    exec_direct(stmt->hstmt, "SELECT val FROM test_dt");
    ASSERT_EQ(SQLFetch(stmt->hstmt), SQL_SUCCESS);
    SQLCHAR buf[100];
    SQLLEN ind;
    SQLGetData(stmt->hstmt, 1, SQL_C_BINARY, buf, sizeof(buf), &ind);
    EXPECT_EQ(ind, 2);
    EXPECT_EQ(buf[0], 0xCA);
    EXPECT_EQ(buf[1], 0xFE);
}

// UniqueIdentifier
TEST_F(DataTypesTest, UniqueIdentifier) {
    roundtrip_string("UNIQUEIDENTIFIER", "'6F9619FF-8B86-D011-B42D-00CF4FC964FF'",
                     "6F9619FF-8B86-D011-B42D-00CF4FC964FF");
}

// Money types
TEST_F(DataTypesTest, Money) { roundtrip_string("MONEY", "1234.5600", "1234.5600"); }
TEST_F(DataTypesTest, SmallMoney) { roundtrip_string("SMALLMONEY", "99.99", "99.9900"); }
