#pragma once

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <cstring>
#include <codecvt>
#include <locale>
#include <iostream>

// Connection string matching pyodbc pattern
static const char* CONN_STR_UTF8 =
    "DRIVER={Furball};SERVER=localhost;DATABASE=master;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes";

// UTF-8 to UTF-16LE conversion
inline std::u16string to_utf16(const std::string& utf8) {
    std::u16string result;
    size_t i = 0;
    while (i < utf8.size()) {
        uint32_t cp = 0;
        unsigned char c = utf8[i];
        if (c < 0x80) { cp = c; i += 1; }
        else if (c < 0xE0) { cp = (c & 0x1F) << 6 | (utf8[i+1] & 0x3F); i += 2; }
        else if (c < 0xF0) { cp = (c & 0x0F) << 12 | (utf8[i+1] & 0x3F) << 6 | (utf8[i+2] & 0x3F); i += 3; }
        else { cp = (c & 0x07) << 18 | (utf8[i+1] & 0x3F) << 12 | (utf8[i+2] & 0x3F) << 6 | (utf8[i+3] & 0x3F); i += 4; }
        if (cp <= 0xFFFF) {
            result.push_back(static_cast<char16_t>(cp));
        } else {
            cp -= 0x10000;
            result.push_back(static_cast<char16_t>(0xD800 + (cp >> 10)));
            result.push_back(static_cast<char16_t>(0xDC00 + (cp & 0x3FF)));
        }
    }
    return result;
}

// UTF-16 to UTF-8 conversion
inline std::string from_utf16(const SQLWCHAR* buf, SQLLEN len_chars) {
    std::string result;
    for (SQLLEN i = 0; i < len_chars; i++) {
        uint32_t cp = buf[i];
        if (cp >= 0xD800 && cp <= 0xDBFF && i + 1 < len_chars) {
            uint32_t lo = buf[++i];
            cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
        }
        if (cp < 0x80) result.push_back(static_cast<char>(cp));
        else if (cp < 0x800) { result.push_back(0xC0 | (cp >> 6)); result.push_back(0x80 | (cp & 0x3F)); }
        else if (cp < 0x10000) { result.push_back(0xE0 | (cp >> 12)); result.push_back(0x80 | ((cp >> 6) & 0x3F)); result.push_back(0x80 | (cp & 0x3F)); }
        else { result.push_back(0xF0 | (cp >> 18)); result.push_back(0x80 | ((cp >> 12) & 0x3F)); result.push_back(0x80 | ((cp >> 6) & 0x3F)); result.push_back(0x80 | (cp & 0x3F)); }
    }
    return result;
}

// Get diagnostic message (pyodbc style - uses W variant)
inline std::string get_diag(SQLSMALLINT handleType, SQLHANDLE handle) {
    SQLWCHAR state[6], msg[1024];
    SQLINTEGER native;
    SQLSMALLINT len;
    if (SQLGetDiagRecW(handleType, handle, 1, state, &native, msg, 1024, &len) == SQL_SUCCESS) {
        return from_utf16(state, 5) + ": " + from_utf16(msg, len);
    }
    return "(no diag)";
}

// Execute a UTF-8 SQL string via SQLExecDirectW
inline SQLRETURN exec_direct(SQLHSTMT stmt, const std::string& sql) {
    auto wsql = to_utf16(sql);
    return SQLExecDirectW(stmt, (SQLWCHAR*)wsql.c_str(), (SQLINTEGER)wsql.size());
}

// Prepare a UTF-8 SQL string via SQLPrepareW
inline SQLRETURN prepare(SQLHSTMT stmt, const std::string& sql) {
    auto wsql = to_utf16(sql);
    return SQLPrepareW(stmt, (SQLWCHAR*)wsql.c_str(), (SQLINTEGER)wsql.size());
}

// Get string result from column via SQLGetData (SQL_C_WCHAR like pyodbc)
inline std::string get_string_col(SQLHSTMT stmt, SQLUSMALLINT col) {
    SQLWCHAR buf[4096];
    SQLLEN ind;
    SQLRETURN rc = SQLGetData(stmt, col, SQL_C_WCHAR, buf, sizeof(buf), &ind);
    if (rc == SQL_SUCCESS && ind != SQL_NULL_DATA) {
        return from_utf16(buf, ind / sizeof(SQLWCHAR));
    }
    return "";
}

// Get int result via SQLGetData
inline int get_int_col(SQLHSTMT stmt, SQLUSMALLINT col) {
    SQLINTEGER val = 0;
    SQLLEN ind;
    SQLGetData(stmt, col, SQL_C_SLONG, &val, sizeof(val), &ind);
    return val;
}

// Get int64 result via SQLGetData
inline int64_t get_bigint_col(SQLHSTMT stmt, SQLUSMALLINT col) {
    SQLBIGINT val = 0;
    SQLLEN ind;
    SQLGetData(stmt, col, SQL_C_SBIGINT, &val, sizeof(val), &ind);
    return val;
}

// Get double result via SQLGetData
inline double get_double_col(SQLHSTMT stmt, SQLUSMALLINT col) {
    double val = 0;
    SQLLEN ind;
    SQLGetData(stmt, col, SQL_C_DOUBLE, &val, sizeof(val), &ind);
    return val;
}

// Check if column is NULL
inline bool is_null_col(SQLHSTMT stmt, SQLUSMALLINT col) {
    SQLWCHAR buf[2];
    SQLLEN ind;
    SQLGetData(stmt, col, SQL_C_WCHAR, buf, sizeof(buf), &ind);
    return ind == SQL_NULL_DATA;
}

// RAII wrappers
struct OdbcEnv {
    SQLHENV henv = SQL_NULL_HENV;
    OdbcEnv() {
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
        SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    }
    ~OdbcEnv() { if (henv != SQL_NULL_HENV) SQLFreeHandle(SQL_HANDLE_ENV, henv); }
};

struct OdbcConn {
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHENV henv;
    bool connected = false;

    OdbcConn(SQLHENV env) : henv(env) {
        SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    }
    bool connect() {
        // Use narrow SQLDriverConnect through unixODBC DM.
        // The DM's SQLDriverConnectW has issues routing to some drivers.
        // pyodbc works because it links differently; our tests use narrow connect
        // but W functions for all statement operations (which is what matters).
        SQLCHAR out[1024];
        SQLSMALLINT outlen;
        SQLRETURN rc = SQLDriverConnect(hdbc, nullptr,
            (SQLCHAR*)CONN_STR_UTF8, SQL_NTS, out, 1024, &outlen, SQL_DRIVER_NOPROMPT);
        connected = SQL_SUCCEEDED(rc);
        return connected;
    }
    ~OdbcConn() {
        if (connected) SQLDisconnect(hdbc);
        if (hdbc != SQL_NULL_HDBC) SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    }
};

struct OdbcStmt {
    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    OdbcStmt(SQLHDBC hdbc) {
        SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    }
    ~OdbcStmt() {
        if (hstmt != SQL_NULL_HSTMT) SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    }
};

// Base test fixture with full connection
class OdbcTest : public ::testing::Test {
protected:
    OdbcEnv* env;
    OdbcConn* conn;
    OdbcStmt* stmt;

    void SetUp() override {
        env = new OdbcEnv();
        ASSERT_TRUE(env->henv != SQL_NULL_HENV);
        conn = new OdbcConn(env->henv);
        ASSERT_TRUE(conn->connect()) << get_diag(SQL_HANDLE_DBC, conn->hdbc);
        stmt = new OdbcStmt(conn->hdbc);
        ASSERT_TRUE(stmt->hstmt != SQL_NULL_HSTMT);
    }

    void TearDown() override {
        delete stmt;
        delete conn;
        delete env;
    }

    void drop_table(const std::string& name) {
        exec_direct(stmt->hstmt, "DROP TABLE IF EXISTS " + name);
    }
};
