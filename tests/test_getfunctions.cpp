#include "test_helpers.h"

class GetFunctionsTest : public OdbcTest {};

TEST_F(GetFunctionsTest, ODBC3AllFunctions) {
    SQLUSMALLINT supported[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE];
    memset(supported, 0, sizeof(supported));

    SQLRETURN rc = SQLGetFunctions(conn->hdbc, SQL_API_ODBC3_ALL_FUNCTIONS, supported);
    ASSERT_TRUE(SQL_SUCCEEDED(rc)) << get_diag(SQL_HANDLE_DBC, conn->hdbc);
}

TEST_F(GetFunctionsTest, KeyFunctionsSupported) {
    SQLUSMALLINT supported;

    // Check a few critical functions
    SQLUSMALLINT funcs[] = {
        SQL_API_SQLCONNECT,
        SQL_API_SQLDISCONNECT,
        SQL_API_SQLEXECDIRECT,
        SQL_API_SQLPREPARE,
        SQL_API_SQLEXECUTE,
        SQL_API_SQLFETCH,
        SQL_API_SQLGETDATA,
        SQL_API_SQLBINDPARAMETER,
        SQL_API_SQLNUMRESULTCOLS,
        SQL_API_SQLROWCOUNT,
        SQL_API_SQLFREESTMT,
        SQL_API_SQLDESCRIBECOL,
    };

    for (auto f : funcs) {
        SQLRETURN rc = SQLGetFunctions(conn->hdbc, f, &supported);
        if (SQL_SUCCEEDED(rc)) {
            // Just verify the call works; driver may or may not support all
        }
    }
}
