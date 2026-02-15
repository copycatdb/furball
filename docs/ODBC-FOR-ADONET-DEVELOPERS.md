# ODBC for ADO.NET Developers

You know `SqlConnection`, `SqlCommand`, `SqlDataReader` inside out. You've debugged TDS packets and traced parameter encoding through `SqlClient`. Now you need to understand ODBC — the C API that predates all of it. This guide maps what you already know to the ODBC equivalents, with real C code you can compile and run.

---

## 1. The Mental Model

In ADO.NET, the object hierarchy is:

```
SqlConnection → SqlCommand → SqlDataReader
```

In ODBC, it's:

```
HENV → HDBC → HSTMT
(Environment → Connection → Statement)
```

Both are handle-based. The difference: ADO.NET wraps handles in managed objects with finalizers and `IDisposable`. ODBC gives you raw `SQLHANDLE` pointers and expects you to allocate, use, and free them yourself.

| Concept | ADO.NET | ODBC |
|---|---|---|
| Lifetime management | GC + `Dispose()` | Manual `SQLAllocHandle` / `SQLFreeHandle` |
| Error propagation | Exceptions (`SqlException`) | Return codes + `SQLGetDiagRec` |
| String encoding | UTF-16 internally | ANSI (`SQLExecDirect`) or UTF-16 (`SQLExecDirectW`) |
| Column indexing | 0-based | **1-based** |

Think of ODBC as what you'd get if you took `SqlClient`, stripped away the managed wrapper, and exposed the wire protocol layer as a C API. The concepts map almost 1:1 — it's the ergonomics that differ.

---

## 2. Connecting

### C# — What You Know

```csharp
var connStr = "Data Source=localhost;Initial Catalog=mydb;User ID=sa;Password=secret;TrustServerCertificate=true";
using var conn = new SqlConnection(connStr);
conn.Open();
// conn is ready
```

### ODBC — The C Equivalent

```c
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>

SQLHENV henv = SQL_NULL_HENV;
SQLHDBC hdbc = SQL_NULL_HDBC;
SQLRETURN ret;

// Step 1: Allocate environment handle (no ADO.NET equivalent — it's implicit)
ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);

// Step 2: Declare ODBC version (required before any connection)
ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION,
                    (SQLPOINTER)SQL_OV_ODBC3, 0);

// Step 3: Allocate connection handle (like `new SqlConnection()`)
ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

// Step 4: Connect (like `.Open()`)
SQLCHAR connStr[] = "DRIVER={ODBC Driver 18 for SQL Server};"
                    "SERVER=localhost;"
                    "DATABASE=mydb;"
                    "UID=sa;"
                    "PWD=secret;"
                    "TrustServerCertificate=yes;";
SQLCHAR outConnStr[1024];
SQLSMALLINT outConnStrLen;

ret = SQLDriverConnect(hdbc, NULL,
                       connStr, SQL_NTS,
                       outConnStr, sizeof(outConnStr),
                       &outConnStrLen,
                       SQL_DRIVER_NOPROMPT);

if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    // Handle error (see Section 8)
    fprintf(stderr, "Connection failed\n");
}
```

### Connection String Key Mapping

| ADO.NET (`SqlClient`) | ODBC | Notes |
|---|---|---|
| `Data Source` | `SERVER` | |
| `Initial Catalog` | `DATABASE` | |
| `User ID` | `UID` | |
| `Password` | `PWD` | |
| `TrustServerCertificate` | `TrustServerCertificate` | Same key, different values: `true`/`false` vs `yes`/`no` |
| *(implicit)* | `DRIVER` | ODBC needs an explicit driver name — no default |

The `DRIVER` key has no ADO.NET equivalent because `SqlClient` *is* the driver. In ODBC, the driver is a pluggable shared library, and you pick it by name. For SQL Server, it's typically `{ODBC Driver 18 for SQL Server}` or `{FreeTDS}` on Linux.

There's also `SQLConnect` (simpler, uses DSN) vs `SQLDriverConnect` (connection string, like you're used to). Always use `SQLDriverConnect` — DSNs are a relic.

---

## 3. Executing Queries

### ExecuteNonQuery → SQLExecDirect + SQLRowCount

**C#:**
```csharp
using var cmd = new SqlCommand("DELETE FROM users WHERE inactive = 1", conn);
int rowsAffected = cmd.ExecuteNonQuery();
```

**ODBC C:**
```c
SQLHSTMT hstmt = SQL_NULL_HSTMT;
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

ret = SQLExecDirect(hstmt,
    (SQLCHAR*)"DELETE FROM users WHERE inactive = 1", SQL_NTS);

SQLLEN rowCount = 0;
if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
    SQLRowCount(hstmt, &rowCount);
    printf("Rows affected: %ld\n", (long)rowCount);
}

SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
```

### ExecuteReader → SQLExecDirect + SQLFetch + SQLGetData

**C#:**
```csharp
using var cmd = new SqlCommand("SELECT id, name FROM users", conn);
using var reader = cmd.ExecuteReader();
while (reader.Read()) {
    Console.WriteLine($"{reader.GetInt32(0)}: {reader.GetString(1)}");
}
```

**ODBC C:**
```c
SQLHSTMT hstmt = SQL_NULL_HSTMT;
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

ret = SQLExecDirect(hstmt, (SQLCHAR*)"SELECT id, name FROM users", SQL_NTS);

SQLINTEGER id;
SQLCHAR name[256];
SQLLEN id_ind, name_ind;

while (SQLFetch(hstmt) == SQL_SUCCESS) {
    SQLGetData(hstmt, 1, SQL_C_LONG,  &id,   sizeof(id),   &id_ind);
    SQLGetData(hstmt, 2, SQL_C_CHAR,  name,  sizeof(name), &name_ind);
    printf("%d: %s\n", id, name);
}

SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
```

Note: `SQLExecDirect` is equivalent to sending the query. There's no separate "reader" object — the statement handle *is* the reader. You fetch from it directly.

### ExecuteScalar → SQLExecDirect + SQLFetch + SQLGetData(col 1)

**C#:**
```csharp
using var cmd = new SqlCommand("SELECT COUNT(*) FROM users", conn);
int count = (int)cmd.ExecuteScalar();
```

**ODBC C:**
```c
SQLHSTMT hstmt = SQL_NULL_HSTMT;
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

ret = SQLExecDirect(hstmt, (SQLCHAR*)"SELECT COUNT(*) FROM users", SQL_NTS);

SQLINTEGER count = 0;
SQLLEN ind;

if (SQLFetch(hstmt) == SQL_SUCCESS) {
    SQLGetData(hstmt, 1, SQL_C_LONG, &count, sizeof(count), &ind);
    printf("Count: %d\n", count);
}

SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
```

There's no `ExecuteScalar` shortcut — it's just "execute, fetch one row, read column 1."

---

## 4. Reading Results

### The Fetch Loop

**C#:**
```csharp
while (reader.Read()) {                          // returns bool
    string name = reader.GetString(0);            // 0-based
    int age = reader.GetInt32(1);
    bool isNull = reader.IsDBNull(2);
}
```

**ODBC C:**
```c
SQLCHAR name[256];
SQLINTEGER age;
SQLCHAR nullable_col[256];
SQLLEN name_ind, age_ind, nullable_ind;

while (SQLFetch(hstmt) == SQL_SUCCESS) {          // returns SQL_SUCCESS or SQL_NO_DATA
    SQLGetData(hstmt, 1, SQL_C_CHAR, name, sizeof(name), &name_ind);    // 1-based!
    SQLGetData(hstmt, 2, SQL_C_LONG, &age,  sizeof(age),  &age_ind);

    SQLGetData(hstmt, 3, SQL_C_CHAR, nullable_col, sizeof(nullable_col), &nullable_ind);
    if (nullable_ind == SQL_NULL_DATA) {
        printf("Column 3 is NULL\n");             // IsDBNull equivalent
    }
}
```

### Method Mapping

| ADO.NET | ODBC | Watch Out |
|---|---|---|
| `reader.Read()` | `SQLFetch(hstmt) == SQL_SUCCESS` | `SQL_NO_DATA` means end, not error |
| `reader.GetString(0)` | `SQLGetData(hstmt, 1, SQL_C_CHAR, buf, bufLen, &ind)` | **1-based** column index |
| `reader.GetInt32(0)` | `SQLGetData(hstmt, 1, SQL_C_LONG, &val, sizeof(val), &ind)` | `SQLINTEGER` is 32-bit |
| `reader.GetInt64(0)` | `SQLGetData(hstmt, 1, SQL_C_SBIGINT, &val, sizeof(val), &ind)` | |
| `reader.GetDouble(0)` | `SQLGetData(hstmt, 1, SQL_C_DOUBLE, &val, sizeof(val), &ind)` | |
| `reader.IsDBNull(0)` | Check `ind == SQL_NULL_DATA` after `SQLGetData` | No separate call — it's a side effect |
| `reader.GetOrdinal("Name")` | No direct equivalent | Loop `SQLDescribeCol` to match by name (see Section 5) |

The `str_len_or_ind` parameter (the last argument to `SQLGetData`) serves double duty: it returns the length of the data *or* `SQL_NULL_DATA` (-1) if the column is null. This is the ODBC pattern for null-checking — there's no separate `IsDBNull`.

For strings, if the data is longer than your buffer, `SQLGetData` returns `SQL_SUCCESS_WITH_INFO` and you need to call it again to get the rest. It's like a streaming read. ADO.NET handles this internally with dynamic buffers.

---

## 5. Column Metadata

**C#:**
```csharp
int fieldCount = reader.FieldCount;
for (int i = 0; i < fieldCount; i++) {
    string name = reader.GetName(i);
    Type type = reader.GetFieldType(i);
}
```

**ODBC C:**
```c
SQLSMALLINT numCols;
SQLNumResultCols(hstmt, &numCols);                // reader.FieldCount

for (SQLUSMALLINT i = 1; i <= numCols; i++) {     // 1-based!
    SQLCHAR colName[256];
    SQLSMALLINT colNameLen, dataType, decimalDigits, nullable;
    SQLULEN colSize;

    SQLDescribeCol(hstmt, i,
                   colName, sizeof(colName), &colNameLen,
                   &dataType,       // SQL_INTEGER, SQL_VARCHAR, etc.
                   &colSize,        // e.g., 255 for VARCHAR(255)
                   &decimalDigits,
                   &nullable);      // SQL_NULLABLE, SQL_NO_NULLS

    printf("Column %d: %s (type=%d, size=%lu)\n",
           i, colName, dataType, (unsigned long)colSize);
}
```

`SQLDescribeCol` gives you name + type in one call. For more detailed attributes (display size, auto-increment, etc.), use `SQLColAttribute`:

```c
SQLLEN isAutoIncrement;
SQLColAttribute(hstmt, 1, SQL_DESC_AUTO_UNIQUE_VALUE,
                NULL, 0, NULL, &isAutoIncrement);
```

### Implementing GetOrdinal

ODBC has no `GetOrdinal`. Roll your own:

```c
SQLUSMALLINT get_ordinal(SQLHSTMT hstmt, const char* target) {
    SQLSMALLINT numCols;
    SQLNumResultCols(hstmt, &numCols);
    for (SQLUSMALLINT i = 1; i <= numCols; i++) {
        SQLCHAR colName[256];
        SQLSMALLINT nameLen;
        SQLDescribeCol(hstmt, i, colName, sizeof(colName), &nameLen,
                       NULL, NULL, NULL, NULL);
        if (strcasecmp((char*)colName, target) == 0)
            return i;
    }
    return 0; // not found
}
```

---

## 6. Parameters

### Named vs Positional

ADO.NET uses named parameters (`@name`). ODBC uses positional markers (`?`). This is probably the single most annoying difference.

**C#:**
```csharp
using var cmd = new SqlCommand(
    "INSERT INTO users (name, age) VALUES (@name, @age)", conn);
cmd.Parameters.AddWithValue("@name", "Alice");
cmd.Parameters.AddWithValue("@age", 30);
cmd.ExecuteNonQuery();
```

**ODBC C:**
```c
SQLHSTMT hstmt = SQL_NULL_HSTMT;
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

// Prepare with ? markers (positional, not named)
ret = SQLPrepare(hstmt,
    (SQLCHAR*)"INSERT INTO users (name, age) VALUES (?, ?)", SQL_NTS);

// Bind parameter 1: name (VARCHAR)
SQLCHAR nameVal[] = "Alice";
SQLLEN nameInd = SQL_NTS;
SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT,
                 SQL_C_CHAR, SQL_VARCHAR,
                 50,           // column size
                 0,            // decimal digits
                 nameVal, sizeof(nameVal), &nameInd);

// Bind parameter 2: age (INTEGER)
SQLINTEGER ageVal = 30;
SQLLEN ageInd = sizeof(ageVal);
SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT,
                 SQL_C_LONG, SQL_INTEGER,
                 0, 0,
                 &ageVal, sizeof(ageVal), &ageInd);

ret = SQLExecute(hstmt);  // Execute the prepared statement

SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
```

### Key Differences

- `SQLPrepare` + `SQLExecute` is the ODBC equivalent of parameterized execution. `SQLExecDirect` doesn't support bound parameters — you must prepare first.
- `SQLBindParameter` takes both the C type (`SQL_C_CHAR`) and the SQL type (`SQL_VARCHAR`). The driver does the conversion.
- Parameters are bound to *buffers*. The driver reads the buffer at `SQLExecute` time, not at bind time. So you can rebind, change the value, and re-execute without re-preparing.
- To pass NULL: set the indicator to `SQL_NULL_DATA`.

```c
SQLLEN nullInd = SQL_NULL_DATA;
SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT,
                 SQL_C_CHAR, SQL_VARCHAR,
                 50, 0, NULL, 0, &nullInd);
```

> **furball note:** Real ODBC drivers (like Microsoft's) use TDS RPC calls for parameterized queries, sending parameter metadata and values separately. furball currently uses string substitution — it formats the parameter values into the SQL text before sending. This means parameterized queries in furball are syntactic sugar, not true parameterization. Something to keep in mind for testing.

---

## 7. Transactions

### C#

```csharp
using var tx = conn.BeginTransaction();
try {
    using var cmd = new SqlCommand("UPDATE accounts SET balance = balance - 100 WHERE id = 1", conn, tx);
    cmd.ExecuteNonQuery();

    using var cmd2 = new SqlCommand("UPDATE accounts SET balance = balance + 100 WHERE id = 2", conn, tx);
    cmd2.ExecuteNonQuery();

    tx.Commit();
} catch {
    tx.Rollback();
    throw;
}
```

### ODBC C

```c
// Turn off autocommit (like BeginTransaction)
SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
                  (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);

SQLHSTMT hstmt = SQL_NULL_HSTMT;
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

ret = SQLExecDirect(hstmt,
    (SQLCHAR*)"UPDATE accounts SET balance = balance - 100 WHERE id = 1",
    SQL_NTS);

if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);
    goto cleanup;
}

ret = SQLExecDirect(hstmt,
    (SQLCHAR*)"UPDATE accounts SET balance = balance + 100 WHERE id = 2",
    SQL_NTS);

if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);
    goto cleanup;
}

// Commit
SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);

cleanup:
SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

// Restore autocommit
SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
                  (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
```

ODBC defaults to autocommit ON — same as ADO.NET without an explicit transaction. `SQLEndTran` is how you commit or rollback. Note it takes the *connection* handle, not a statement handle.

You can reuse the same `HSTMT` for multiple queries within a transaction — unlike ADO.NET where each `SqlCommand` is a separate object. The statement handle is just an execution context.

---

## 8. Error Handling

### C# — Exceptions

```csharp
try {
    cmd.ExecuteNonQuery();
} catch (SqlException ex) {
    Console.WriteLine($"Error {ex.Number}: {ex.Message}");
    Console.WriteLine($"State: {ex.State}");
    foreach (SqlError err in ex.Errors) {
        Console.WriteLine($"  [{err.Class}] {err.Message}");
    }
}
```

### ODBC — Return Codes + Diagnostic Records

Every ODBC function returns a `SQLRETURN`:

| Return Code | Meaning | ADO.NET Equivalent |
|---|---|---|
| `SQL_SUCCESS` | All good | No exception |
| `SQL_SUCCESS_WITH_INFO` | Worked, but there are warnings | No exception (info in `SqlConnection.InfoMessage`) |
| `SQL_ERROR` | Failed | `SqlException` thrown |
| `SQL_NO_DATA` | No more rows / no rows affected | `reader.Read()` returns `false` |
| `SQL_INVALID_HANDLE` | Bug — you passed a bad handle | `NullReferenceException` vibes |

Here's a reusable error extraction function:

```c
void print_odbc_error(SQLSMALLINT handleType, SQLHANDLE handle) {
    SQLCHAR sqlState[6];
    SQLINTEGER nativeError;
    SQLCHAR message[1024];
    SQLSMALLINT messageLen;
    SQLSMALLINT i = 1;

    while (SQLGetDiagRec(handleType, handle, i,
                         sqlState, &nativeError,
                         message, sizeof(message), &messageLen)
           == SQL_SUCCESS)
    {
        fprintf(stderr, "[%s] (%d) %s\n", sqlState, nativeError, message);
        i++;
    }
}
```

Usage pattern:

```c
ret = SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM nonexistent", SQL_NTS);
if (ret == SQL_ERROR) {
    print_odbc_error(SQL_HANDLE_STMT, hstmt);
    // Output: [42S02] (208) Invalid object name 'nonexistent'.
}
```

The `SQLSTATE` is a 5-character code standardized by SQL/ISO. It's like `SqlException.State` but more structured. `42S02` = table not found. `42000` = syntax error. `08001` = unable to connect. The `nativeError` is the SQL Server error number — same as `SqlException.Number`.

Multiple diagnostic records can exist (like `SqlException.Errors` having multiple entries). The `i` parameter to `SQLGetDiagRec` iterates through them, starting at 1.

---

## 9. Schema Discovery

### C#

```csharp
DataTable tables = conn.GetSchema("Tables");
DataTable columns = conn.GetSchema("Columns", new[] { null, null, "users" });
```

### ODBC — Catalog Functions

ODBC has dedicated functions that return result sets — you fetch from them exactly like a regular query.

```c
SQLHSTMT hstmt = SQL_NULL_HSTMT;
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

// List all tables (like GetSchema("Tables"))
ret = SQLTables(hstmt,
    NULL, 0,                  // catalog (database) — NULL = current
    NULL, 0,                  // schema — NULL = all
    NULL, 0,                  // table name — NULL = all
    (SQLCHAR*)"TABLE", SQL_NTS);  // table type filter

// Fetch results — same pattern as any query
SQLCHAR tableName[256];
SQLLEN ind;
while (SQLFetch(hstmt) == SQL_SUCCESS) {
    SQLGetData(hstmt, 3, SQL_C_CHAR, tableName, sizeof(tableName), &ind);
    printf("Table: %s\n", tableName);
}
SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
```

```c
// List columns for a specific table (like GetSchema("Columns"))
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
ret = SQLColumns(hstmt,
    NULL, 0,                     // catalog
    NULL, 0,                     // schema
    (SQLCHAR*)"users", SQL_NTS,  // table name
    NULL, 0);                    // column name — NULL = all

SQLCHAR colName[256];
SQLSMALLINT dataType;
SQLLEN colNameInd, dataTypeInd;
while (SQLFetch(hstmt) == SQL_SUCCESS) {
    SQLGetData(hstmt, 4, SQL_C_CHAR, colName, sizeof(colName), &colNameInd);
    SQLGetData(hstmt, 5, SQL_C_SHORT, &dataType, sizeof(dataType), &dataTypeInd);
    printf("Column: %s (type: %d)\n", colName, dataType);
}
SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
```

Full list of catalog functions:

| ODBC Function | ADO.NET Equivalent |
|---|---|
| `SQLTables` | `GetSchema("Tables")` |
| `SQLColumns` | `GetSchema("Columns")` |
| `SQLPrimaryKeys` | `GetSchema("IndexColumns")` with PK filter |
| `SQLForeignKeys` | `GetSchema("ForeignKeys")` |
| `SQLGetTypeInfo` | `GetSchema("DataTypes")` |
| `SQLStatistics` | `GetSchema("Indexes")` |
| `SQLProcedures` | `GetSchema("Procedures")` |
| `SQLProcedureColumns` | `GetSchema("ProcedureParameters")` |

These all return well-defined result set schemas documented in the ODBC spec. Column positions are standardized — e.g., `SQLTables` always returns TABLE_CAT in column 1, TABLE_SCHEM in column 2, TABLE_NAME in column 3, etc.

---

## 10. Cleanup

### C# — `using` / `Dispose()`

```csharp
using var conn = new SqlConnection(connStr);
using var cmd = conn.CreateCommand();
using var reader = cmd.ExecuteReader();
// Everything cleaned up automatically in reverse order
```

### ODBC — Manual `SQLFreeHandle`

```c
// Free in reverse order of allocation
SQLFreeHandle(SQL_HANDLE_STMT, hstmt);  // Free statement first

SQLDisconnect(hdbc);                     // Disconnect before freeing
SQLFreeHandle(SQL_HANDLE_DBC, hdbc);     // Free connection

SQLFreeHandle(SQL_HANDLE_ENV, henv);     // Free environment last
```

Order matters. Freeing a connection handle before its statement handles is undefined behavior. Think of it like `Dispose()` — you wouldn't dispose a `SqlConnection` while a `SqlDataReader` is still open (well, you *can* in ADO.NET because it handles the cascade, but in ODBC you do it yourself).

A full program skeleton:

```c
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <stdlib.h>

int main() {
    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    SQLRETURN ret;

    // Setup
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    SQLCHAR connStr[] = "DRIVER={ODBC Driver 18 for SQL Server};"
                        "SERVER=localhost;DATABASE=mydb;"
                        "UID=sa;PWD=secret;"
                        "TrustServerCertificate=yes;";
    ret = SQLDriverConnect(hdbc, NULL, connStr, SQL_NTS,
                           NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        fprintf(stderr, "Connect failed\n");
        goto cleanup;
    }

    // Work
    SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    ret = SQLExecDirect(hstmt, (SQLCHAR*)"SELECT @@VERSION", SQL_NTS);
    if (ret == SQL_SUCCESS) {
        SQLCHAR version[1024];
        SQLLEN ind;
        if (SQLFetch(hstmt) == SQL_SUCCESS) {
            SQLGetData(hstmt, 1, SQL_C_CHAR, version, sizeof(version), &ind);
            printf("%s\n", version);
        }
    }

cleanup:
    if (hstmt != SQL_NULL_HSTMT) SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    if (hdbc != SQL_NULL_HDBC) {
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    }
    if (henv != SQL_NULL_HENV) SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
```

Compile with: `gcc -o example example.c -lodbc`

---

## 11. Quick Reference Table

| ADO.NET | ODBC | Notes |
|---|---|---|
| `SqlConnection` | `HDBC` + `SQLDriverConnect` | ODBC also needs `HENV` allocated first |
| `SqlCommand` | `HSTMT` + `SQLExecDirect` / `SQLPrepare` + `SQLExecute` | Statement handle *is* the command |
| `SqlDataReader` | `HSTMT` + `SQLFetch` + `SQLGetData` | Same handle as the command — no separate reader |
| `SqlParameter` | `SQLBindParameter` | Positional `?` markers, not `@name` |
| `SqlTransaction` | `SQLSetConnectAttr(AUTOCOMMIT OFF)` + `SQLEndTran` | No transaction object — it's connection-level state |
| `SqlException` | `SQLGetDiagRec` | Must call after every failed return code |
| `reader.Read()` | `SQLFetch()` | Returns `SQL_SUCCESS` / `SQL_NO_DATA` |
| `reader.GetString(i)` | `SQLGetData(i+1, SQL_C_CHAR, ...)` | 0-based → 1-based |
| `reader.FieldCount` | `SQLNumResultCols` | |
| `reader.GetName(i)` | `SQLDescribeCol(i+1, ...)` | |
| `conn.GetSchema()` | `SQLTables` / `SQLColumns` / etc. | Return result sets |
| `cmd.ExecuteNonQuery()` | `SQLExecDirect` + `SQLRowCount` | |
| `cmd.ExecuteScalar()` | `SQLExecDirect` + `SQLFetch` + `SQLGetData(1, ...)` | No shortcut — manual |
| `using` / `Dispose()` | `SQLFreeHandle` | Manual, reverse order |
| `conn.Open()` | `SQLDriverConnect` | |
| `conn.Close()` | `SQLDisconnect` | |
| `Connection pooling` | *(Driver Manager may pool)* | Not guaranteed; DM-dependent |

---

## 12. Key Gotchas for ADO.NET Developers

1. **1-based column indexing.** ODBC columns start at 1. ADO.NET starts at 0. You *will* get this wrong at least once. `SQLGetData(hstmt, 0, ...)` is invalid.

2. **Manual memory management.** You provide the buffers for `SQLGetData`. If your buffer is too small, you get truncated data with `SQL_SUCCESS_WITH_INFO`. There's no auto-growing `string` — you're in C, you manage memory.

3. **No built-in connection pooling.** The ODBC Driver Manager *may* implement pooling (`SQLSetEnvAttr(SQL_ATTR_CONNECTION_POOLING)`), but it's not the robust, well-tested pool you get in ADO.NET. Many applications roll their own.

4. **Parameter markers are `?`, not `@name`.** They're positional. If you have `INSERT INTO t (a, b) VALUES (?, ?)`, the first `SQLBindParameter(..., 1, ...)` binds to the first `?`. No names. Reordering markers means rebinding.

5. **Check return codes on every call.** There are no exceptions. If you skip checking, you'll silently proceed with failed operations. Develop a macro:

    ```c
    #define CHECK_ODBC(fn, handleType, handle) do { \
        SQLRETURN _r = (fn);                        \
        if (_r == SQL_ERROR) {                      \
            print_odbc_error(handleType, handle);   \
            goto cleanup;                           \
        }                                           \
    } while(0)
    ```

6. **W-functions vs A-functions.** `SQLExecDirect` takes `SQLCHAR*` (ANSI). `SQLExecDirectW` takes `SQLWCHAR*` (UTF-16). On Windows, the Driver Manager can convert between them. On Linux with unixODBC, the DM also does conversion. Know which encoding your driver expects. For SQL Server, the wire protocol is UTF-16 (UCS-2 technically), so using W-functions avoids a conversion hop.

7. **`SQL_NTS` means null-terminated string.** Whenever you pass a string length, you can pass `SQL_NTS` instead of the actual byte count, and ODBC will `strlen` it for you. Convenient, but means your strings must actually be null-terminated.

8. **No `NextResult`.** Well, there is — it's `SQLMoreResults`. If your query returns multiple result sets, call `SQLMoreResults(hstmt)` to advance. Returns `SQL_SUCCESS` if there's another result set, `SQL_NO_DATA` if done.

9. **Statement handle reuse.** Unlike ADO.NET where you typically create a new `SqlCommand` per query, in ODBC it's common to reuse statement handles. After fetching all results, you can call `SQLFreeStmt(hstmt, SQL_CLOSE)` to close the cursor without freeing the handle, then execute another query on the same handle.

10. **No async/await.** ODBC has `SQL_ASYNC_ENABLE` but it's polling-based, not callback-based. In practice, almost nobody uses it. If you need async, you do it at the application layer (threads, event loops, etc.).
