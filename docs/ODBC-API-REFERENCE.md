# Furball ODBC API Reference

Furball is an ODBC driver for Microsoft SQL Server implemented in Rust. It wraps the [tabby](https://github.com/prisma/tiberius)-compatible async TDS client library behind the synchronous ODBC C ABI, exposing a shared library (`libfurball.so`) loadable by any ODBC driver manager (unixODBC, iODBC).

---

## 1. Architecture Overview

### Handle Hierarchy

```
HENV (Environment)
 └── HDBC (Connection)      ← owns tabby::Client
      └── HSTMT (Statement)  ← owns result set + bound params
```

- **Environment** — holds `odbc_version` and a list of child `Connection` pointers.
- **Connection** — owns a `tabby::Client<Compat<TcpStream>>`, connection metadata (server, database, uid, pwd), autocommit/transaction state, diagnostics, and child `Statement` pointers.
- **Statement** — owns column descriptors, the full result set as `Vec<Vec<Option<String>>>`, a row cursor index, prepared SQL, bound parameters, and diagnostics.

Parent handles track child pointers; `SQLFreeHandle` removes the child from the parent's list and drops it.

### Global Tokio Runtime

A single multi-threaded Tokio runtime (1 worker thread) is lazily initialized via `once_cell::sync::Lazy`:

```rust
pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| { ... });
pub fn block_on<F: Future>(future: F) -> F::Output { RUNTIME.block_on(future) }
```

Every async tabby call is executed via `runtime::block_on()`, bridging ODBC's synchronous C API to tabby's async Rust API. This means all ODBC calls from all threads share a single Tokio runtime and **block the calling thread** until the async operation completes.

### StringRowWriter Pattern

All query results are materialized eagerly into memory as strings. `StringRowWriter` implements tabby's `RowWriter` trait:

- Every value (integers, floats, dates, GUIDs, etc.) is converted to `Option<String>` on arrival.
- `None` represents SQL NULL.
- Binary data is hex-encoded.
- The full result set is stored in `Statement.rows: Vec<Vec<Option<String>>>`.
- `SQLGetData` then parses strings back to the requested C type at retrieval time.

This simplifies implementation at the cost of precision (floating-point round-trips through string representation) and memory (entire result set buffered).

### Sync-over-Async Bridge

```
Application → SQLExecDirect() → exec_direct()
  → runtime::block_on(client.batch_into(sql, &mut writer))
  → tabby async TDS protocol over TCP
  → StringRowWriter collects results
  → Statement populated with columns + rows
```

---

## 2. ODBC Functions by Category

### Handle Management

#### SQLAllocHandle

Allocates Environment, Connection, or Statement handles.

- **SQL_HANDLE_ENV**: Creates `Environment` with `odbc_version = SQL_OV_ODBC3`.
- **SQL_HANDLE_DBC**: Creates `Connection`, links to parent Environment.
- **SQL_HANDLE_STMT**: Creates `Statement`, links to parent Connection.

Returns `Box::into_raw()` pointer as the handle. No tabby API calls.

Also provides ODBC 2.x compat: `SQLAllocEnv`, `SQLAllocConnect`, `SQLAllocStmt`.

#### SQLFreeHandle

Frees a handle via `Box::from_raw()`. Removes the handle from the parent's child list. Also frees child handles transitively (Rust `Drop`).

- **SQL_HANDLE_STMT**: Also available as `SQLFreeStmt(SQL_DROP)`.
- ODBC 2.x compat: `SQLFreeEnv`, `SQLFreeConnect`.

#### SQLFreeStmt

- **SQL_CLOSE**: Clears columns, rows, resets cursor. Same as `SQLCloseCursor`.
- **SQL_UNBIND**: No-op (column binding not implemented).
- **SQL_RESET_PARAMS**: Clears `bound_params`.
- **SQL_DROP**: Delegates to `SQLFreeHandle(SQL_HANDLE_STMT)`.

#### SQLCloseCursor

Delegates to `SQLFreeStmt(SQL_CLOSE)`.

---

### Connection

#### SQLDriverConnect / SQLDriverConnectW

Parses a connection string with keys: `Server` (host,port), `Database`/`Initial Catalog`, `UID`/`User ID`, `PWD`/`Password`, `TrustServerCertificate`.

**Tabby API**: `TcpStream::connect()` → `tabby::Client::connect(config, tcp)` via `runtime::block_on()`.

- Encryption: always `EncryptionLevel::Required`.
- TCP_NODELAY enabled.
- `_driver_completion` parameter is ignored (no UI prompt support).
- W variant converts UTF-16 → UTF-8, delegates to ANSI variant, converts output back.
- Connection string is written back to `conn_str_out` if buffer provided.

**Diagnostics**: SQLSTATE `08001` on connection failure.

#### SQLConnect / SQLConnectW

Takes DSN name, UID, PWD separately. Resolves DSN from `~/.odbc.ini` or `/etc/odbc.ini` by parsing INI sections, builds a connection string, then delegates to `driver_connect()`.

UID/PWD parameters override DSN-defined values.

#### SQLDisconnect

Sets `client = None`, `connected = false`. No explicit TDS logout — connection is dropped.

---

### Execution

#### SQLExecDirect / SQLExecDirectW

Executes SQL immediately.

**Tabby API**: `client.batch_into(sql, &mut StringRowWriter)`.

- If `autocommit = false` and not in a transaction, sends `BEGIN TRANSACTION` first.
- Results fully materialized into `stmt.rows` and `stmt.columns`.
- `row_count`: set to `done_rows` for DML (no columns), `-1` for SELECT.
- W variant converts UTF-16 → UTF-8, delegates to same `exec_direct()`.

**Diagnostics**: SQLSTATE `08003` if not connected, `HY000` for execution errors.

#### SQLPrepare / SQLPrepareW

Stores SQL text in `stmt.prepared_sql`. **No server-side prepare** — the SQL is stored locally and executed via `batch_into()` when `SQLExecute` is called.

#### SQLExecute

Executes `prepared_sql` via `exec_direct()`.

- Substitutes bound parameters by replacing `?` placeholders with literal values (client-side parameter substitution).
- String values are SQL-escaped with `N'...'` quoting.
- Numeric types sent as bare literals.
- Clears `bound_params` after execution.

**Diagnostics**: SQLSTATE `HY010` if no prepared statement.

**Limitation**: No true server-side prepared statements. Parameter substitution is textual, which has SQL injection implications if misused (though parameters come from the application, not user input directly).

---

### Fetch / Data Retrieval

#### SQLFetch

Advances `row_index` by 1. Returns `SQL_NO_DATA` when past the last row, `SQL_SUCCESS` otherwise. Forward-only cursor.

#### SQLGetData / SQLGetDataW

Retrieves a column value from the current row by parsing the stored `Option<String>`.

**Supported C target types** (`eff_type`):

| C Type | Behavior |
|--------|----------|
| `SQL_C_CHAR` | Returns UTF-8 bytes, null-terminated. Truncates with `SQL_SUCCESS_WITH_INFO`. |
| `SQL_C_WCHAR` | Returns UTF-16, null-terminated. Truncates with `SQL_SUCCESS_WITH_INFO`. |
| `SQL_C_LONG` / `SQL_C_SLONG` | Parses as `i32`. |
| `SQL_C_SHORT` | Parses as `i16`. |
| `SQL_C_SBIGINT` | Parses as `i64`. |
| `SQL_C_DOUBLE` | Parses as `f64`. |
| `SQL_C_FLOAT` | Parses as `f32`. |
| `SQL_C_BIT` | Returns 0 or 1 (`u8`). |
| `SQL_C_UTINYINT` / `SQL_C_STINYINT` | Parses as `u8`. |
| `SQL_C_TYPE_TIMESTAMP` | Parses `"YYYY-MM-DD HH:MM:SS.fff"` → `SqlTimestampStruct`. |
| `SQL_C_TYPE_DATE` | Parses date portion → `SqlDateStruct`. |
| `SQL_C_TYPE_TIME` | Parses time portion → `SqlTimeStruct`. |
| `SQL_C_BINARY` | Hex-decodes if all hex chars, otherwise raw bytes. |
| `SQL_C_GUID` | Parses `"XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"` → `SqlGuid`. |
| `SQL_C_DEFAULT` | Maps from column's SQL type to appropriate C type. |

`NULL` values set `*str_len_or_ind = SQL_NULL_DATA`.

**SQLGetDataW**: For character types (`SQL_C_WCHAR`, `SQL_C_CHAR`, `SQL_C_DEFAULT`), returns UTF-16. Non-character types delegate to `get_data()`.

**Limitations**:
- No piecewise retrieval for large data — entire value returned or truncated in one call.
- Parse errors default to 0 (numeric) silently.

#### SQLNumResultCols

Returns `stmt.columns.len()`. No server call.

#### SQLDescribeCol / SQLDescribeColW

Returns column metadata from `stmt.columns[col_number - 1]`: name, SQL type, size, decimal digits, nullable.

W variant returns column name as UTF-16.

#### SQLColAttribute / SQLColAttributeW

Returns column attributes by field identifier:

| Field | Value |
|-------|-------|
| `SQL_DESC_NAME` / `SQL_DESC_LABEL` / `SQL_COLUMN_NAME` | Column name |
| `SQL_DESC_CONCISE_TYPE` / `SQL_DESC_TYPE` | SQL type code |
| `SQL_DESC_LENGTH` / `SQL_COLUMN_LENGTH` | Column size |
| `SQL_DESC_DISPLAY_SIZE` | Type-specific display width (min = name length) |
| `SQL_DESC_PRECISION` | Column size |
| `SQL_DESC_SCALE` | Decimal digits |
| `SQL_DESC_NULLABLE` | Nullable flag |
| `SQL_DESC_TYPE_NAME` | Type name string (e.g., "int", "nvarchar") |
| `SQL_DESC_COUNT` | Total column count (col_number = 0) |
| `SQL_DESC_TABLE_NAME` | Always empty string |
| `SQL_DESC_SEARCHABLE` | Always 3 (SQL_SEARCHABLE) |
| `SQL_DESC_UPDATABLE` | Always 0 (SQL_ATTR_READONLY) |
| `SQL_DESC_AUTO_UNIQUE_VALUE` | Always 0 |
| `SQL_DESC_UNSIGNED` | Always 0 |
| Others | 0 |

W variant returns string attributes as UTF-16.

#### SQLRowCount

Returns `stmt.row_count`. For SELECT: `-1`. For DML: rows affected from TDS Done token.

#### SQLMoreResults

Always returns `SQL_NO_DATA`. **Only the first result set is captured** by StringRowWriter.

---

### Parameters

#### SQLBindParameter

Stores parameter binding metadata in `stmt.bound_params`. Parameters are identified by `param_number` (1-based, corresponding to `?` position in SQL).

Stored fields: `value_type`, `parameter_type`, `column_size`, `decimal_digits`, `value_ptr`, `buffer_length`, `len_ind_ptr`.

**Note**: Pointers are stored as-is. The application must keep parameter buffers valid until `SQLExecute`. Parameters are read at execute time and substituted textually.

#### SQLNumParams

Counts `?` characters in `prepared_sql`. No server-side parameter metadata.

#### SQLFreeStmt(SQL_RESET_PARAMS)

Clears `bound_params`.

---

### Transactions

#### SQLSetConnectAttr / SQLSetConnectAttrW — SQL_ATTR_AUTOCOMMIT

- Turning autocommit OFF: sets flag only. `BEGIN TRANSACTION` is deferred until next `exec_direct()`.
- Turning autocommit ON while in a transaction: sends `COMMIT` via `client.batch_into()`.
- Also accepts `SQL_ATTR_LOGIN_TIMEOUT` and `SQL_ATTR_CONNECTION_TIMEOUT` (ignored).

W variant delegates to same implementation.

#### SQLGetConnectAttr

Returns `autocommit` flag for `SQL_ATTR_AUTOCOMMIT`. Other attributes return `SQL_SUCCESS` with no action.

#### SQLGetConnectAttrW

Always returns `SQL_SUCCESS` (no-op for all attributes). **Bug**: doesn't return autocommit like the ANSI version.

#### SQLEndTran

- **SQL_HANDLE_DBC**: Sends `COMMIT` or `ROLLBACK` via `client.batch_into()`. Sets `in_transaction = false`.
- **SQL_HANDLE_ENV**: Returns `SQL_SUCCESS` without action.

**Diagnostics**: SQLSTATE `HY000` on error.

---

### Environment

#### SQLSetEnvAttr

- **SQL_ATTR_ODBC_VERSION**: Stores the version (accepts `SQL_OV_ODBC2`, `SQL_OV_ODBC3`). Other attributes silently succeed.

---

### Info / Capabilities

#### SQLGetInfo / SQLGetInfoW

Returns driver and server metadata. Key values:

| Info Type | Value |
|-----------|-------|
| `SQL_DRIVER_NAME` | `"libfurball.so"` |
| `SQL_DRIVER_VER` | `"01.00.0000"` |
| `SQL_DBMS_NAME` | `"Microsoft SQL Server"` |
| `SQL_DBMS_VER` | `"16.00.0000"` (hardcoded) |
| `SQL_SERVER_NAME` | Connection's server string |
| `SQL_DATABASE_NAME` | Connection's database |
| `SQL_IDENTIFIER_QUOTE_CHAR` | `"\""` |
| `SQL_CATALOG_NAME_SEPARATOR` | `"."` |
| `SQL_SEARCH_PATTERN_ESCAPE` | `"\\"` |
| `SQL_GETDATA_EXTENSIONS` | `SQL_GD_ANY_COLUMN \| SQL_GD_ANY_ORDER` |
| `SQL_TXN_CAPABLE` | `SQL_TC_ALL` |
| `SQL_DEFAULT_TXN_ISOLATION` | `READ_COMMITTED` (2) |
| `SQL_MAX_IDENTIFIER_LEN` | 128 |

W variant returns string values as UTF-16, numeric values identically.

**Limitation**: `SQL_DBMS_VER` is hardcoded to 16.00, not queried from server.

#### SQLGetFunctions

- For `SQL_API_ODBC3_ALL_FUNCTIONS` (999): fills a 250-word bitmap with bits set for ~30 supported function IDs.
- For individual function queries: always returns `1` (supported).

#### SQLGetDiagRec / SQLGetDiagRecW

Retrieves diagnostic records from Connection or Statement handles. Environment returns `SQL_NO_DATA`.

Each record contains: 5-char SQLSTATE, native error code, message text.

W variant: gets ANSI result then converts to UTF-16 (byte-by-byte widening, ASCII only).

Also provides ODBC 2.x `SQLError` which tries STMT → DBC → ENV in order.

#### SQLGetDiagField / SQLGetDiagFieldW

Always returns `SQL_NO_DATA` (stub).

---

### Catalog Functions

All catalog functions work by generating T-SQL queries against `sys.*` catalog views and executing them via `exec_direct()`. Results come back as regular result sets.

#### SQLTables / SQLTablesW

Queries `sys.objects JOIN sys.schemas`. Supports filtering by table name, schema, and table type (`TABLE`, `VIEW`, `SYSTEM TABLE`). Catalog parameter is ignored (always current database).

Returns: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `TABLE_TYPE`, `REMARKS`.

#### SQLColumns / SQLColumnsW

Queries `sys.all_columns JOIN sys.all_objects JOIN sys.schemas JOIN sys.types`. Supports LIKE filtering on table, schema, column.

Returns: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `TYPE_NAME`, `COLUMN_SIZE`, `BUFFER_LENGTH`, `DECIMAL_DIGITS`, `NUM_PREC_RADIX`, `NULLABLE`, `REMARKS`, `ORDINAL_POSITION`.

**Note**: `DATA_TYPE` returns `system_type_id` (SQL Server internal), not ODBC type codes.

#### SQLPrimaryKeys / SQLPrimaryKeysW

Queries `sys.indexes WHERE is_primary_key = 1` joined with columns.

Returns: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `KEY_SEQ`, `PK_NAME`.

#### SQLStatistics / SQLStatisticsW

Queries `sys.indexes` joined with columns. Filters by unique-only if `unique = 0` (SQL_INDEX_UNIQUE).

Returns: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `NON_UNIQUE`, `INDEX_QUALIFIER`, `INDEX_NAME`, `TYPE`, `ORDINAL_POSITION`, `COLUMN_NAME`, `ASC_OR_DESC`, `CARDINALITY` (NULL), `PAGES` (NULL), `FILTER_CONDITION` (NULL).

#### SQLForeignKeys / SQLForeignKeysW

Queries `sys.foreign_keys` with joins to parent/child tables and columns.

Returns: `PKTABLE_CAT`, `PKTABLE_SCHEM`, `PKTABLE_NAME`, `PKCOLUMN_NAME`, `FKTABLE_CAT`, `FKTABLE_SCHEM`, `FKTABLE_NAME`, `FKCOLUMN_NAME`, `KEY_SEQ`, `UPDATE_RULE`, `DELETE_RULE`, `FK_NAME`, `PK_NAME`, `DEFERRABILITY`.

**Limitation**: `UPDATE_RULE` and `DELETE_RULE` are hardcoded to 1 (SQL_CASCADE), not derived from actual FK definitions.

#### SQLSpecialColumns / SQLSpecialColumnsW

- `id_type = 1` (SQL_BEST_ROWID): returns identity columns.
- `id_type = 2` (SQL_ROWVER): returns timestamp/rowversion columns.

`_scope` and `_nullable` parameters are ignored.

#### SQLGetTypeInfo / SQLGetTypeInfoW

Queries `sys.types` with a CASE expression mapping type names to ODBC type codes. Returns the standard 19-column ODBC type info result set.

W variant delegates to ANSI version.

#### SQLProcedures / SQLProceduresW

Returns empty result set (no columns, no rows). Stub only.

---

### Other Functions

#### SQLBindCol

Always returns `SQL_SUCCESS`. No-op — column binding is not implemented. Applications must use `SQLGetData`.

#### SQLNativeSql

Returns the input SQL unchanged (pass-through).

#### SQLCancel

Returns `SQL_SUCCESS`. No-op — no async cancellation support.

#### SQLFetchScroll

`SQL_FETCH_NEXT` delegates to `SQLFetch`. All other orientations return `SQL_ERROR`.

#### SQLSetStmtAttr / SQLSetStmtAttrW / SQLGetStmtAttr / SQLGetStmtAttrW

All return `SQL_SUCCESS` without action (stubs). Statement attributes like `SQL_ATTR_ROW_ARRAY_SIZE`, `SQL_ATTR_CURSOR_TYPE`, etc. are accepted but ignored.

---

## 3. Type Mapping Table

### tabby ColumnType → ODBC SQL Type

| tabby ColumnType | ODBC SQL Type | SQL Constant |
|------------------|---------------|--------------|
| `Int4` | `SQL_INTEGER` | 4 |
| `Int2` | `SQL_SMALLINT` | 5 |
| `Int1` | `SQL_TINYINT` | -6 |
| `Int8` / `Intn` | `SQL_BIGINT` | -5 |
| `Float8` / `Floatn` | `SQL_DOUBLE` | 8 |
| `Float4` | `SQL_REAL` | 7 |
| `Bit` / `Bitn` | `SQL_BIT` | -7 |
| `BigVarChar` / `NVarchar` | `SQL_WVARCHAR` | -9 |
| `BigChar` / `NChar` | `SQL_WCHAR` | -8 |
| `Text` | `SQL_LONGVARCHAR` | -1 |
| `NText` | `SQL_WLONGVARCHAR` | -10 |
| `BigBinary` | `SQL_BINARY` | -2 |
| `BigVarBin` | `SQL_VARBINARY` | -3 |
| `Image` | `SQL_LONGVARBINARY` | -4 |
| `Decimaln` / `Numericn` / `Money` / `Money4` | `SQL_DECIMAL` | 3 |
| `Datetime` / `Datetimen` / `Datetime4` / `Datetime2` | `SQL_TYPE_TIMESTAMP` | 93 |
| `Daten` | `SQL_TYPE_DATE` | 91 |
| `Timen` | `SQL_TYPE_TIME` | 92 |
| `Guid` | `SQL_GUID` | -11 |
| *(all others)* | `SQL_VARCHAR` | 12 |

### ODBC SQL Type → Default C Type (SQL_C_DEFAULT mapping)

| SQL Type | Default C Type |
|----------|---------------|
| `SQL_INTEGER` | `SQL_C_LONG` |
| `SQL_SMALLINT` | `SQL_C_SHORT` |
| `SQL_BIGINT` | `SQL_C_SBIGINT` |
| `SQL_DOUBLE` / `SQL_FLOAT` | `SQL_C_DOUBLE` |
| `SQL_REAL` | `SQL_C_FLOAT` |
| `SQL_BIT` | `SQL_C_BIT` |
| `SQL_TINYINT` | `SQL_C_UTINYINT` |
| `SQL_TYPE_TIMESTAMP` | `SQL_C_TYPE_TIMESTAMP` |
| `SQL_TYPE_DATE` | `SQL_C_TYPE_DATE` |
| `SQL_TYPE_TIME` | `SQL_C_TYPE_TIME` |
| `SQL_BINARY` / `SQL_VARBINARY` / `SQL_LONGVARBINARY` | `SQL_C_BINARY` |
| `SQL_GUID` | `SQL_C_GUID` |
| *(all others)* | `SQL_C_CHAR` |

### Column Size Defaults

| SQL Type | Size |
|----------|------|
| `SQL_INTEGER` | 10 |
| `SQL_SMALLINT` | 5 |
| `SQL_TINYINT` | 3 |
| `SQL_BIGINT` | 19 |
| `SQL_DOUBLE` | 53 |
| `SQL_REAL` | 24 |
| `SQL_BIT` | 1 |
| `SQL_TYPE_TIMESTAMP` | 23 |
| `SQL_TYPE_DATE` | 10 |
| `SQL_TYPE_TIME` | 16 |
| `SQL_GUID` | 36 |
| `SQL_DECIMAL` | 38 |
| *(all others)* | 256 |

---

## 4. StringRowWriter

`StringRowWriter` implements tabby's `RowWriter` trait. It is the bridge between tabby's streaming row protocol and furball's in-memory result set.

### Trait Callbacks

| Callback | Behavior |
|----------|----------|
| `on_metadata(columns)` | Converts `tabby::Column` → `ColumnDesc` (name, sql_type, size, nullable). Only processes the **first** result set (subsequent metadata is ignored). |
| `on_row_done()` | Moves `current_row` into `rows` vec, starts new row. |
| `on_done(rows)` | Accumulates `done_rows` count (used for DML row counts). |
| `write_null(col)` | Pushes `None`. |
| `write_bool(col, val)` | Pushes `"1"` or `"0"`. |
| `write_u8(col, val)` | Pushes `val.to_string()`. |
| `write_i16(col, val)` | Pushes `val.to_string()`. |
| `write_i32(col, val)` | Pushes `val.to_string()`. |
| `write_i64(col, val)` | Pushes `val.to_string()`. |
| `write_f32(col, val)` | Pushes `val.to_string()`. |
| `write_f64(col, val)` | Pushes `val.to_string()`. |
| `write_str(col, val)` | Pushes `val.to_string()`. |
| `write_bytes(col, val)` | Pushes hex-encoded string. |
| `write_date(col, days)` | Converts days-since-epoch → `"YYYY-MM-DD"` via civil date algorithm. |
| `write_time(col, nanos)` | Converts nanoseconds → `"HH:MM:SS.mmm"`. |
| `write_datetime(col, micros)` | Converts microseconds-since-epoch → `"YYYY-MM-DD HH:MM:SS.mmm"`. |
| `write_datetimeoffset(col, micros, offset)` | Datetime + `" ±HH:MM"` offset suffix. |
| `write_decimal(col, value, precision, scale)` | Formats `i128` with decimal point at `scale` position. |
| `write_guid(col, bytes)` | Formats as `"XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"` (LE byte order for first 3 groups). |

### Data Flow

```
tabby TDS stream
  → on_metadata() → populates columns
  → write_*() calls per cell → pushes to current_row
  → on_row_done() → moves current_row to rows
  → on_done() → records affected row count
```

---

## 5. Known Limitations

1. **Entire result set buffered in memory** — No streaming/cursor-based fetch. Large result sets consume proportional memory.

2. **All values stored as strings** — Floating-point values lose precision through `f64 → String → f64` round-trips. Decimal values may similarly lose precision depending on string formatting.

3. **No server-side prepared statements** — `SQLPrepare`/`SQLExecute` does client-side textual parameter substitution. No parameterized query protocol.

4. **Single result set only** — `SQLMoreResults` always returns `SQL_NO_DATA`. Only the first result set from a batch is captured.

5. **Forward-only cursor** — `SQLFetchScroll` only supports `SQL_FETCH_NEXT`. No scrollable cursors.

6. **No SQLBindCol support** — `SQLBindCol` is a no-op. Applications must use `SQLGetData` for all data retrieval.

7. **No piecewise data retrieval** — `SQLGetData` returns the entire value or truncates. No continuation calls for large values.

8. **Hardcoded DBMS version** — `SQL_DBMS_VER` returns `"16.00.0000"` regardless of actual server version.

9. **No async/notification support** — `SQLCancel` is a no-op. No asynchronous execution mode.

10. **Catalog DATA_TYPE values** — `SQLColumns` returns SQL Server's `system_type_id` rather than ODBC standard type codes.

11. **Foreign key rules hardcoded** — `SQLForeignKeys` returns `UPDATE_RULE = 1` and `DELETE_RULE = 1` regardless of actual constraint definitions.

12. **SQLGetConnectAttrW bug** — Does not return autocommit state (always returns `SQL_SUCCESS` with no value written), unlike the ANSI variant.

13. **SQLGetDiagField stub** — Always returns `SQL_NO_DATA`.

14. **Statement attributes ignored** — `SQLSetStmtAttr` accepts all attributes but stores nothing. Cursor type, concurrency, row array size, etc. have no effect.

15. **No encryption negotiation** — Always uses `EncryptionLevel::Required`. Cannot connect to servers that don't support encryption.

16. **Single Tokio worker thread** — All connections share one runtime worker thread, which may become a bottleneck under concurrent use.

17. **Parameter substitution is textual** — Bound parameters are string-interpolated into SQL. While values are SQL-escaped, this is fundamentally different from true parameterized queries.

18. **No SQLProcedures implementation** — Returns empty result set.

19. **Date/time parsing fragile** — Timestamp parsing uses simple string splitting; unusual formats may produce incorrect results.

20. **DSN resolution reads INI files directly** — Does not use the ODBC driver manager's DSN resolution; reads `~/.odbc.ini` and `/etc/odbc.ini` manually.
