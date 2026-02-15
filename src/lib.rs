#![allow(non_snake_case)]

mod attr;
mod connect;
mod diagnostics;
mod execute;
mod fetch;
mod handle;
mod runtime;
mod types;

use handle::*;
use std::ffi::CStr;
use std::ptr;
use types::*;

// ── Helper: extract string from SQLCHAR* + length ───────────────────

fn wchar_to_string(ptr: *const SQLWCHAR, len: SQLSMALLINT) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let count = if len < 0 {
        let mut n = 0;
        unsafe {
            while *ptr.add(n) != 0 {
                n += 1;
            }
        }
        n
    } else {
        len as usize
    };
    let slice = unsafe { std::slice::from_raw_parts(ptr, count) };
    String::from_utf16_lossy(slice)
}

unsafe fn sql_str(ptr: *const SQLCHAR, len: SQLSMALLINT) -> String {
    if ptr.is_null() {
        return String::new();
    }
    if len == SQL_NTS as SQLSMALLINT {
        CStr::from_ptr(ptr as *const i8)
            .to_string_lossy()
            .into_owned()
    } else if len > 0 {
        let slice = std::slice::from_raw_parts(ptr, len as usize);
        String::from_utf8_lossy(slice).into_owned()
    } else {
        String::new()
    }
}

unsafe fn sql_str_isize(ptr: *const SQLCHAR, len: SQLLEN) -> String {
    if ptr.is_null() {
        return String::new();
    }
    if len == SQL_NTS {
        CStr::from_ptr(ptr as *const i8)
            .to_string_lossy()
            .into_owned()
    } else if len > 0 {
        let slice = std::slice::from_raw_parts(ptr, len as usize);
        String::from_utf8_lossy(slice).into_owned()
    } else {
        String::new()
    }
}

// ── Handle Management ───────────────────────────────────────────────

fn alloc_handle_impl(
    handle_type: SQLSMALLINT,
    input_handle: SQLHANDLE,
    output_handle: *mut SQLHANDLE,
) -> SQLRETURN {
    if output_handle.is_null() {
        return SQL_ERROR;
    }

    match handle_type {
        SQL_HANDLE_ENV => {
            let env = Box::new(Environment {
                odbc_version: SQL_OV_ODBC3,
                connections: Vec::new(),
            });
            unsafe {
                *output_handle = Box::into_raw(env) as SQLHANDLE;
            }
            SQL_SUCCESS
        }
        SQL_HANDLE_DBC => {
            let conn = Box::new(Connection {
                env: if input_handle.is_null() {
                    std::ptr::null_mut()
                } else {
                    input_handle as *mut Environment
                },
                client: None,
                server: String::new(),
                database: String::new(),
                uid: String::new(),
                pwd: String::new(),
                diagnostics: Vec::new(),
                statements: Vec::new(),
                connected: false,
                autocommit: true,
                in_transaction: false,
            });
            let conn_ptr = Box::into_raw(conn);
            if !input_handle.is_null() {
                let env = unsafe { &mut *(input_handle as *mut Environment) };
                env.connections.push(conn_ptr);
            }
            unsafe {
                *output_handle = conn_ptr as SQLHANDLE;
            }
            SQL_SUCCESS
        }
        SQL_HANDLE_STMT => {
            let stmt = Box::new(Statement {
                conn: if input_handle.is_null() {
                    std::ptr::null_mut()
                } else {
                    input_handle as *mut Connection
                },
                columns: Vec::new(),
                rows: Vec::new(),
                row_index: -1,
                diagnostics: Vec::new(),
                executed: false,
                prepared_sql: None,
                row_count: -1,
                bound_params: Vec::new(),
            });
            let stmt_ptr = Box::into_raw(stmt);
            if !input_handle.is_null() {
                let conn = unsafe { &mut *(input_handle as *mut Connection) };
                conn.statements.push(stmt_ptr);
            }
            unsafe {
                *output_handle = stmt_ptr as SQLHANDLE;
            }
            SQL_SUCCESS
        }
        _ => SQL_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLAllocHandle(
    handle_type: SQLSMALLINT,
    input_handle: SQLHANDLE,
    output_handle: *mut SQLHANDLE,
) -> SQLRETURN {
    alloc_handle_impl(handle_type, input_handle, output_handle)
}

fn free_handle_impl(handle_type: SQLSMALLINT, handle: SQLHANDLE) -> SQLRETURN {
    if handle.is_null() {
        return SQL_INVALID_HANDLE;
    }

    match handle_type {
        SQL_HANDLE_ENV => {
            let _ = unsafe { Box::from_raw(handle as *mut Environment) };
            SQL_SUCCESS
        }
        SQL_HANDLE_DBC => {
            let conn = unsafe { Box::from_raw(handle as *mut Connection) };
            // Remove from env's connection list
            if !conn.env.is_null() {
                let env = unsafe { &mut *conn.env };
                env.connections.retain(|&p| p != handle as *mut Connection);
            }
            drop(conn);
            SQL_SUCCESS
        }
        SQL_HANDLE_STMT => {
            let stmt = unsafe { Box::from_raw(handle as *mut Statement) };
            // Remove from connection's statement list
            if !stmt.conn.is_null() {
                let conn = unsafe { &mut *stmt.conn };
                conn.statements.retain(|&p| p != handle as *mut Statement);
            }
            drop(stmt);
            SQL_SUCCESS
        }
        _ => SQL_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLFreeHandle(handle_type: SQLSMALLINT, handle: SQLHANDLE) -> SQLRETURN {
    free_handle_impl(handle_type, handle)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLFreeStmt(hstmt: SQLHSTMT, option: SQLUSMALLINT) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    match option {
        SQL_CLOSE => {
            stmt.columns.clear();
            stmt.rows.clear();
            stmt.row_index = -1;
            stmt.executed = false;
            stmt.row_count = -1;
            SQL_SUCCESS
        }
        SQL_UNBIND | SQL_RESET_PARAMS => {
            if option == SQL_RESET_PARAMS {
                stmt.bound_params.clear();
            }
            SQL_SUCCESS
        }
        SQL_DROP => free_handle_impl(SQL_HANDLE_STMT, hstmt),
        _ => SQL_ERROR,
    }
}

// ── Connection ──────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLDriverConnect(
    hdbc: SQLHDBC,
    _hwnd: SQLHWND,
    conn_str_in: *const SQLCHAR,
    conn_str_in_len: SQLSMALLINT,
    conn_str_out: *mut SQLCHAR,
    conn_str_out_max: SQLSMALLINT,
    conn_str_out_len: *mut SQLSMALLINT,
    _driver_completion: SQLUSMALLINT,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let conn = unsafe { &mut *(hdbc as *mut Connection) };
    conn.diagnostics.clear();

    let conn_str = unsafe { sql_str(conn_str_in, conn_str_in_len) };

    // Write back the connection string
    if !conn_str_out.is_null() && conn_str_out_max > 0 {
        let bytes = conn_str.as_bytes();
        let copy_len = std::cmp::min(bytes.len(), (conn_str_out_max as usize).saturating_sub(1));
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), conn_str_out, copy_len);
            *conn_str_out.add(copy_len) = 0;
        }
        if !conn_str_out_len.is_null() {
            unsafe {
                *conn_str_out_len = bytes.len() as SQLSMALLINT;
            }
        }
    }

    connect::driver_connect(conn, &conn_str)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLDisconnect(hdbc: SQLHDBC) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let conn = unsafe { &mut *(hdbc as *mut Connection) };
    connect::disconnect(conn)
}

// ── Execution ───────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLExecDirect(
    hstmt: SQLHSTMT,
    statement_text: *const SQLCHAR,
    text_length: SQLINTEGER,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.diagnostics.clear();

    let sql = unsafe { sql_str(statement_text, text_length as SQLSMALLINT) };
    execute::exec_direct(stmt, &sql)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLExecDirectW(
    hstmt: SQLHSTMT,
    statement_text: *const SQLWCHAR,
    text_length: SQLINTEGER,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.diagnostics.clear();

    // Convert UTF-16 to UTF-8
    let sql = if statement_text.is_null() {
        String::new()
    } else {
        let len = if text_length == SQL_NTS as SQLINTEGER {
            let mut l = 0usize;
            unsafe {
                while *statement_text.add(l) != 0 {
                    l += 1;
                }
            }
            l
        } else {
            text_length as usize
        };
        let slice = unsafe { std::slice::from_raw_parts(statement_text, len) };
        String::from_utf16_lossy(slice)
    };

    execute::exec_direct(stmt, &sql)
}

// ── Results ─────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLNumResultCols(hstmt: SQLHSTMT, column_count: *mut SQLSMALLINT) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    if !column_count.is_null() {
        unsafe {
            *column_count = fetch::num_result_cols(stmt);
        }
    }
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLDescribeCol(
    hstmt: SQLHSTMT,
    col_number: SQLUSMALLINT,
    col_name: *mut SQLCHAR,
    buffer_length: SQLSMALLINT,
    name_length: *mut SQLSMALLINT,
    data_type: *mut SQLSMALLINT,
    column_size: *mut SQLULEN,
    decimal_digits: *mut SQLSMALLINT,
    nullable: *mut SQLSMALLINT,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    fetch::describe_col(
        stmt,
        col_number,
        col_name,
        buffer_length,
        name_length,
        data_type,
        column_size,
        decimal_digits,
        nullable,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLFetch(hstmt: SQLHSTMT) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    fetch::fetch(stmt)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetData(
    hstmt: SQLHSTMT,
    col: SQLUSMALLINT,
    target_type: SQLSMALLINT,
    target_value: SQLPOINTER,
    buffer_length: SQLLEN,
    str_len_or_ind: *mut SQLLEN,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    fetch::get_data(
        stmt,
        col,
        target_type,
        target_value,
        buffer_length,
        str_len_or_ind,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetDataW(
    hstmt: SQLHSTMT,
    col: SQLUSMALLINT,
    target_type: SQLSMALLINT,
    target_value: SQLPOINTER,
    buffer_length: SQLLEN,
    str_len_or_ind: *mut SQLLEN,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };

    // For non-character target types, delegate to ANSI version
    if target_type != SQL_C_WCHAR && target_type != SQL_C_DEFAULT && target_type != SQL_C_CHAR {
        return fetch::get_data(
            stmt,
            col,
            target_type,
            target_value,
            buffer_length,
            str_len_or_ind,
        );
    }

    // Character data: return UTF-16
    if stmt.row_index < 0 || stmt.row_index as usize >= stmt.rows.len() {
        return SQL_ERROR;
    }
    let row = &stmt.rows[stmt.row_index as usize];
    let col_idx = (col as usize).wrapping_sub(1);
    if col_idx >= row.len() {
        return SQL_ERROR;
    }

    match &row[col_idx] {
        None => {
            if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = SQL_NULL_DATA;
                }
            }
            SQL_SUCCESS
        }
        Some(val) => {
            let utf16: Vec<u16> = val.encode_utf16().collect();
            let data_len_bytes = (utf16.len() * 2) as SQLLEN;

            if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = data_len_bytes;
                }
            }

            if !target_value.is_null() && buffer_length > 0 {
                let buf_u16_cap = (buffer_length as usize) / 2;
                let copy_count = std::cmp::min(utf16.len(), buf_u16_cap.saturating_sub(1));
                let dest = target_value as *mut u16;
                unsafe {
                    ptr::copy_nonoverlapping(utf16.as_ptr(), dest, copy_count);
                    *dest.add(copy_count) = 0; // null terminate
                }
                if utf16.len() >= buf_u16_cap {
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
            SQL_SUCCESS
        }
    }
}

// ── Diagnostics ─────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetDiagRec(
    handle_type: SQLSMALLINT,
    handle: SQLHANDLE,
    rec_number: SQLSMALLINT,
    sql_state: *mut SQLCHAR,
    native_error: *mut SQLINTEGER,
    message_text: *mut SQLCHAR,
    buffer_length: SQLSMALLINT,
    text_length: *mut SQLSMALLINT,
) -> SQLRETURN {
    diagnostics::get_diag_rec(
        handle_type,
        handle,
        rec_number,
        sql_state,
        native_error,
        message_text,
        buffer_length,
        text_length,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetDiagRecW(
    handle_type: SQLSMALLINT,
    handle: SQLHANDLE,
    rec_number: SQLSMALLINT,
    sql_state: *mut SQLWCHAR,
    native_error: *mut SQLINTEGER,
    message_text: *mut SQLWCHAR,
    buffer_length: SQLSMALLINT,
    text_length: *mut SQLSMALLINT,
) -> SQLRETURN {
    // Get the ANSI version first, then convert
    let mut state_buf = [0u8; 6];
    let mut native = 0i32;
    let mut msg_buf = [0u8; 4096];
    let mut msg_len: SQLSMALLINT = 0;

    let ret = diagnostics::get_diag_rec(
        handle_type,
        handle,
        rec_number,
        state_buf.as_mut_ptr(),
        &mut native,
        msg_buf.as_mut_ptr(),
        4096,
        &mut msg_len,
    );

    if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
        return ret;
    }

    // Copy SQLSTATE as wide chars
    if !sql_state.is_null() {
        for i in 0..6 {
            unsafe {
                *sql_state.add(i) = state_buf[i] as u16;
            }
        }
    }
    if !native_error.is_null() {
        unsafe {
            *native_error = native;
        }
    }

    // Copy message as wide chars
    let msg_len_usize = msg_len as usize;
    if !message_text.is_null() && buffer_length > 0 {
        let copy_len = std::cmp::min(msg_len_usize, (buffer_length as usize).saturating_sub(1));
        for i in 0..copy_len {
            unsafe {
                *message_text.add(i) = msg_buf[i] as u16;
            }
        }
        unsafe {
            *message_text.add(copy_len) = 0;
        }
    }
    if !text_length.is_null() {
        unsafe {
            *text_length = msg_len;
        }
    }

    ret
}

// ── Attributes ──────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLSetEnvAttr(
    henv: SQLHENV,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    string_length: SQLINTEGER,
) -> SQLRETURN {
    if henv.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let env = unsafe { &mut *(henv as *mut Environment) };
    attr::set_env_attr(env, attribute, value, string_length)
}

#[unsafe(no_mangle)]
#[unsafe(no_mangle)]
pub extern "C" fn SQLSetConnectAttr(
    hdbc: SQLHDBC,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    string_length: SQLINTEGER,
) -> SQLRETURN {
    set_connect_attr_impl(hdbc, attribute, value, string_length)
}

fn set_connect_attr_impl(
    hdbc: SQLHDBC,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    string_length: SQLINTEGER,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let conn = unsafe { &mut *(hdbc as *mut Connection) };
    attr::set_connect_attr(conn, attribute, value, string_length)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLSetConnectAttrW(
    hdbc: SQLHDBC,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    string_length: SQLINTEGER,
) -> SQLRETURN {
    set_connect_attr_impl(hdbc, attribute, value, string_length)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetConnectAttr(
    hdbc: SQLHDBC,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    buffer_length: SQLINTEGER,
    string_length: *mut SQLINTEGER,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let conn = unsafe { &*(hdbc as *mut Connection) };
    let _ = (buffer_length, string_length);
    match attribute {
        SQL_ATTR_AUTOCOMMIT => {
            if !value.is_null() {
                unsafe {
                    *(value as *mut SQLULEN) = if conn.autocommit { 1 } else { 0 };
                }
            }
            SQL_SUCCESS
        }
        _ => SQL_SUCCESS,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetConnectAttrW(
    hdbc: SQLHDBC,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    buffer_length: SQLINTEGER,
    string_length: *mut SQLINTEGER,
) -> SQLRETURN {
    let _ = (attribute, value, buffer_length, string_length);
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetInfo(
    hdbc: SQLHDBC,
    info_type: SQLUSMALLINT,
    info_value: SQLPOINTER,
    buffer_length: SQLSMALLINT,
    string_length: *mut SQLSMALLINT,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let conn = unsafe { &*(hdbc as *const Connection) };
    attr::get_info(conn, info_type, info_value, buffer_length, string_length)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetInfoW(
    hdbc: SQLHDBC,
    info_type: SQLUSMALLINT,
    info_value: SQLPOINTER,
    buffer_length: SQLSMALLINT,
    string_length: *mut SQLSMALLINT,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let conn = unsafe { &*(hdbc as *const Connection) };
    attr::get_info_w(conn, info_type, info_value, buffer_length, string_length)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLSetStmtAttr(
    hstmt: SQLHSTMT,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    string_length: SQLINTEGER,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    attr::set_stmt_attr(stmt, attribute, value, string_length)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLSetStmtAttrW(
    hstmt: SQLHSTMT,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    string_length: SQLINTEGER,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    attr::set_stmt_attr(stmt, attribute, value, string_length)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetStmtAttr(
    hstmt: SQLHSTMT,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    buffer_length: SQLINTEGER,
    string_length: *mut SQLINTEGER,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    attr::get_stmt_attr(stmt, attribute, value, buffer_length, string_length)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetStmtAttrW(
    hstmt: SQLHSTMT,
    attribute: SQLINTEGER,
    value: SQLPOINTER,
    buffer_length: SQLINTEGER,
    string_length: *mut SQLINTEGER,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    attr::get_stmt_attr(stmt, attribute, value, buffer_length, string_length)
}

// ── Column Attributes (needed by isql) ──────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLColAttribute(
    hstmt: SQLHSTMT,
    col_number: SQLUSMALLINT,
    field_identifier: SQLUSMALLINT,
    char_attr: SQLPOINTER,
    buffer_length: SQLSMALLINT,
    string_length: *mut SQLSMALLINT,
    numeric_attr: *mut SQLLEN,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    let idx = (col_number as usize).wrapping_sub(1);

    // For SQL_DESC_COUNT, col_number is 0
    if field_identifier == SQL_DESC_COUNT {
        if !numeric_attr.is_null() {
            unsafe {
                *numeric_attr = stmt.columns.len() as SQLLEN;
            }
        }
        return SQL_SUCCESS;
    }

    if idx >= stmt.columns.len() {
        return SQL_ERROR;
    }
    let col = &stmt.columns[idx];

    let write_str_attr = |s: &str| -> SQLRETURN {
        if !string_length.is_null() {
            unsafe {
                *string_length = s.len() as SQLSMALLINT;
            }
        }
        if !char_attr.is_null() && buffer_length > 0 {
            let bytes = s.as_bytes();
            let copy_len = std::cmp::min(bytes.len(), (buffer_length as usize).saturating_sub(1));
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), char_attr as *mut u8, copy_len);
                *((char_attr as *mut u8).add(copy_len)) = 0;
            }
        }
        SQL_SUCCESS
    };

    let write_num = |v: SQLLEN| -> SQLRETURN {
        if !numeric_attr.is_null() {
            unsafe {
                *numeric_attr = v;
            }
        }
        SQL_SUCCESS
    };

    match field_identifier {
        SQL_DESC_NAME | SQL_COLUMN_NAME | SQL_DESC_LABEL | SQL_COLUMN_LABEL => {
            write_str_attr(&col.name)
        }
        SQL_DESC_CONCISE_TYPE | SQL_DESC_TYPE | SQL_COLUMN_TYPE => {
            write_num(col.sql_type as SQLLEN)
        }
        SQL_DESC_LENGTH | SQL_COLUMN_LENGTH => write_num(col.size as SQLLEN),
        SQL_DESC_DISPLAY_SIZE | SQL_COLUMN_DISPLAY_SIZE => {
            let display_size = match col.sql_type {
                SQL_INTEGER => 11,
                SQL_SMALLINT => 6,
                SQL_TINYINT => 4,
                SQL_BIGINT => 20,
                SQL_BIT => 1,
                SQL_DOUBLE | SQL_FLOAT => 24,
                SQL_REAL => 14,
                SQL_TYPE_TIMESTAMP => 23,
                SQL_TYPE_DATE => 10,
                SQL_TYPE_TIME => 16,
                SQL_GUID => 36,
                SQL_DECIMAL | SQL_NUMERIC => 40,
                _ => col.size as SQLLEN,
            };
            // Ensure at least the column name length
            let min_size = col.name.len() as SQLLEN;
            write_num(std::cmp::max(display_size, min_size))
        }
        SQL_DESC_OCTET_LENGTH => write_num(col.size as SQLLEN),
        SQL_DESC_PRECISION => write_num(col.size as SQLLEN),
        SQL_DESC_SCALE => write_num(col.decimal_digits as SQLLEN),
        SQL_DESC_NULLABLE | SQL_COLUMN_NULLABLE => write_num(col.nullable as SQLLEN),
        SQL_DESC_UNNAMED => write_num(0), // SQL_NAMED
        SQL_DESC_AUTO_UNIQUE_VALUE => write_num(0),
        SQL_DESC_CASE_SENSITIVE => write_num(0),
        SQL_DESC_FIXED_PREC_SCALE => write_num(0),
        SQL_DESC_SEARCHABLE => write_num(3), // SQL_SEARCHABLE
        SQL_DESC_UNSIGNED => write_num(0),
        SQL_DESC_UPDATABLE => write_num(0), // SQL_ATTR_READONLY
        SQL_DESC_TABLE_NAME => write_str_attr(""),
        SQL_DESC_TYPE_NAME => {
            let type_name = match col.sql_type {
                SQL_INTEGER => "int",
                SQL_SMALLINT => "smallint",
                SQL_TINYINT => "tinyint",
                SQL_BIGINT => "bigint",
                SQL_BIT => "bit",
                SQL_DOUBLE | SQL_FLOAT => "float",
                SQL_REAL => "real",
                SQL_VARCHAR => "varchar",
                SQL_CHAR => "char",
                SQL_WVARCHAR => "nvarchar",
                SQL_WCHAR => "nchar",
                SQL_TYPE_TIMESTAMP => "datetime",
                SQL_TYPE_DATE => "date",
                SQL_TYPE_TIME => "time",
                SQL_DECIMAL | SQL_NUMERIC => "decimal",
                SQL_BINARY => "binary",
                SQL_VARBINARY => "varbinary",
                SQL_GUID => "uniqueidentifier",
                _ => "varchar",
            };
            write_str_attr(type_name)
        }
        _ => write_num(0),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLColAttributeW(
    hstmt: SQLHSTMT,
    col_number: SQLUSMALLINT,
    field_identifier: SQLUSMALLINT,
    char_attr: SQLPOINTER,
    buffer_length: SQLSMALLINT,
    string_length: *mut SQLSMALLINT,
    numeric_attr: *mut SQLLEN,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    let idx = (col_number as usize).wrapping_sub(1);

    if field_identifier == SQL_DESC_COUNT {
        if !numeric_attr.is_null() {
            unsafe {
                *numeric_attr = stmt.columns.len() as SQLLEN;
            }
        }
        return SQL_SUCCESS;
    }

    if idx >= stmt.columns.len() {
        return SQL_ERROR;
    }
    let col = &stmt.columns[idx];

    let write_str_w = |s: &str| -> SQLRETURN {
        let utf16: Vec<u16> = s.encode_utf16().collect();
        if !string_length.is_null() {
            unsafe {
                *string_length = (utf16.len() * 2) as SQLSMALLINT;
            }
        }
        if !char_attr.is_null() && buffer_length > 0 {
            let buf_cap = (buffer_length as usize) / 2;
            let copy_count = std::cmp::min(utf16.len(), buf_cap.saturating_sub(1));
            let dest = char_attr as *mut u16;
            for i in 0..copy_count {
                unsafe {
                    *dest.add(i) = utf16[i];
                }
            }
            unsafe {
                *dest.add(copy_count) = 0;
            }
        }
        SQL_SUCCESS
    };

    let write_num = |v: SQLLEN| -> SQLRETURN {
        if !numeric_attr.is_null() {
            unsafe {
                *numeric_attr = v;
            }
        }
        SQL_SUCCESS
    };

    match field_identifier {
        SQL_DESC_NAME | SQL_COLUMN_NAME | SQL_DESC_LABEL | SQL_COLUMN_LABEL => {
            write_str_w(&col.name)
        }
        SQL_DESC_CONCISE_TYPE | SQL_DESC_TYPE | SQL_COLUMN_TYPE => {
            write_num(col.sql_type as SQLLEN)
        }
        SQL_DESC_LENGTH | SQL_COLUMN_LENGTH => write_num(col.size as SQLLEN),
        SQL_DESC_DISPLAY_SIZE | SQL_COLUMN_DISPLAY_SIZE => {
            let display_size = match col.sql_type {
                SQL_INTEGER => 11,
                SQL_SMALLINT => 6,
                SQL_TINYINT => 4,
                SQL_BIGINT => 20,
                SQL_BIT => 1,
                SQL_DOUBLE | SQL_FLOAT => 24,
                SQL_REAL => 14,
                SQL_TYPE_TIMESTAMP => 23,
                SQL_TYPE_DATE => 10,
                SQL_TYPE_TIME => 16,
                SQL_GUID => 36,
                SQL_DECIMAL | SQL_NUMERIC => 40,
                _ => col.size as SQLLEN,
            };
            let min_size = col.name.len() as SQLLEN;
            write_num(std::cmp::max(display_size, min_size))
        }
        SQL_DESC_OCTET_LENGTH => write_num(col.size as SQLLEN),
        SQL_DESC_PRECISION => write_num(col.size as SQLLEN),
        SQL_DESC_SCALE => write_num(col.decimal_digits as SQLLEN),
        SQL_DESC_NULLABLE | SQL_COLUMN_NULLABLE => write_num(col.nullable as SQLLEN),
        SQL_DESC_UNNAMED => write_num(0),
        SQL_DESC_AUTO_UNIQUE_VALUE => write_num(0),
        SQL_DESC_CASE_SENSITIVE => write_num(0),
        SQL_DESC_FIXED_PREC_SCALE => write_num(0),
        SQL_DESC_SEARCHABLE => write_num(3),
        SQL_DESC_UNSIGNED => write_num(0),
        SQL_DESC_UPDATABLE => write_num(0),
        SQL_DESC_TABLE_NAME => write_str_w(""),
        SQL_DESC_TYPE_NAME => {
            let type_name = match col.sql_type {
                SQL_INTEGER => "int",
                SQL_SMALLINT => "smallint",
                SQL_TINYINT => "tinyint",
                SQL_BIGINT => "bigint",
                SQL_BIT => "bit",
                SQL_DOUBLE | SQL_FLOAT => "float",
                SQL_REAL => "real",
                SQL_VARCHAR => "varchar",
                SQL_CHAR => "char",
                SQL_WVARCHAR => "nvarchar",
                SQL_WCHAR => "nchar",
                SQL_TYPE_TIMESTAMP => "datetime",
                SQL_TYPE_DATE => "date",
                SQL_TYPE_TIME => "time",
                SQL_DECIMAL | SQL_NUMERIC => "decimal",
                SQL_BINARY => "binary",
                SQL_VARBINARY => "varbinary",
                SQL_GUID => "uniqueidentifier",
                _ => "varchar",
            };
            write_str_w(type_name)
        }
        _ => write_num(0),
    }
}

// ── Catalog functions (stubs) ───────────────────────────────────────

fn catalog_tables(
    hstmt: SQLHSTMT,
    catalog: &str,
    schema: &str,
    table: &str,
    table_type: &str,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.diagnostics.clear();

    // Build SQL query for sys catalog
    let mut conditions = Vec::new();
    if !table.is_empty() && table != "%" {
        conditions.push(format!("o.name LIKE N'{}'", table.replace('\'', "''")));
    }
    if !schema.is_empty() && schema != "%" {
        conditions.push(format!("s.name LIKE N'{}'", schema.replace('\'', "''")));
    }
    let type_filter = if !table_type.is_empty() && table_type != "%" {
        let types: Vec<&str> = table_type
            .split(',')
            .map(|t| t.trim().trim_matches('\''))
            .collect();
        let mut parts = Vec::new();
        for t in types {
            match t {
                "TABLE" => parts.push("o.type = 'U'"),
                "VIEW" => parts.push("o.type = 'V'"),
                "SYSTEM TABLE" => parts.push("o.type = 'S'"),
                _ => {}
            }
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!("({})", parts.join(" OR "))
        }
    } else {
        String::new()
    };
    if !type_filter.is_empty() {
        conditions.push(type_filter);
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let sql = format!(
        "SELECT DB_NAME() AS TABLE_CAT, s.name AS TABLE_SCHEM, o.name AS TABLE_NAME, \
         CASE o.type WHEN 'U' THEN 'TABLE' WHEN 'V' THEN 'VIEW' WHEN 'S' THEN 'SYSTEM TABLE' ELSE 'TABLE' END AS TABLE_TYPE, \
         CAST(NULL AS NVARCHAR(1)) AS REMARKS \
         FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id \
         {}ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME",
        where_clause
    );
    let _ = (catalog,); // catalog is always current DB for SQL Server
    execute::exec_direct(stmt, &sql)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLTables(
    hstmt: SQLHSTMT,
    catalog: *const SQLCHAR,
    catalog_len: SQLSMALLINT,
    schema: *const SQLCHAR,
    schema_len: SQLSMALLINT,
    table: *const SQLCHAR,
    table_len: SQLSMALLINT,
    table_type: *const SQLCHAR,
    table_type_len: SQLSMALLINT,
) -> SQLRETURN {
    let cat = unsafe { sql_str(catalog, catalog_len) };
    let sch = unsafe { sql_str(schema, schema_len) };
    let tbl = unsafe { sql_str(table, table_len) };
    let tt = unsafe { sql_str(table_type, table_type_len) };
    catalog_tables(hstmt, &cat, &sch, &tbl, &tt)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLTablesW(
    hstmt: SQLHSTMT,
    catalog: *const SQLWCHAR,
    catalog_len: SQLSMALLINT,
    schema: *const SQLWCHAR,
    schema_len: SQLSMALLINT,
    table: *const SQLWCHAR,
    table_len: SQLSMALLINT,
    table_type: *const SQLWCHAR,
    table_type_len: SQLSMALLINT,
) -> SQLRETURN {
    let cat = wchar_to_string(catalog, catalog_len);
    let sch = wchar_to_string(schema, schema_len);
    let tbl = wchar_to_string(table, table_len);
    let tt = wchar_to_string(table_type, table_type_len);
    catalog_tables(hstmt, &cat, &sch, &tbl, &tt)
}

fn catalog_columns(
    hstmt: SQLHSTMT,
    _catalog: &str,
    schema: &str,
    table: &str,
    column: &str,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.diagnostics.clear();

    let mut conditions = vec!["1=1".to_string()];
    if !table.is_empty() && table != "%" {
        conditions.push(format!("o.name LIKE N'{}'", table.replace('\'', "''")));
    }
    if !schema.is_empty() && schema != "%" {
        conditions.push(format!("s.name LIKE N'{}'", schema.replace('\'', "''")));
    }
    if !column.is_empty() && column != "%" {
        conditions.push(format!("c.name LIKE N'{}'", column.replace('\'', "''")));
    }

    let sql = format!(
        "SELECT DB_NAME() AS TABLE_CAT, s.name AS TABLE_SCHEM, o.name AS TABLE_NAME, \
         c.name AS COLUMN_NAME, \
         tp.system_type_id AS DATA_TYPE, \
         tp.name AS TYPE_NAME, \
         COALESCE(c.max_length, 0) AS COLUMN_SIZE, \
         COALESCE(c.max_length, 0) AS BUFFER_LENGTH, \
         c.scale AS DECIMAL_DIGITS, \
         10 AS NUM_PREC_RADIX, \
         CASE c.is_nullable WHEN 1 THEN 1 ELSE 0 END AS NULLABLE, \
         CAST(NULL AS NVARCHAR(1)) AS REMARKS, \
         c.column_id AS ORDINAL_POSITION \
         FROM sys.all_columns c \
         JOIN sys.all_objects o ON c.object_id = o.object_id \
         JOIN sys.schemas s ON o.schema_id = s.schema_id \
         JOIN sys.types tp ON c.system_type_id = tp.system_type_id AND tp.system_type_id = tp.user_type_id \
         WHERE {} ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION",
        conditions.join(" AND ")
    );
    execute::exec_direct(stmt, &sql)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLColumns(
    hstmt: SQLHSTMT,
    catalog: *const SQLCHAR,
    catalog_len: SQLSMALLINT,
    schema: *const SQLCHAR,
    schema_len: SQLSMALLINT,
    table: *const SQLCHAR,
    table_len: SQLSMALLINT,
    column: *const SQLCHAR,
    column_len: SQLSMALLINT,
) -> SQLRETURN {
    let cat = unsafe { sql_str(catalog, catalog_len) };
    let sch = unsafe { sql_str(schema, schema_len) };
    let tbl = unsafe { sql_str(table, table_len) };
    let col = unsafe { sql_str(column, column_len) };
    catalog_columns(hstmt, &cat, &sch, &tbl, &col)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLColumnsW(
    hstmt: SQLHSTMT,
    catalog: *const SQLWCHAR,
    catalog_len: SQLSMALLINT,
    schema: *const SQLWCHAR,
    schema_len: SQLSMALLINT,
    table: *const SQLWCHAR,
    table_len: SQLSMALLINT,
    column: *const SQLWCHAR,
    column_len: SQLSMALLINT,
) -> SQLRETURN {
    let cat = wchar_to_string(catalog, catalog_len);
    let sch = wchar_to_string(schema, schema_len);
    let tbl = wchar_to_string(table, table_len);
    let col = wchar_to_string(column, column_len);
    catalog_columns(hstmt, &cat, &sch, &tbl, &col)
}

// ── Row count ───────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLRowCount(hstmt: SQLHSTMT, row_count: *mut SQLLEN) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    if !row_count.is_null() {
        unsafe {
            *row_count = stmt.row_count;
        }
    }
    SQL_SUCCESS
}

// ── Prepare/Execute stubs ───────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLPrepare(
    hstmt: SQLHSTMT,
    statement_text: *const SQLCHAR,
    text_length: SQLINTEGER,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.diagnostics.clear();
    let sql = read_c_str_i32(statement_text, text_length);
    stmt.prepared_sql = Some(sql);
    SQL_SUCCESS
}

fn read_c_str_i32(ptr: *const u8, len: SQLINTEGER) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let slice = if len == SQL_NTS as SQLINTEGER || len < 0 {
        let mut end = 0;
        unsafe {
            while *ptr.add(end) != 0 {
                end += 1;
            }
        }
        unsafe { std::slice::from_raw_parts(ptr, end) }
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len as usize) }
    };
    String::from_utf8_lossy(slice).to_string()
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLPrepareW(
    hstmt: SQLHSTMT,
    statement_text: *const SQLWCHAR,
    text_length: SQLINTEGER,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.diagnostics.clear();
    let count = if text_length < 0 {
        let mut n = 0;
        unsafe {
            while *statement_text.add(n) != 0 {
                n += 1;
            }
        }
        n
    } else {
        text_length as usize
    };
    let slice = unsafe { std::slice::from_raw_parts(statement_text, count) };
    stmt.prepared_sql = Some(String::from_utf16_lossy(slice));
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLExecute(hstmt: SQLHSTMT) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.diagnostics.clear();
    let sql = match &stmt.prepared_sql {
        Some(s) => s.clone(),
        None => {
            stmt.diagnostics.push(DiagRecord {
                state: "HY010".to_string(),
                native_error: 0,
                message: "No prepared statement".to_string(),
            });
            return SQL_ERROR;
        }
    };

    // Substitute bound parameters
    let final_sql = if stmt.bound_params.is_empty() {
        sql
    } else {
        substitute_params(&sql, &stmt.bound_params)
    };

    let ret = execute::exec_direct(stmt, &final_sql);
    // Reset params after execute
    stmt.bound_params.clear();
    ret
}

fn substitute_params(sql: &str, params: &[BoundParam]) -> String {
    let mut result = String::with_capacity(sql.len() + 64);
    let mut param_idx = 0u16;
    for ch in sql.chars() {
        if ch == '?' {
            param_idx += 1;
            if let Some(param) = params.iter().find(|p| p.param_number == param_idx) {
                let val = read_param_value(param);
                result.push_str(&val);
            } else {
                result.push_str("NULL");
            }
        } else {
            result.push(ch);
        }
    }
    result
}

fn read_param_value(param: &BoundParam) -> String {
    // Check for NULL
    if !param.len_ind_ptr.is_null() {
        let len_ind = unsafe { *param.len_ind_ptr };
        if len_ind == SQL_NULL_DATA {
            return "NULL".to_string();
        }
    }

    if param.value_ptr.is_null() {
        return "NULL".to_string();
    }

    unsafe {
        match param.value_type {
            SQL_C_LONG | SQL_C_SLONG => {
                let v = *(param.value_ptr as *const i32);
                v.to_string()
            }
            SQL_C_SHORT => {
                let v = *(param.value_ptr as *const i16);
                v.to_string()
            }
            SQL_C_SBIGINT => {
                let v = *(param.value_ptr as *const i64);
                v.to_string()
            }
            SQL_C_DOUBLE => {
                let v = *(param.value_ptr as *const f64);
                v.to_string()
            }
            SQL_C_FLOAT => {
                let v = *(param.value_ptr as *const f32);
                v.to_string()
            }
            SQL_C_WCHAR => {
                // UTF-16 string
                let len_ind = if !param.len_ind_ptr.is_null() {
                    *param.len_ind_ptr
                } else {
                    SQL_NTS
                };
                let ptr = param.value_ptr as *const u16;
                let count = if len_ind == SQL_NTS || len_ind < 0 {
                    let mut n = 0;
                    while *ptr.add(n) != 0 {
                        n += 1;
                    }
                    n
                } else {
                    (len_ind as usize) / 2
                };
                let slice = std::slice::from_raw_parts(ptr, count);
                let s = String::from_utf16_lossy(slice);
                // SQL-escape single quotes
                format!("N'{}'", s.replace('\'', "''"))
            }
            SQL_C_CHAR | _ => {
                // ANSI string
                let len_ind = if !param.len_ind_ptr.is_null() {
                    *param.len_ind_ptr
                } else {
                    SQL_NTS
                };
                let ptr = param.value_ptr as *const u8;
                let count = if len_ind == SQL_NTS || len_ind < 0 {
                    let mut n = 0;
                    while *ptr.add(n) != 0 {
                        n += 1;
                    }
                    n
                } else {
                    len_ind as usize
                };
                let slice = std::slice::from_raw_parts(ptr, count);
                let s = String::from_utf8_lossy(slice);
                // Check if this is a numeric type being sent as string
                if matches!(
                    param.parameter_type,
                    SQL_INTEGER
                        | SQL_SMALLINT
                        | SQL_BIGINT
                        | SQL_TINYINT
                        | SQL_DOUBLE
                        | SQL_FLOAT
                        | SQL_REAL
                        | SQL_DECIMAL
                        | SQL_NUMERIC
                        | SQL_BIT
                ) {
                    s.to_string()
                } else {
                    format!("N'{}'", s.replace('\'', "''"))
                }
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLBindCol(
    hstmt: SQLHSTMT,
    _col_number: SQLUSMALLINT,
    _target_type: SQLSMALLINT,
    _target_value: SQLPOINTER,
    _buffer_length: SQLLEN,
    _str_len_or_ind: *mut SQLLEN,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLMoreResults(hstmt: SQLHSTMT) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    SQL_NO_DATA
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetTypeInfo(hstmt: SQLHSTMT, _data_type: SQLSMALLINT) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetTypeInfoW(hstmt: SQLHSTMT, data_type: SQLSMALLINT) -> SQLRETURN {
    SQLGetTypeInfo(hstmt, data_type)
}

// ── Wide-char variants for driver manager ───────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLDriverConnectW(
    hdbc: SQLHDBC,
    hwnd: SQLHWND,
    conn_str_in: *const SQLWCHAR,
    conn_str_in_len: SQLSMALLINT,
    conn_str_out: *mut SQLWCHAR,
    conn_str_out_max: SQLSMALLINT,
    conn_str_out_len: *mut SQLSMALLINT,
    driver_completion: SQLUSMALLINT,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }

    // Convert UTF-16 input to UTF-8
    let utf8_str = if conn_str_in.is_null() {
        String::new()
    } else {
        let len = if conn_str_in_len == SQL_NTS as SQLSMALLINT {
            let mut l = 0usize;
            unsafe {
                while *conn_str_in.add(l) != 0 {
                    l += 1;
                }
            }
            l
        } else {
            conn_str_in_len as usize
        };
        let slice = unsafe { std::slice::from_raw_parts(conn_str_in, len) };
        String::from_utf16_lossy(slice)
    };

    // Use the ANSI version with a temp buffer
    let utf8_bytes = utf8_str.as_bytes();
    let mut out_buf = vec![0u8; 4096];
    let mut out_len: SQLSMALLINT = 0;

    let ret = SQLDriverConnect(
        hdbc,
        hwnd,
        utf8_bytes.as_ptr(),
        utf8_bytes.len() as SQLSMALLINT,
        out_buf.as_mut_ptr(),
        out_buf.len() as SQLSMALLINT,
        &mut out_len,
        driver_completion,
    );

    // Convert output to UTF-16
    if !conn_str_out.is_null() && conn_str_out_max > 0 {
        let out_str = &out_buf[..out_len as usize];
        let copy_len = std::cmp::min(out_str.len(), (conn_str_out_max as usize).saturating_sub(1));
        for i in 0..copy_len {
            unsafe {
                *conn_str_out.add(i) = out_str[i] as u16;
            }
        }
        unsafe {
            *conn_str_out.add(copy_len) = 0;
        }
        if !conn_str_out_len.is_null() {
            unsafe {
                *conn_str_out_len = out_len;
            }
        }
    }

    ret
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLDescribeColW(
    hstmt: SQLHSTMT,
    col_number: SQLUSMALLINT,
    col_name: *mut SQLWCHAR,
    buffer_length: SQLSMALLINT,
    name_length: *mut SQLSMALLINT,
    data_type: *mut SQLSMALLINT,
    column_size: *mut SQLULEN,
    decimal_digits: *mut SQLSMALLINT,
    nullable: *mut SQLSMALLINT,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    let idx = (col_number as usize).wrapping_sub(1);
    if idx >= stmt.columns.len() {
        return SQL_ERROR;
    }
    let col = &stmt.columns[idx];

    // Write column name as UTF-16
    if !col_name.is_null() && buffer_length > 0 {
        let name_utf16: Vec<u16> = col.name.encode_utf16().collect();
        let copy_len = std::cmp::min(name_utf16.len(), (buffer_length as usize).saturating_sub(1));
        for i in 0..copy_len {
            unsafe {
                *col_name.add(i) = name_utf16[i];
            }
        }
        unsafe {
            *col_name.add(copy_len) = 0;
        }
    }
    if !name_length.is_null() {
        unsafe {
            *name_length = col.name.encode_utf16().count() as SQLSMALLINT;
        }
    }
    if !data_type.is_null() {
        unsafe {
            *data_type = col.sql_type;
        }
    }
    if !column_size.is_null() {
        unsafe {
            *column_size = col.size;
        }
    }
    if !decimal_digits.is_null() {
        unsafe {
            *decimal_digits = col.decimal_digits;
        }
    }
    if !nullable.is_null() {
        unsafe {
            *nullable = col.nullable;
        }
    }
    SQL_SUCCESS
}

// ── SQLGetDiagField (needed by some driver managers) ────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetDiagField(
    _handle_type: SQLSMALLINT,
    _handle: SQLHANDLE,
    _rec_number: SQLSMALLINT,
    _diag_identifier: SQLSMALLINT,
    _diag_info: SQLPOINTER,
    _buffer_length: SQLSMALLINT,
    _string_length: *mut SQLSMALLINT,
) -> SQLRETURN {
    SQL_NO_DATA
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetDiagFieldW(
    handle_type: SQLSMALLINT,
    handle: SQLHANDLE,
    rec_number: SQLSMALLINT,
    diag_identifier: SQLSMALLINT,
    diag_info: SQLPOINTER,
    buffer_length: SQLSMALLINT,
    string_length: *mut SQLSMALLINT,
) -> SQLRETURN {
    SQLGetDiagField(
        handle_type,
        handle,
        rec_number,
        diag_identifier,
        diag_info,
        buffer_length,
        string_length,
    )
}

// ── SQLEndTran (needed by some apps) ────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLEndTran(
    handle_type: SQLSMALLINT,
    handle: SQLHANDLE,
    completion_type: SQLSMALLINT,
) -> SQLRETURN {
    if handle.is_null() {
        return SQL_INVALID_HANDLE;
    }

    let conn = match handle_type {
        SQL_HANDLE_DBC => unsafe { &mut *(handle as *mut Connection) },
        SQL_HANDLE_ENV => {
            // For ENV handle, commit/rollback all connections — simplified: just succeed
            return SQL_SUCCESS;
        }
        _ => return SQL_INVALID_HANDLE,
    };

    if !conn.in_transaction {
        return SQL_SUCCESS;
    }

    let sql = if completion_type == SQL_COMMIT {
        "COMMIT"
    } else {
        "ROLLBACK"
    };

    let client = match conn.client.as_mut() {
        Some(c) => c,
        None => return SQL_ERROR,
    };

    let result = crate::runtime::block_on(async {
        let mut w = StringRowWriter::new();
        client
            .batch_into(sql, &mut w)
            .await
            .map_err(|e| e.to_string())
    });

    conn.in_transaction = false;

    match result {
        Ok(()) => SQL_SUCCESS,
        Err(msg) => {
            conn.diagnostics.push(DiagRecord {
                state: "HY000".to_string(),
                native_error: 0,
                message: msg,
            });
            SQL_ERROR
        }
    }
}

// ── SQLCloseCursor ──────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLCloseCursor(hstmt: SQLHSTMT) -> SQLRETURN {
    SQLFreeStmt(hstmt, SQL_CLOSE)
}

// ── SQLNativeSql ────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLNativeSql(
    hdbc: SQLHDBC,
    in_statement: *const SQLCHAR,
    text_length: SQLINTEGER,
    out_statement: *mut SQLCHAR,
    buffer_length: SQLINTEGER,
    text_length_ptr: *mut SQLINTEGER,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let sql = unsafe { sql_str(in_statement, text_length as SQLSMALLINT) };
    let bytes = sql.as_bytes();
    if !text_length_ptr.is_null() {
        unsafe {
            *text_length_ptr = bytes.len() as SQLINTEGER;
        }
    }
    if !out_statement.is_null() && buffer_length > 0 {
        let copy_len = std::cmp::min(bytes.len(), (buffer_length as usize).saturating_sub(1));
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), out_statement, copy_len);
            *out_statement.add(copy_len) = 0;
        }
    }
    SQL_SUCCESS
}

// ── SQLNumParams ────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLNumParams(hstmt: SQLHSTMT, param_count: *mut SQLSMALLINT) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &*(hstmt as *const Statement) };
    if !param_count.is_null() {
        let count = stmt
            .prepared_sql
            .as_ref()
            .map(|s| s.matches('?').count() as SQLSMALLINT)
            .unwrap_or(0);
        unsafe {
            *param_count = count;
        }
    }
    SQL_SUCCESS
}

// ── SQLGetFunctions ─────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLGetFunctions(
    hdbc: SQLHDBC,
    function_id: SQLUSMALLINT,
    supported: *mut SQLUSMALLINT,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    if !supported.is_null() {
        // For SQL_API_ODBC3_ALL_FUNCTIONS (999), fill the bitmap
        if function_id == 999 {
            // 250 words, set relevant bits
            let arr = unsafe { std::slice::from_raw_parts_mut(supported, 250) };
            for item in arr.iter_mut() {
                *item = 0;
            }
            // Set bits for supported functions (ODBC API function IDs from sql.h/sqlext.h)
            let supported_funcs: &[u16] = &[
                6,    // SQL_API_SQLCOLATTRIBUTE
                7,    // SQL_API_SQLCONNECT
                8,    // SQL_API_SQLDESCRIBECOL
                9,    // SQL_API_SQLDISCONNECT
                11,   // SQL_API_SQLEXECDIRECT
                12,   // SQL_API_SQLEXECUTE
                13,   // SQL_API_SQLFETCH
                16,   // SQL_API_SQLFREESTMT
                18,   // SQL_API_SQLNUMRESULTCOLS
                19,   // SQL_API_SQLPREPARE
                20,   // SQL_API_SQLROWCOUNT
                41,   // SQL_API_SQLDRIVERCONNECT
                43,   // SQL_API_SQLGETDATA
                44,   // SQL_API_SQLGETFUNCTIONS
                45,   // SQL_API_SQLGETINFO
                47,   // SQL_API_SQLGETTYPEINFO
                54,   // SQL_API_SQLNATIVESQL
                60,   // SQL_API_SQLBINDPARAMETER (ODBC2 location)
                61,   // SQL_API_SQLMORERESULTS
                63,   // SQL_API_SQLNUMPARAMS
                72,   // SQL_API_SQLBINDPARAMETER
                1000, // SQL_API_SQLCLOSECURSOR
                1001, // SQL_API_SQLALLOCHANDLE
                1003, // SQL_API_SQLBINDPARAM
                1005, // SQL_API_SQLENDTRAN
                1006, // SQL_API_SQLFREEHANDLE
                1010, // SQL_API_SQLGETDIAGFIELD
                1011, // SQL_API_SQLGETDIAGREC
                1016, // SQL_API_SQLSETCONNECTATTR
                1019, // SQL_API_SQLSETENVATTR
                1020, // SQL_API_SQLSETSTMTATTR
            ];
            for &f in supported_funcs {
                let word = (f >> 4) as usize;
                let bit = f & 0xF;
                if word < 250 {
                    arr[word] |= 1 << bit;
                }
            }
        } else {
            // Individual function query — say yes to everything
            unsafe {
                *supported = 1;
            }
        }
    }
    SQL_SUCCESS
}

// ── SQLSpecialColumns / SQLStatistics / SQLPrimaryKeys (stubs) ──────

#[unsafe(no_mangle)]
pub extern "C" fn SQLSpecialColumns(
    hstmt: SQLHSTMT,
    _id_type: SQLUSMALLINT,
    _catalog: *const SQLCHAR,
    _catalog_len: SQLSMALLINT,
    _schema: *const SQLCHAR,
    _schema_len: SQLSMALLINT,
    _table: *const SQLCHAR,
    _table_len: SQLSMALLINT,
    _scope: SQLUSMALLINT,
    _nullable: SQLUSMALLINT,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.columns.clear();
    stmt.rows.clear();
    stmt.row_index = -1;
    stmt.executed = true;
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLSpecialColumnsW(
    hstmt: SQLHSTMT,
    id_type: SQLUSMALLINT,
    _catalog: *const SQLWCHAR,
    _catalog_len: SQLSMALLINT,
    _schema: *const SQLWCHAR,
    _schema_len: SQLSMALLINT,
    _table: *const SQLWCHAR,
    _table_len: SQLSMALLINT,
    scope: SQLUSMALLINT,
    nullable: SQLUSMALLINT,
) -> SQLRETURN {
    SQLSpecialColumns(
        hstmt,
        id_type,
        ptr::null(),
        0,
        ptr::null(),
        0,
        ptr::null(),
        0,
        scope,
        nullable,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLStatistics(
    hstmt: SQLHSTMT,
    _catalog: *const SQLCHAR,
    _catalog_len: SQLSMALLINT,
    _schema: *const SQLCHAR,
    _schema_len: SQLSMALLINT,
    _table: *const SQLCHAR,
    _table_len: SQLSMALLINT,
    _unique: SQLUSMALLINT,
    _reserved: SQLUSMALLINT,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.columns.clear();
    stmt.rows.clear();
    stmt.row_index = -1;
    stmt.executed = true;
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLStatisticsW(
    hstmt: SQLHSTMT,
    _catalog: *const SQLWCHAR,
    _catalog_len: SQLSMALLINT,
    _schema: *const SQLWCHAR,
    _schema_len: SQLSMALLINT,
    _table: *const SQLWCHAR,
    _table_len: SQLSMALLINT,
    unique: SQLUSMALLINT,
    reserved: SQLUSMALLINT,
) -> SQLRETURN {
    SQLStatistics(
        hstmt,
        ptr::null(),
        0,
        ptr::null(),
        0,
        ptr::null(),
        0,
        unique,
        reserved,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLPrimaryKeys(
    hstmt: SQLHSTMT,
    _catalog: *const SQLCHAR,
    _catalog_len: SQLSMALLINT,
    _schema: *const SQLCHAR,
    _schema_len: SQLSMALLINT,
    _table: *const SQLCHAR,
    _table_len: SQLSMALLINT,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.columns.clear();
    stmt.rows.clear();
    stmt.row_index = -1;
    stmt.executed = true;
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLPrimaryKeysW(
    hstmt: SQLHSTMT,
    _catalog: *const SQLWCHAR,
    _catalog_len: SQLSMALLINT,
    _schema: *const SQLWCHAR,
    _schema_len: SQLSMALLINT,
    _table: *const SQLWCHAR,
    _table_len: SQLSMALLINT,
) -> SQLRETURN {
    SQLPrimaryKeys(hstmt, ptr::null(), 0, ptr::null(), 0, ptr::null(), 0)
}

// ── SQLForeignKeys / SQLProcedures (stubs) ──────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLForeignKeys(
    hstmt: SQLHSTMT,
    _pk_cat: *const SQLCHAR,
    _pk_cat_len: SQLSMALLINT,
    _pk_sch: *const SQLCHAR,
    _pk_sch_len: SQLSMALLINT,
    _pk_tbl: *const SQLCHAR,
    _pk_tbl_len: SQLSMALLINT,
    _fk_cat: *const SQLCHAR,
    _fk_cat_len: SQLSMALLINT,
    _fk_sch: *const SQLCHAR,
    _fk_sch_len: SQLSMALLINT,
    _fk_tbl: *const SQLCHAR,
    _fk_tbl_len: SQLSMALLINT,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.columns.clear();
    stmt.rows.clear();
    stmt.row_index = -1;
    stmt.executed = true;
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLForeignKeysW(
    hstmt: SQLHSTMT,
    _pk_cat: *const SQLWCHAR,
    _pk_cat_len: SQLSMALLINT,
    _pk_sch: *const SQLWCHAR,
    _pk_sch_len: SQLSMALLINT,
    _pk_tbl: *const SQLWCHAR,
    _pk_tbl_len: SQLSMALLINT,
    _fk_cat: *const SQLWCHAR,
    _fk_cat_len: SQLSMALLINT,
    _fk_sch: *const SQLWCHAR,
    _fk_sch_len: SQLSMALLINT,
    _fk_tbl: *const SQLWCHAR,
    _fk_tbl_len: SQLSMALLINT,
) -> SQLRETURN {
    SQLForeignKeys(
        hstmt,
        ptr::null(),
        0,
        ptr::null(),
        0,
        ptr::null(),
        0,
        ptr::null(),
        0,
        ptr::null(),
        0,
        ptr::null(),
        0,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLProcedures(
    hstmt: SQLHSTMT,
    _catalog: *const SQLCHAR,
    _catalog_len: SQLSMALLINT,
    _schema: *const SQLCHAR,
    _schema_len: SQLSMALLINT,
    _proc: *const SQLCHAR,
    _proc_len: SQLSMALLINT,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };
    stmt.columns.clear();
    stmt.rows.clear();
    stmt.row_index = -1;
    stmt.executed = true;
    SQL_SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLProceduresW(
    hstmt: SQLHSTMT,
    _catalog: *const SQLWCHAR,
    _catalog_len: SQLSMALLINT,
    _schema: *const SQLWCHAR,
    _schema_len: SQLSMALLINT,
    _proc: *const SQLWCHAR,
    _proc_len: SQLSMALLINT,
) -> SQLRETURN {
    SQLProcedures(hstmt, ptr::null(), 0, ptr::null(), 0, ptr::null(), 0)
}

// ── SQLBindParameter (stub) ─────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLBindParameter(
    hstmt: SQLHSTMT,
    param_number: SQLUSMALLINT,
    _input_output_type: SQLSMALLINT,
    value_type: SQLSMALLINT,
    parameter_type: SQLSMALLINT,
    column_size: SQLULEN,
    decimal_digits: SQLSMALLINT,
    parameter_value: SQLPOINTER,
    buffer_length: SQLLEN,
    str_len_or_ind: *mut SQLLEN,
) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let stmt = unsafe { &mut *(hstmt as *mut Statement) };

    let param = BoundParam {
        param_number,
        value_type,
        parameter_type,
        column_size,
        decimal_digits,
        value_ptr: parameter_value,
        buffer_length,
        len_ind_ptr: str_len_or_ind,
    };

    // Replace if already bound at this position
    if let Some(existing) = stmt
        .bound_params
        .iter_mut()
        .find(|p| p.param_number == param_number)
    {
        *existing = param;
    } else {
        stmt.bound_params.push(param);
    }

    SQL_SUCCESS
}

// ── SQLCancel ───────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLCancel(hstmt: SQLHSTMT) -> SQLRETURN {
    if hstmt.is_null() {
        return SQL_INVALID_HANDLE;
    }
    SQL_SUCCESS
}

// ── SQLFetchScroll ──────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLFetchScroll(
    hstmt: SQLHSTMT,
    fetch_orientation: SQLSMALLINT,
    _fetch_offset: SQLLEN,
) -> SQLRETURN {
    if fetch_orientation == SQL_FETCH_NEXT {
        SQLFetch(hstmt)
    } else if hstmt.is_null() {
        SQL_INVALID_HANDLE
    } else {
        SQL_ERROR
    }
}

// ── SQLAllocConnect / SQLAllocEnv / SQLAllocStmt (ODBC 2.x compat) ──

#[unsafe(no_mangle)]
pub extern "C" fn SQLAllocConnect(henv: SQLHENV, phdbc: *mut SQLHDBC) -> SQLRETURN {
    alloc_handle_impl(SQL_HANDLE_DBC, henv, phdbc)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLAllocEnv(phenv: *mut SQLHENV) -> SQLRETURN {
    alloc_handle_impl(SQL_HANDLE_ENV, ptr::null_mut(), phenv)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLAllocStmt(hdbc: SQLHDBC, phstmt: *mut SQLHSTMT) -> SQLRETURN {
    alloc_handle_impl(SQL_HANDLE_STMT, hdbc, phstmt)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLFreeConnect(hdbc: SQLHDBC) -> SQLRETURN {
    free_handle_impl(SQL_HANDLE_DBC, hdbc)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLFreeEnv(henv: SQLHENV) -> SQLRETURN {
    free_handle_impl(SQL_HANDLE_ENV, henv)
}

// ── SQLError (ODBC 2.x compat) ─────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn SQLError(
    henv: SQLHENV,
    hdbc: SQLHDBC,
    hstmt: SQLHSTMT,
    sql_state: *mut SQLCHAR,
    native_error: *mut SQLINTEGER,
    message_text: *mut SQLCHAR,
    buffer_length: SQLSMALLINT,
    text_length: *mut SQLSMALLINT,
) -> SQLRETURN {
    // Try stmt first, then dbc, then env
    if !hstmt.is_null() {
        return SQLGetDiagRec(
            SQL_HANDLE_STMT,
            hstmt,
            1,
            sql_state,
            native_error,
            message_text,
            buffer_length,
            text_length,
        );
    }
    if !hdbc.is_null() {
        return SQLGetDiagRec(
            SQL_HANDLE_DBC,
            hdbc,
            1,
            sql_state,
            native_error,
            message_text,
            buffer_length,
            text_length,
        );
    }
    if !henv.is_null() {
        return SQLGetDiagRec(
            SQL_HANDLE_ENV,
            henv,
            1,
            sql_state,
            native_error,
            message_text,
            buffer_length,
            text_length,
        );
    }
    SQL_NO_DATA
}

// ── SQLConnect (simple connect with DSN params from odbc.ini) ───────

fn read_c_str(ptr: *const SQLCHAR, len: SQLSMALLINT) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let slice = if len == SQL_NTS as SQLSMALLINT || len < 0 {
        let mut end = 0;
        unsafe {
            while *ptr.add(end) != 0 {
                end += 1;
            }
        }
        unsafe { std::slice::from_raw_parts(ptr, end) }
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len as usize) }
    };
    String::from_utf8_lossy(slice).to_string()
}

fn resolve_dsn(dsn: &str) -> String {
    // Read DSN properties from ~/.odbc.ini or /etc/odbc.ini
    let mut props = std::collections::HashMap::new();
    for path in &[
        format!("{}/.odbc.ini", std::env::var("HOME").unwrap_or_default()),
        "/etc/odbc.ini".to_string(),
    ] {
        if let Ok(content) = std::fs::read_to_string(path) {
            let mut in_section = false;
            for line in content.lines() {
                let line = line.trim();
                if line.starts_with('[') && line.ends_with(']') {
                    in_section = &line[1..line.len() - 1] == dsn;
                } else if in_section {
                    if let Some(idx) = line.find('=') {
                        let key = line[..idx].trim().to_string();
                        let val = line[idx + 1..].trim().to_string();
                        props.insert(key, val);
                    }
                }
            }
            if !props.is_empty() {
                break;
            }
        }
    }
    // Build connection string from DSN properties
    props
        .iter()
        .filter(|(k, _)| k.to_lowercase() != "driver" && k.to_lowercase() != "description")
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(";")
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLConnect(
    hdbc: SQLHDBC,
    dsn: *const SQLCHAR,
    dsn_len: SQLSMALLINT,
    uid: *const SQLCHAR,
    uid_len: SQLSMALLINT,
    pwd: *const SQLCHAR,
    pwd_len: SQLSMALLINT,
) -> SQLRETURN {
    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let conn = unsafe { &mut *(hdbc as *mut Connection) };
    conn.diagnostics.clear();

    let dsn_name = read_c_str(dsn, dsn_len);
    let uid_str = read_c_str(uid, uid_len);
    let pwd_str = read_c_str(pwd, pwd_len);

    // Resolve DSN from odbc.ini
    let mut conn_str = resolve_dsn(&dsn_name);
    // Override UID/PWD if provided
    if !uid_str.is_empty() {
        conn_str = format!("{};UID={}", conn_str, uid_str);
    }
    if !pwd_str.is_empty() {
        conn_str = format!("{};PWD={}", conn_str, pwd_str);
    }

    connect::driver_connect(conn, &conn_str)
}

#[unsafe(no_mangle)]
pub extern "C" fn SQLConnectW(
    hdbc: SQLHDBC,
    dsn: *const SQLWCHAR,
    dsn_len: SQLSMALLINT,
    uid: *const SQLWCHAR,
    uid_len: SQLSMALLINT,
    pwd: *const SQLWCHAR,
    pwd_len: SQLSMALLINT,
) -> SQLRETURN {
    // Convert UTF-16 to UTF-8 and delegate
    fn wchar_to_string(ptr: *const u16, len: SQLSMALLINT) -> String {
        if ptr.is_null() {
            return String::new();
        }
        let count = if len < 0 {
            let mut n = 0;
            unsafe {
                while *ptr.add(n) != 0 {
                    n += 1;
                }
            }
            n
        } else {
            len as usize
        };
        let slice = unsafe { std::slice::from_raw_parts(ptr, count) };
        String::from_utf16_lossy(slice)
    }

    if hdbc.is_null() {
        return SQL_INVALID_HANDLE;
    }
    let conn = unsafe { &mut *(hdbc as *mut Connection) };
    conn.diagnostics.clear();

    let dsn_name = wchar_to_string(dsn, dsn_len);
    let uid_str = wchar_to_string(uid, uid_len);
    let pwd_str = wchar_to_string(pwd, pwd_len);

    let mut conn_str = resolve_dsn(&dsn_name);
    if !uid_str.is_empty() {
        conn_str = format!("{};UID={}", conn_str, uid_str);
    }
    if !pwd_str.is_empty() {
        conn_str = format!("{};PWD={}", conn_str, pwd_str);
    }

    connect::driver_connect(conn, &conn_str)
}
