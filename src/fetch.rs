use crate::handle::*;
use crate::types::*;
use std::ptr;

pub fn fetch(stmt: &mut Statement) -> SQLRETURN {
    if !stmt.executed {
        return SQL_ERROR;
    }
    stmt.row_index += 1;
    if stmt.row_index as usize >= stmt.rows.len() {
        SQL_NO_DATA
    } else {
        SQL_SUCCESS
    }
}

pub fn get_data(
    stmt: &Statement,
    col: SQLUSMALLINT,
    target_type: SQLSMALLINT,
    target_value: SQLPOINTER,
    buffer_length: SQLLEN,
    str_len_or_ind: *mut SQLLEN,
) -> SQLRETURN {
    if stmt.row_index < 0 || stmt.row_index as usize >= stmt.rows.len() {
        return SQL_ERROR;
    }
    let row = &stmt.rows[stmt.row_index as usize];
    let col_idx = (col as usize).wrapping_sub(1); // 1-based to 0-based
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
            // Determine effective target type
            let eff_type = if target_type == SQL_C_DEFAULT {
                // Use column's SQL type to pick a C type
                if col_idx < stmt.columns.len() {
                    match stmt.columns[col_idx].sql_type {
                        SQL_INTEGER => SQL_C_LONG,
                        SQL_SMALLINT => SQL_C_SHORT,
                        SQL_BIGINT => SQL_C_SBIGINT,
                        SQL_DOUBLE | SQL_FLOAT => SQL_C_DOUBLE,
                        SQL_REAL => SQL_C_FLOAT,
                        _ => SQL_C_CHAR,
                    }
                } else {
                    SQL_C_CHAR
                }
            } else {
                target_type
            };

            match eff_type {
                SQL_C_LONG | SQL_C_SLONG => {
                    let v: i32 = val.parse().unwrap_or(0);
                    if !target_value.is_null() {
                        unsafe {
                            *(target_value as *mut i32) = v;
                        }
                    }
                    if !str_len_or_ind.is_null() {
                        unsafe {
                            *str_len_or_ind = 4;
                        }
                    }
                    SQL_SUCCESS
                }
                SQL_C_SHORT => {
                    let v: i16 = val.parse().unwrap_or(0);
                    if !target_value.is_null() {
                        unsafe {
                            *(target_value as *mut i16) = v;
                        }
                    }
                    if !str_len_or_ind.is_null() {
                        unsafe {
                            *str_len_or_ind = 2;
                        }
                    }
                    SQL_SUCCESS
                }
                SQL_C_SBIGINT => {
                    let v: i64 = val.parse().unwrap_or(0);
                    if !target_value.is_null() {
                        unsafe {
                            *(target_value as *mut i64) = v;
                        }
                    }
                    if !str_len_or_ind.is_null() {
                        unsafe {
                            *str_len_or_ind = 8;
                        }
                    }
                    SQL_SUCCESS
                }
                SQL_C_DOUBLE => {
                    let v: f64 = val.parse().unwrap_or(0.0);
                    if !target_value.is_null() {
                        unsafe {
                            *(target_value as *mut f64) = v;
                        }
                    }
                    if !str_len_or_ind.is_null() {
                        unsafe {
                            *str_len_or_ind = 8;
                        }
                    }
                    SQL_SUCCESS
                }
                SQL_C_FLOAT => {
                    let v: f32 = val.parse().unwrap_or(0.0);
                    if !target_value.is_null() {
                        unsafe {
                            *(target_value as *mut f32) = v;
                        }
                    }
                    if !str_len_or_ind.is_null() {
                        unsafe {
                            *str_len_or_ind = 4;
                        }
                    }
                    SQL_SUCCESS
                }
                SQL_C_WCHAR => {
                    // UTF-16
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
                            *dest.add(copy_count) = 0;
                        }
                        if utf16.len() >= buf_u16_cap {
                            return SQL_SUCCESS_WITH_INFO;
                        }
                    }
                    SQL_SUCCESS
                }
                _ => {
                    // SQL_C_CHAR or unknown: return as ANSI string
                    let bytes = val.as_bytes();
                    let data_len = bytes.len() as SQLLEN;

                    if !str_len_or_ind.is_null() {
                        unsafe {
                            *str_len_or_ind = data_len;
                        }
                    }

                    if !target_value.is_null() && buffer_length > 0 {
                        let copy_len = std::cmp::min(data_len, buffer_length - 1) as usize;
                        unsafe {
                            ptr::copy_nonoverlapping(
                                bytes.as_ptr(),
                                target_value as *mut u8,
                                copy_len,
                            );
                            *((target_value as *mut u8).add(copy_len)) = 0;
                        }
                        if data_len >= buffer_length {
                            return SQL_SUCCESS_WITH_INFO;
                        }
                    }
                    SQL_SUCCESS
                }
            }
        }
    }
}

pub fn num_result_cols(stmt: &Statement) -> SQLSMALLINT {
    stmt.columns.len() as SQLSMALLINT
}

pub fn describe_col(
    stmt: &Statement,
    col_number: SQLUSMALLINT,
    col_name: *mut SQLCHAR,
    buffer_length: SQLSMALLINT,
    name_length: *mut SQLSMALLINT,
    data_type: *mut SQLSMALLINT,
    column_size: *mut SQLULEN,
    decimal_digits: *mut SQLSMALLINT,
    nullable: *mut SQLSMALLINT,
) -> SQLRETURN {
    let idx = (col_number as usize).wrapping_sub(1);
    if idx >= stmt.columns.len() {
        return SQL_ERROR;
    }
    let col = &stmt.columns[idx];

    // Copy column name
    if !col_name.is_null() && buffer_length > 0 {
        let name_bytes = col.name.as_bytes();
        let copy_len = std::cmp::min(name_bytes.len(), (buffer_length as usize).saturating_sub(1));
        unsafe {
            ptr::copy_nonoverlapping(name_bytes.as_ptr(), col_name, copy_len);
            *col_name.add(copy_len) = 0;
        }
    }
    if !name_length.is_null() {
        unsafe {
            *name_length = col.name.len() as SQLSMALLINT;
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
