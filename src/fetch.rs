use crate::handle::*;
use crate::runtime;
use crate::types::*;
use std::ptr;
use tabby::BatchFetchResult;

/// Single-row writer that appends values to a Vec<CellValue>
struct SingleRowWriter<'a> {
    row: &'a mut Vec<CellValue>,
    info_messages: Vec<(u32, String)>,
}

impl<'a> tabby::RowWriter for SingleRowWriter<'a> {
    fn write_null(&mut self, _col: usize) {
        self.row.push(CellValue::Null);
    }
    fn write_bool(&mut self, _col: usize, val: bool) {
        self.row.push(CellValue::Bool(val));
    }
    fn write_u8(&mut self, _col: usize, val: u8) {
        self.row.push(CellValue::U8(val));
    }
    fn write_i16(&mut self, _col: usize, val: i16) {
        self.row.push(CellValue::I16(val));
    }
    fn write_i32(&mut self, _col: usize, val: i32) {
        self.row.push(CellValue::I32(val));
    }
    fn write_i64(&mut self, _col: usize, val: i64) {
        self.row.push(CellValue::I64(val));
    }
    fn write_f32(&mut self, _col: usize, val: f32) {
        self.row.push(CellValue::F32(val));
    }
    fn write_f64(&mut self, _col: usize, val: f64) {
        self.row.push(CellValue::F64(val));
    }
    fn write_str(&mut self, _col: usize, val: &str) {
        self.row.push(CellValue::String(val.to_string()));
    }
    fn write_utf16(&mut self, _col: usize, val: &[u16]) {
        self.row
            .push(CellValue::String(String::from_utf16_lossy(val)));
    }
    fn write_bytes(&mut self, _col: usize, val: &[u8]) {
        self.row.push(CellValue::Bytes(val.to_vec()));
    }
    fn write_date(&mut self, _col: usize, days: i32) {
        self.row.push(CellValue::Date { days });
    }
    fn write_time(&mut self, _col: usize, nanos: i64) {
        self.row.push(CellValue::Time { nanos });
    }
    fn write_datetime(&mut self, _col: usize, micros: i64) {
        self.row.push(CellValue::DateTime { micros });
    }
    fn write_datetimeoffset(&mut self, _col: usize, micros: i64, offset_minutes: i16) {
        self.row.push(CellValue::DateTimeOffset {
            micros,
            offset_min: offset_minutes,
        });
    }
    fn write_decimal(&mut self, _col: usize, value: i128, precision: u8, scale: u8) {
        self.row.push(CellValue::Decimal {
            value,
            precision,
            scale,
        });
    }
    fn write_guid(&mut self, _col: usize, bytes: &[u8; 16]) {
        self.row.push(CellValue::Guid(*bytes));
    }
    fn on_info(&mut self, number: u32, message: &str) {
        self.info_messages.push((number, message.to_string()));
    }
}

pub fn fetch(stmt: &mut Statement) -> SQLRETURN {
    if !stmt.executed {
        return SQL_ERROR;
    }

    // Reset read offsets on each new row
    stmt.read_offsets.clear();

    if stmt.streaming {
        // If prefetch buffer is empty and no terminal state, fill it
        if stmt.prefetch_buffer.is_empty() && stmt.prefetch_done.is_none() {
            let conn = unsafe { &mut *stmt.conn };
            let client = match conn.client.as_mut() {
                Some(c) => c,
                None => return SQL_ERROR,
            };

            let mut row_buf = Vec::new();
            let mut info_msgs = Vec::new();
            let string_buf = &mut stmt.stream_string_buf;
            let bytes_buf = &mut stmt.stream_bytes_buf;
            let prefetch_buffer = &mut stmt.prefetch_buffer;

            let terminal = runtime::block_on(async {
                for _ in 0..256 {
                    row_buf.clear();
                    let mut writer = SingleRowWriter {
                        row: &mut row_buf,
                        info_messages: Vec::new(),
                    };
                    match client
                        .batch_fetch_row(&mut writer, string_buf, bytes_buf)
                        .await
                    {
                        Ok(BatchFetchResult::Row) => {
                            info_msgs.extend(writer.info_messages);
                            prefetch_buffer.push_back(std::mem::replace(&mut row_buf, Vec::new()));
                        }
                        Ok(BatchFetchResult::Done(_)) => {
                            info_msgs.extend(writer.info_messages);
                            return Some(PrefetchTerminal::Done);
                        }
                        Ok(BatchFetchResult::MoreResults) => {
                            info_msgs.extend(writer.info_messages);
                            return Some(PrefetchTerminal::MoreResults);
                        }
                        Err(e) => {
                            info_msgs.extend(writer.info_messages);
                            return Some(PrefetchTerminal::Error(e.to_string()));
                        }
                    }
                }
                None // filled 256 rows, no terminal yet
            });

            // Transfer info messages
            for (number, message) in info_msgs {
                stmt.diagnostics.push(DiagRecord {
                    state: "01000".to_string(),
                    native_error: number as i32,
                    message,
                });
            }

            stmt.prefetch_done = terminal;
        }

        // Pop from buffer
        match stmt.prefetch_buffer.pop_front() {
            Some(row) => {
                stmt.rows.clear();
                stmt.rows.push(row);
                stmt.row_index = 0;
                SQL_SUCCESS
            }
            None => {
                // Buffer empty — handle terminal state
                match stmt.prefetch_done.take() {
                    Some(PrefetchTerminal::Done) => {
                        stmt.streaming = false;
                        SQL_NO_DATA
                    }
                    Some(PrefetchTerminal::MoreResults) => {
                        stmt.streaming = false;
                        SQL_NO_DATA
                    }
                    Some(PrefetchTerminal::Error(msg)) => {
                        stmt.streaming = false;
                        stmt.diagnostics.push(DiagRecord {
                            state: "HY000".to_string(),
                            native_error: 0,
                            message: msg,
                        });
                        SQL_ERROR
                    }
                    None => SQL_NO_DATA,
                }
            }
        }
    } else {
        // Non-streaming mode (buffered rows from pending_result_sets, or legacy)
        stmt.row_index += 1;
        if stmt.row_index as usize >= stmt.rows.len() {
            SQL_NO_DATA
        } else {
            SQL_SUCCESS
        }
    }
}

/// Helper: write a fixed-size numeric value to the target buffer
unsafe fn write_fixed<T: Copy>(
    target_value: SQLPOINTER,
    str_len_or_ind: *mut SQLLEN,
    val: T,
    read_offsets: &mut [usize],
    col_idx: usize,
) -> SQLRETURN {
    if !target_value.is_null() {
        *(target_value as *mut T) = val;
    }
    if !str_len_or_ind.is_null() {
        *str_len_or_ind = std::mem::size_of::<T>() as SQLLEN;
    }
    read_offsets[col_idx] = 0;
    SQL_SUCCESS
}

/// Helper: convert CellValue to i64 for numeric cross-type conversions
fn cell_to_i64(cell: &CellValue) -> i64 {
    match cell {
        CellValue::Bool(v) => *v as i64,
        CellValue::U8(v) => *v as i64,
        CellValue::I16(v) => *v as i64,
        CellValue::I32(v) => *v as i64,
        CellValue::I64(v) => *v,
        CellValue::F32(v) => *v as i64,
        CellValue::F64(v) => *v as i64,
        CellValue::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn cell_to_f64(cell: &CellValue) -> f64 {
    match cell {
        CellValue::Bool(v) => {
            if *v {
                1.0
            } else {
                0.0
            }
        }
        CellValue::U8(v) => *v as f64,
        CellValue::I16(v) => *v as f64,
        CellValue::I32(v) => *v as f64,
        CellValue::I64(v) => *v as f64,
        CellValue::F32(v) => *v as f64,
        CellValue::F64(v) => *v,
        CellValue::String(s) => s.parse().unwrap_or(0.0),
        CellValue::Decimal { value, scale, .. } => *value as f64 / 10f64.powi(*scale as i32),
        _ => 0.0,
    }
}

pub fn get_data(
    stmt: &mut Statement,
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

    // Ensure read_offsets is large enough
    while stmt.read_offsets.len() <= col_idx {
        stmt.read_offsets.push(0);
    }

    let cell = &row[col_idx];

    // Handle NULL
    if matches!(cell, CellValue::Null) {
        if !str_len_or_ind.is_null() {
            unsafe {
                *str_len_or_ind = SQL_NULL_DATA;
            }
        }
        stmt.read_offsets[col_idx] = 0;
        return SQL_SUCCESS;
    }

    // Determine effective target type
    let eff_type = if target_type == SQL_C_DEFAULT {
        if col_idx < stmt.columns.len() {
            match stmt.columns[col_idx].sql_type {
                SQL_INTEGER => SQL_C_LONG,
                SQL_SMALLINT => SQL_C_SHORT,
                SQL_BIGINT => SQL_C_SBIGINT,
                SQL_DOUBLE | SQL_FLOAT => SQL_C_DOUBLE,
                SQL_REAL => SQL_C_FLOAT,
                SQL_BIT => SQL_C_BIT,
                SQL_TYPE_TIMESTAMP => SQL_C_TYPE_TIMESTAMP,
                SQL_TYPE_DATE => SQL_C_TYPE_DATE,
                SQL_TYPE_TIME => SQL_C_TYPE_TIME,
                SQL_BINARY | SQL_VARBINARY | SQL_LONGVARBINARY => SQL_C_BINARY,
                SQL_GUID => SQL_C_GUID,
                SQL_TINYINT => SQL_C_UTINYINT,
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
            let v: i32 = match cell {
                CellValue::I32(v) => *v,
                CellValue::Bool(v) => *v as i32,
                CellValue::U8(v) => *v as i32,
                CellValue::I16(v) => *v as i32,
                _ => cell_to_i64(cell) as i32,
            };
            unsafe {
                write_fixed(
                    target_value,
                    str_len_or_ind,
                    v,
                    &mut stmt.read_offsets,
                    col_idx,
                )
            }
        }
        SQL_C_SHORT => {
            let v: i16 = match cell {
                CellValue::I16(v) => *v,
                _ => cell_to_i64(cell) as i16,
            };
            unsafe {
                write_fixed(
                    target_value,
                    str_len_or_ind,
                    v,
                    &mut stmt.read_offsets,
                    col_idx,
                )
            }
        }
        SQL_C_SBIGINT => {
            let v: i64 = match cell {
                CellValue::I64(v) => *v,
                _ => cell_to_i64(cell),
            };
            unsafe {
                write_fixed(
                    target_value,
                    str_len_or_ind,
                    v,
                    &mut stmt.read_offsets,
                    col_idx,
                )
            }
        }
        SQL_C_DOUBLE => {
            let v: f64 = match cell {
                CellValue::F64(v) => *v,
                _ => cell_to_f64(cell),
            };
            unsafe {
                write_fixed(
                    target_value,
                    str_len_or_ind,
                    v,
                    &mut stmt.read_offsets,
                    col_idx,
                )
            }
        }
        SQL_C_FLOAT => {
            let v: f32 = match cell {
                CellValue::F32(v) => *v,
                _ => cell_to_f64(cell) as f32,
            };
            unsafe {
                write_fixed(
                    target_value,
                    str_len_or_ind,
                    v,
                    &mut stmt.read_offsets,
                    col_idx,
                )
            }
        }
        SQL_C_BIT => {
            let v: u8 = match cell {
                CellValue::Bool(b) => {
                    if *b {
                        1
                    } else {
                        0
                    }
                }
                CellValue::U8(v) => {
                    if *v != 0 {
                        1
                    } else {
                        0
                    }
                }
                CellValue::String(s) => {
                    if s == "0" || s.is_empty() {
                        0
                    } else {
                        1
                    }
                }
                _ => {
                    if cell_to_i64(cell) != 0 {
                        1
                    } else {
                        0
                    }
                }
            };
            unsafe {
                write_fixed(
                    target_value,
                    str_len_or_ind,
                    v,
                    &mut stmt.read_offsets,
                    col_idx,
                )
            }
        }
        SQL_C_UTINYINT | SQL_C_STINYINT => {
            let v: u8 = match cell {
                CellValue::U8(v) => *v,
                _ => cell_to_i64(cell) as u8,
            };
            unsafe {
                write_fixed(
                    target_value,
                    str_len_or_ind,
                    v,
                    &mut stmt.read_offsets,
                    col_idx,
                )
            }
        }
        SQL_C_WCHAR => {
            // Fast path: if we already have UTF-16, skip encoding entirely
            let utf16: std::borrow::Cow<[u16]> = match cell {
                CellValue::Utf16(u) => std::borrow::Cow::Borrowed(u.as_slice()),
                _ => {
                    let val = cell.to_string_repr().unwrap_or_default();
                    std::borrow::Cow::Owned(val.encode_utf16().collect())
                }
            };
            let total_bytes = (utf16.len() * 2) as SQLLEN;
            let offset = stmt.read_offsets[col_idx]; // offset in u16 units
            let remaining_u16 = if offset < utf16.len() {
                &utf16[offset..]
            } else {
                if !str_len_or_ind.is_null() {
                    unsafe {
                        *str_len_or_ind = 0;
                    }
                }
                stmt.read_offsets[col_idx] = 0;
                return SQL_NO_DATA;
            };

            if offset == 0 {
                if !str_len_or_ind.is_null() {
                    unsafe {
                        *str_len_or_ind = total_bytes;
                    }
                }
            } else if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = (remaining_u16.len() * 2) as SQLLEN;
                }
            }

            if !target_value.is_null() && buffer_length > 0 {
                let buf_u16_cap = (buffer_length as usize) / 2;
                let copy_count = std::cmp::min(remaining_u16.len(), buf_u16_cap.saturating_sub(1));
                let dest = target_value as *mut u16;
                unsafe {
                    ptr::copy_nonoverlapping(remaining_u16.as_ptr(), dest, copy_count);
                    *dest.add(copy_count) = 0;
                }
                stmt.read_offsets[col_idx] = offset + copy_count;
                if remaining_u16.len() > copy_count {
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
            stmt.read_offsets[col_idx] = 0;
            SQL_SUCCESS
        }
        SQL_C_TYPE_TIMESTAMP => {
            let ts = match cell {
                CellValue::DateTime { micros } => {
                    let (year, month, day, h, mi, sec, millis) = micros_to_timestamp_parts(*micros);
                    SqlTimestampStruct {
                        year: year as i16,
                        month: month as u16,
                        day: day as u16,
                        hour: h as u16,
                        minute: mi as u16,
                        second: sec as u16,
                        fraction: millis * 1_000_000, // millis -> nanoseconds
                    }
                }
                CellValue::DateTimeOffset { micros, .. } => {
                    let (year, month, day, h, mi, sec, millis) = micros_to_timestamp_parts(*micros);
                    SqlTimestampStruct {
                        year: year as i16,
                        month: month as u16,
                        day: day as u16,
                        hour: h as u16,
                        minute: mi as u16,
                        second: sec as u16,
                        fraction: millis * 1_000_000,
                    }
                }
                CellValue::Date { .. } => {
                    let s = cell.to_string_repr().unwrap_or_default();
                    parse_timestamp(&s)
                }
                _ => {
                    let s = cell.to_string_repr().unwrap_or_default();
                    parse_timestamp(&s)
                }
            };
            if !target_value.is_null() {
                unsafe {
                    *(target_value as *mut SqlTimestampStruct) = ts;
                }
            }
            if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = std::mem::size_of::<SqlTimestampStruct>() as SQLLEN;
                }
            }
            stmt.read_offsets[col_idx] = 0;
            SQL_SUCCESS
        }
        SQL_C_TYPE_DATE => {
            let ts = match cell {
                CellValue::DateTime { micros } | CellValue::DateTimeOffset { micros, .. } => {
                    let (year, month, day, ..) = micros_to_timestamp_parts(*micros);
                    SqlDateStruct {
                        year: year as i16,
                        month: month as u16,
                        day: day as u16,
                    }
                }
                CellValue::Date { .. } => {
                    let s = cell.to_string_repr().unwrap_or_default();
                    let ts = parse_timestamp(&s);
                    SqlDateStruct {
                        year: ts.year,
                        month: ts.month,
                        day: ts.day,
                    }
                }
                _ => {
                    let s = cell.to_string_repr().unwrap_or_default();
                    let ts = parse_timestamp(&s);
                    SqlDateStruct {
                        year: ts.year,
                        month: ts.month,
                        day: ts.day,
                    }
                }
            };
            if !target_value.is_null() {
                unsafe {
                    *(target_value as *mut SqlDateStruct) = ts;
                }
            }
            if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = std::mem::size_of::<SqlDateStruct>() as SQLLEN;
                }
            }
            stmt.read_offsets[col_idx] = 0;
            SQL_SUCCESS
        }
        SQL_C_TYPE_TIME => {
            let ts = match cell {
                CellValue::Time { nanos } => {
                    let total_secs = (*nanos / 1_000_000_000) as u32;
                    SqlTimeStruct {
                        hour: (total_secs / 3600) as u16,
                        minute: ((total_secs % 3600) / 60) as u16,
                        second: (total_secs % 60) as u16,
                    }
                }
                CellValue::DateTime { micros } | CellValue::DateTimeOffset { micros, .. } => {
                    let (_, _, _, h, mi, sec, _) = micros_to_timestamp_parts(*micros);
                    SqlTimeStruct {
                        hour: h as u16,
                        minute: mi as u16,
                        second: sec as u16,
                    }
                }
                _ => {
                    let s = cell.to_string_repr().unwrap_or_default();
                    let ts = parse_timestamp(&s);
                    SqlTimeStruct {
                        hour: ts.hour,
                        minute: ts.minute,
                        second: ts.second,
                    }
                }
            };
            if !target_value.is_null() {
                unsafe {
                    *(target_value as *mut SqlTimeStruct) = ts;
                }
            }
            if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = std::mem::size_of::<SqlTimeStruct>() as SQLLEN;
                }
            }
            stmt.read_offsets[col_idx] = 0;
            SQL_SUCCESS
        }
        SQL_C_BINARY => {
            let bytes: Vec<u8> = match cell {
                CellValue::Bytes(b) => b.clone(),
                CellValue::Guid(g) => g.to_vec(),
                _ => {
                    let s = cell.to_string_repr().unwrap_or_default();
                    if s.chars().all(|c| c.is_ascii_hexdigit()) && s.len() % 2 == 0 {
                        hex_decode(&s)
                    } else {
                        s.into_bytes()
                    }
                }
            };
            let offset = stmt.read_offsets[col_idx];
            let remaining = if offset < bytes.len() {
                &bytes[offset..]
            } else {
                if !str_len_or_ind.is_null() {
                    unsafe {
                        *str_len_or_ind = 0;
                    }
                }
                stmt.read_offsets[col_idx] = 0;
                return SQL_NO_DATA;
            };

            let remaining_len = remaining.len() as SQLLEN;
            if offset == 0 {
                if !str_len_or_ind.is_null() {
                    unsafe {
                        *str_len_or_ind = bytes.len() as SQLLEN;
                    }
                }
            } else if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = remaining_len;
                }
            }

            if !target_value.is_null() && buffer_length > 0 {
                let copy_len = std::cmp::min(remaining_len, buffer_length) as usize;
                unsafe {
                    ptr::copy_nonoverlapping(remaining.as_ptr(), target_value as *mut u8, copy_len);
                }
                stmt.read_offsets[col_idx] = offset + copy_len;
                if remaining.len() > copy_len {
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
            stmt.read_offsets[col_idx] = 0;
            SQL_SUCCESS
        }
        SQL_C_GUID => {
            let guid = match cell {
                CellValue::Guid(bytes) => SqlGuid {
                    data1: u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                    data2: u16::from_be_bytes([bytes[4], bytes[5]]),
                    data3: u16::from_be_bytes([bytes[6], bytes[7]]),
                    data4: [
                        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                        bytes[15],
                    ],
                },
                _ => {
                    let s = cell.to_string_repr().unwrap_or_default();
                    parse_guid(&s)
                }
            };
            if !target_value.is_null() {
                unsafe {
                    *(target_value as *mut SqlGuid) = guid;
                }
            }
            if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = 16;
                }
            }
            stmt.read_offsets[col_idx] = 0;
            SQL_SUCCESS
        }
        _ => {
            // SQL_C_CHAR or unknown: return as ANSI string with chunked read support
            let val = cell.to_string_repr().unwrap_or_default();
            let bytes = val.as_bytes();
            let offset = stmt.read_offsets[col_idx];

            let remaining = if offset < bytes.len() {
                &bytes[offset..]
            } else if offset > 0 {
                if !str_len_or_ind.is_null() {
                    unsafe {
                        *str_len_or_ind = 0;
                    }
                }
                stmt.read_offsets[col_idx] = 0;
                return SQL_NO_DATA;
            } else {
                bytes
            };

            let remaining_len = remaining.len() as SQLLEN;

            if offset == 0 {
                if !str_len_or_ind.is_null() {
                    unsafe {
                        *str_len_or_ind = bytes.len() as SQLLEN;
                    }
                }
            } else if !str_len_or_ind.is_null() {
                unsafe {
                    *str_len_or_ind = remaining_len;
                }
            }

            if !target_value.is_null() && buffer_length > 0 {
                let copy_len = std::cmp::min(remaining_len, buffer_length - 1) as usize;
                unsafe {
                    ptr::copy_nonoverlapping(remaining.as_ptr(), target_value as *mut u8, copy_len);
                    *((target_value as *mut u8).add(copy_len)) = 0;
                }
                stmt.read_offsets[col_idx] = offset + copy_len;
                if remaining.len() > copy_len {
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
            stmt.read_offsets[col_idx] = 0;
            SQL_SUCCESS
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

fn parse_timestamp(s: &str) -> SqlTimestampStruct {
    let mut ts = SqlTimestampStruct::default();
    // "YYYY-MM-DD HH:MM:SS.fff"
    let parts: Vec<&str> = s.splitn(2, [' ', 'T']).collect();
    if let Some(date_part) = parts.first() {
        let d: Vec<&str> = date_part.split('-').collect();
        if d.len() >= 3 {
            ts.year = d[0].parse().unwrap_or(0);
            ts.month = d[1].parse().unwrap_or(0);
            ts.day = d[2].parse().unwrap_or(0);
        }
    }
    if let Some(time_part) = parts.get(1) {
        // Strip timezone offset if present
        let time_str = time_part.split(['+', '-']).next().unwrap_or(time_part);
        let t: Vec<&str> = time_str.split(':').collect();
        if t.len() >= 3 {
            ts.hour = t[0].parse().unwrap_or(0);
            ts.minute = t[1].parse().unwrap_or(0);
            // seconds may have fractional part
            let sec_parts: Vec<&str> = t[2].split('.').collect();
            ts.second = sec_parts[0].parse().unwrap_or(0);
            if sec_parts.len() > 1 {
                let frac_str = sec_parts[1];
                // Pad or truncate to 9 digits (nanoseconds)
                let padded = format!("{:0<9}", frac_str);
                ts.fraction = padded[..9].parse().unwrap_or(0);
            }
        }
    }
    ts
}

fn hex_decode(s: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(s.len() / 2);
    let mut chars = s.chars();
    while let (Some(a), Some(b)) = (chars.next(), chars.next()) {
        let byte = u8::from_str_radix(&format!("{}{}", a, b), 16).unwrap_or(0);
        bytes.push(byte);
    }
    bytes
}

fn parse_guid(s: &str) -> SqlGuid {
    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    let bytes = hex_decode(&hex);
    if bytes.len() >= 16 {
        SqlGuid {
            data1: u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            data2: u16::from_be_bytes([bytes[4], bytes[5]]),
            data3: u16::from_be_bytes([bytes[6], bytes[7]]),
            data4: [
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ],
        }
    } else {
        SqlGuid {
            data1: 0,
            data2: 0,
            data3: 0,
            data4: [0; 8],
        }
    }
}
