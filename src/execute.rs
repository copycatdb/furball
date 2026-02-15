use crate::handle::*;
use crate::runtime;
use crate::types::*;

pub fn exec_direct(stmt: &mut Statement, sql: &str) -> SQLRETURN {
    let conn = unsafe { &mut *stmt.conn };

    let client = match conn.client.as_mut() {
        Some(c) => c,
        None => {
            stmt.diagnostics.push(DiagRecord {
                state: "08003".to_string(),
                native_error: 0,
                message: "Not connected".to_string(),
            });
            return SQL_ERROR;
        }
    };

    // If autocommit is OFF and we're not already in a transaction, start one
    if !conn.autocommit && !conn.in_transaction {
        let begin_result = runtime::block_on(async {
            let mut w = StringRowWriter::new();
            client
                .batch_into("BEGIN TRANSACTION", &mut w)
                .await
                .map_err(|e| e.to_string())
        });
        if let Err(msg) = begin_result {
            stmt.diagnostics.push(DiagRecord {
                state: "HY000".to_string(),
                native_error: 0,
                message: format!("Failed to begin transaction: {}", msg),
            });
            return SQL_ERROR;
        }
        conn.in_transaction = true;
    }

    let mut writer = StringRowWriter::new();
    let sql = sql.to_string();

    let result = runtime::block_on(async {
        client
            .batch_into(sql, &mut writer)
            .await
            .map_err(|e| e.to_string())
    });

    match result {
        Ok(()) => {
            stmt.columns = writer.columns;
            stmt.rows = writer.rows;
            stmt.row_index = -1;
            stmt.executed = true;
            stmt.read_offsets.clear();
            // For SELECT (has columns), row_count is -1 (unknown per ODBC spec)
            // For DML (no columns), use the done_rows from the Done token
            stmt.row_count = if stmt.columns.is_empty() {
                if writer.done_rows == 0 {
                    -1 // DDL or statement with no affected rows
                } else {
                    writer.done_rows as SQLLEN
                }
            } else {
                -1
            };
            SQL_SUCCESS
        }
        Err(msg) => {
            let (state, native) = map_sqlstate(&msg);
            stmt.diagnostics.push(DiagRecord {
                state,
                native_error: native,
                message: msg.clone(),
            });
            SQL_ERROR
        }
    }
}

/// Parse SQL Server error number from error message and map to SQLSTATE
fn map_sqlstate(msg: &str) -> (String, i32) {
    // Try to extract error number: patterns like "Msg 2627" or "number: 2627" or just the number
    let native = extract_error_number(msg);
    let state = match native {
        2627 | 2601 | 547 => "23000", // integrity constraint violation
        208 => "42S02",               // table not found
        156 | 102 => "42000",         // syntax error
        _ => "HY000",                 // general error
    };
    (state.to_string(), native)
}

fn extract_error_number(msg: &str) -> i32 {
    // Look for "code: NNNN" pattern (tabby/tiberius error format)
    if let Some(idx) = msg.find("code: ") {
        let rest = &msg[idx + 6..];
        if let Some(end) = rest.find(|c: char| !c.is_ascii_digit()) {
            if let Ok(n) = rest[..end].parse::<i32>() {
                return n;
            }
        } else if let Ok(n) = rest.parse::<i32>() {
            return n;
        }
    }
    // Look for "number: NNNN" pattern
    if let Some(idx) = msg.find("number: ") {
        let rest = &msg[idx + 8..];
        if let Some(end) = rest.find(|c: char| !c.is_ascii_digit()) {
            if let Ok(n) = rest[..end].parse::<i32>() {
                return n;
            }
        } else if let Ok(n) = rest.parse::<i32>() {
            return n;
        }
    }
    // Look for "Msg NNNN" pattern
    if let Some(idx) = msg.find("Msg ") {
        let rest = &msg[idx + 4..];
        if let Some(end) = rest.find(|c: char| !c.is_ascii_digit()) {
            if let Ok(n) = rest[..end].parse::<i32>() {
                return n;
            }
        }
    }
    0
}
