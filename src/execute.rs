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

    // If we were previously streaming, drain the old stream first
    if stmt.streaming {
        let _ = runtime::block_on(async { client.batch_drain().await });
        stmt.streaming = false;
    }

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

    let sql = sql.to_string();

    // Use streaming API: send query, read only until metadata
    let mut rows_affected = 0u64;
    let result = runtime::block_on(async {
        client
            .batch_start_with_rowcount(sql, &mut rows_affected)
            .await
            .map_err(|e| e.to_string())
    });

    match result {
        Ok(columns) => {
            if columns.is_empty() {
                // No result set (DML statement) — the stream is already done
                stmt.columns = Vec::new();
                stmt.rows = Vec::new();
                stmt.row_count = if rows_affected == 0 {
                    -1
                } else {
                    rows_affected as SQLLEN
                };
                stmt.row_index = -1;
                stmt.executed = true;
                stmt.streaming = false;
                stmt.read_offsets.clear();
                stmt.pending_result_sets.clear();
                stmt.current_row.clear();
            } else {
                // Has result set — set up columns, enable streaming
                stmt.columns = columns
                    .iter()
                    .map(|c| {
                        let (sql_type, size, decimal_digits, nullable) = sql_type_from_column(c);
                        ColumnDesc {
                            name: c.name().to_string(),
                            sql_type,
                            size,
                            decimal_digits,
                            nullable,
                        }
                    })
                    .collect();
                stmt.rows = Vec::new(); // no rows buffered
                stmt.row_count = -1;
                stmt.row_index = -1;
                stmt.executed = true;
                stmt.streaming = true;
                stmt.read_offsets.clear();
                stmt.pending_result_sets.clear();
                stmt.current_row.clear();
            }
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
    let native = extract_error_number(msg);
    let state = match native {
        2627 | 2601 | 547 => "23000",
        208 => "42S02",
        156 | 102 => "42000",
        _ => "HY000",
    };
    (state.to_string(), native)
}

fn extract_error_number(msg: &str) -> i32 {
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
