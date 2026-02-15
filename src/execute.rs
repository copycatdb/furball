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
            // For SELECT (has columns), row_count is -1 (unknown per ODBC spec)
            // For DML (no columns), use the done_rows from the Done token
            stmt.row_count = if stmt.columns.is_empty() {
                writer.done_rows as SQLLEN
            } else {
                -1
            };
            SQL_SUCCESS
        }
        Err(msg) => {
            stmt.diagnostics.push(DiagRecord {
                state: "HY000".to_string(),
                native_error: 0,
                message: msg,
            });
            SQL_ERROR
        }
    }
}
