use crate::types::*;
use tabby::RowWriter;

/// Diagnostic record
pub struct DiagRecord {
    pub state: String, // 5-char SQLSTATE e.g. "HY000"
    pub native_error: i32,
    pub message: String,
}

/// Column descriptor
pub struct ColumnDesc {
    pub name: String,
    pub sql_type: SQLSMALLINT,
    pub size: SQLULEN,
    pub decimal_digits: SQLSMALLINT,
    pub nullable: SQLSMALLINT,
}

/// Environment handle
pub struct Environment {
    pub odbc_version: SQLINTEGER,
    pub connections: Vec<*mut Connection>,
}

/// Connection handle
pub struct Connection {
    pub env: *mut Environment,
    pub client: Option<tabby::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>>,
    pub server: String,
    pub database: String,
    pub uid: String,
    pub pwd: String,
    pub diagnostics: Vec<DiagRecord>,
    pub statements: Vec<*mut Statement>,
    pub connected: bool,
}

/// Statement handle  
pub struct Statement {
    pub conn: *mut Connection,
    pub columns: Vec<ColumnDesc>,
    pub rows: Vec<Vec<Option<String>>>, // all results in memory as strings
    pub row_index: isize,               // -1 = before first row
    pub diagnostics: Vec<DiagRecord>,
    pub executed: bool,
    pub prepared_sql: Option<String>,
}

// RowWriter implementation that collects everything as strings
pub struct StringRowWriter {
    pub columns: Vec<ColumnDesc>,
    pub rows: Vec<Vec<Option<String>>>,
    current_row: Vec<Option<String>>,
    got_metadata: bool,
}

impl StringRowWriter {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            current_row: Vec::new(),
            got_metadata: false,
        }
    }
}

fn sql_type_from_column(c: &tabby::Column) -> (SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLSMALLINT) {
    let type_name = format!("{:?}", c.column_type());
    let sql_type = match type_name.as_str() {
        "Int4" => SQL_INTEGER,
        "Int2" => SQL_SMALLINT,
        "Int1" => SQL_TINYINT,
        "Int8" | "Intn" => SQL_BIGINT,
        "Float8" | "Floatn" => SQL_DOUBLE,
        "Float4" => SQL_REAL,
        "Bit" | "Bitn" => SQL_BIT,
        "BigVarChar" | "NVarchar" => SQL_WVARCHAR,
        "BigChar" | "NChar" => SQL_WCHAR,
        "Text" => SQL_LONGVARCHAR,
        "NText" => SQL_WLONGVARCHAR,
        "BigBinary" => SQL_BINARY,
        "BigVarBin" => SQL_VARBINARY,
        "Image" => SQL_LONGVARBINARY,
        "Decimaln" | "Numericn" | "Money" | "Money4" => SQL_DECIMAL,
        "Datetime" | "Datetimen" | "Datetime4" | "Datetime2" => SQL_TYPE_TIMESTAMP,
        "Daten" => SQL_TYPE_DATE,
        "Timen" => SQL_TYPE_TIME,
        "Guid" => SQL_GUID,
        _ => SQL_VARCHAR,
    };
    let nullable = if c.nullable().unwrap_or(true) {
        SQL_NULLABLE
    } else {
        SQL_NO_NULLS
    };

    // Determine size
    let size: SQLULEN = match sql_type {
        SQL_INTEGER => 10,
        SQL_SMALLINT => 5,
        SQL_TINYINT => 3,
        SQL_BIGINT => 19,
        SQL_DOUBLE => 53,
        SQL_REAL => 24,
        SQL_BIT => 1,
        SQL_TYPE_TIMESTAMP => 23,
        SQL_TYPE_DATE => 10,
        SQL_TYPE_TIME => 16,
        SQL_GUID => 36,
        SQL_DECIMAL => 38,
        _ => 256,
    };

    (sql_type, size, 0, nullable)
}

impl RowWriter for StringRowWriter {
    fn on_metadata(&mut self, columns: &[tabby::Column]) {
        // Only use the first result set
        if self.got_metadata {
            return;
        }
        self.got_metadata = true;
        self.columns = columns
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
    }

    fn on_row_done(&mut self) {
        if self.got_metadata {
            let row = std::mem::replace(
                &mut self.current_row,
                Vec::with_capacity(self.columns.len()),
            );
            self.rows.push(row);
        }
    }

    fn write_null(&mut self, _col: usize) {
        self.current_row.push(None);
    }
    fn write_bool(&mut self, _col: usize, val: bool) {
        self.current_row
            .push(Some(if val { "1" } else { "0" }.to_string()));
    }
    fn write_u8(&mut self, _col: usize, val: u8) {
        self.current_row.push(Some(val.to_string()));
    }
    fn write_i16(&mut self, _col: usize, val: i16) {
        self.current_row.push(Some(val.to_string()));
    }
    fn write_i32(&mut self, _col: usize, val: i32) {
        self.current_row.push(Some(val.to_string()));
    }
    fn write_i64(&mut self, _col: usize, val: i64) {
        self.current_row.push(Some(val.to_string()));
    }
    fn write_f32(&mut self, _col: usize, val: f32) {
        self.current_row.push(Some(val.to_string()));
    }
    fn write_f64(&mut self, _col: usize, val: f64) {
        self.current_row.push(Some(val.to_string()));
    }
    fn write_str(&mut self, _col: usize, val: &str) {
        self.current_row.push(Some(val.to_string()));
    }
    fn write_bytes(&mut self, _col: usize, val: &[u8]) {
        self.current_row.push(Some(hex::encode(val)));
    }
    fn write_date(&mut self, _col: usize, days: i32) {
        // days since unix epoch
        let epoch = 719468i32; // days from 0000-03-01 to 1970-01-01
        let d = days + epoch;
        let era = if d >= 0 { d } else { d - 146096 } / 146097;
        let doe = (d - era * 146097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe as i32 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = if m <= 2 { y + 1 } else { y };
        self.current_row
            .push(Some(format!("{:04}-{:02}-{:02}", year, m, day)));
    }
    fn write_time(&mut self, _col: usize, nanos: i64) {
        let total_secs = (nanos / 1_000_000_000) as u32;
        let h = total_secs / 3600;
        let m = (total_secs % 3600) / 60;
        let s = total_secs % 60;
        let frac = (nanos % 1_000_000_000) / 1_000_000;
        self.current_row
            .push(Some(format!("{:02}:{:02}:{:02}.{:03}", h, m, s, frac)));
    }
    fn write_datetime(&mut self, _col: usize, micros: i64) {
        let total_secs = micros.div_euclid(1_000_000);
        let remaining_micros = micros.rem_euclid(1_000_000) as u32;
        let time_of_day = total_secs.rem_euclid(86400) as u32;
        let h = time_of_day / 3600;
        let mi = (time_of_day % 3600) / 60;
        let sec = time_of_day % 60;
        let millis = remaining_micros / 1000;
        let mut days = total_secs.div_euclid(86400) as i32;
        days += 719468;
        let era = if days >= 0 { days } else { days - 146096 } / 146097;
        let doe = (days - era * 146097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe as i32 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = if m <= 2 { y + 1 } else { y };
        self.current_row.push(Some(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
            year, m, d, h, mi, sec, millis
        )));
    }
    fn write_datetimeoffset(&mut self, _col: usize, micros: i64, offset_minutes: i16) {
        // Just write as datetime for now
        self.write_datetime(_col, micros);
        // Append offset
        if let Some(Some(s)) = self.current_row.last_mut() {
            let sign = if offset_minutes >= 0 { "+" } else { "-" };
            let abs = offset_minutes.unsigned_abs();
            s.push_str(&format!(" {}{:02}:{:02}", sign, abs / 60, abs % 60));
        }
    }
    fn write_decimal(&mut self, _col: usize, value: i128, _precision: u8, scale: u8) {
        let negative = value < 0;
        let abs = value.unsigned_abs();
        let s = abs.to_string();
        let scale = scale as usize;
        let result = if scale == 0 {
            s
        } else if s.len() <= scale {
            format!("0.{}{}", "0".repeat(scale - s.len()), s)
        } else {
            let (int_part, frac_part) = s.split_at(s.len() - scale);
            format!("{}.{}", int_part, frac_part)
        };
        let result = if negative {
            format!("-{}", result)
        } else {
            result
        };
        self.current_row.push(Some(result));
    }
    fn write_guid(&mut self, _col: usize, bytes: &[u8; 16]) {
        let fmt = format!(
            "{:08X}-{:04X}-{:04X}-{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            u16::from_le_bytes([bytes[4], bytes[5]]),
            u16::from_le_bytes([bytes[6], bytes[7]]),
            bytes[8],
            bytes[9],
            bytes[10],
            bytes[11],
            bytes[12],
            bytes[13],
            bytes[14],
            bytes[15]
        );
        self.current_row.push(Some(fmt));
    }
}

// hex encode helper (avoid depending on hex crate)
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
