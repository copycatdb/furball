use crate::handle::*;
use crate::runtime;
use crate::types::*;
use tabby::{AuthMethod, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub fn parse_connection_string(conn_str: &str) -> (String, u16, String, String, String, bool) {
    let mut host = "localhost".to_string();
    let mut port: u16 = 1433;
    let mut database = "master".to_string();
    let mut uid = String::new();
    let mut pwd = String::new();
    let mut trust_cert = false;

    for part in conn_str.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(idx) = part.find('=') {
            let key = part[..idx].trim().to_lowercase();
            let val = part[idx + 1..].trim().to_string();
            match key.as_str() {
                "server" => {
                    if let Some(comma) = val.find(',') {
                        host = val[..comma].to_string();
                        if let Ok(p) = val[comma + 1..].trim().parse() {
                            port = p;
                        }
                    } else {
                        host = val;
                    }
                }
                "database" | "initial catalog" => database = val,
                "uid" | "user id" => uid = val,
                "pwd" | "password" => pwd = val,
                "trustservercertificate" => {
                    trust_cert = val.eq_ignore_ascii_case("yes")
                        || val == "1"
                        || val.eq_ignore_ascii_case("true")
                }
                _ => {}
            }
        }
    }
    (host, port, database, uid, pwd, trust_cert)
}

pub fn driver_connect(conn: &mut Connection, conn_str: &str) -> SQLRETURN {
    let (host, port, database, uid, pwd, trust_cert) = parse_connection_string(conn_str);
    conn.server = format!("{}:{}", host, port);
    conn.database = database.clone();
    conn.uid = uid.clone();
    conn.pwd = pwd.clone();

    let result = runtime::block_on(async {
        let mut config = Config::new();
        config.host(&host);
        config.port(port);
        config.database(&database);
        config.authentication(AuthMethod::sql_server(&uid, &pwd));
        if trust_cert {
            config.trust_cert();
        }
        config.encryption(EncryptionLevel::Required);

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| e.to_string())?;
        tcp.set_nodelay(true).map_err(|e| e.to_string())?;

        let client = tabby::Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| e.to_string())?;

        Ok::<_, String>(client)
    });

    match result {
        Ok(client) => {
            conn.client = Some(client);
            conn.connected = true;
            SQL_SUCCESS
        }
        Err(msg) => {
            conn.diagnostics.push(DiagRecord {
                state: "08001".to_string(),
                native_error: 0,
                message: msg,
            });
            SQL_ERROR
        }
    }
}

pub fn disconnect(conn: &mut Connection) -> SQLRETURN {
    conn.client = None;
    conn.connected = false;
    SQL_SUCCESS
}
