use super::config::DmdbConnConf;
use super::error::{DmdbReason, DmdbResult, dmdb_err};
use super::odbc_dyn::{self, ColumnOptions, DynConn};
use std::sync::{Arc, Mutex};
use tokio::task;

/// 达梦模块内可克隆的连接句柄。
/// 这里的"可克隆"仅表示句柄可在同一组件内部跨任务传递，并不表示 source 与 sink
/// 需要或会复用同一条数据库连接。
/// `DynConn` 不是异步对象，因此统一放入 `Arc<Mutex<_>>`
/// 并在阻塞线程中访问，避免直接阻塞 Tokio worker 线程。
pub(crate) type DmdbConnectionHandle = Arc<Mutex<DynConn>>;

/// 在异步上下文中建立达梦连接句柄。
pub(crate) async fn connect_shared(config: &DmdbConnConf) -> DmdbResult<DmdbConnectionHandle> {
    let config = config.clone();
    task::spawn_blocking(move || connect_shared_blocking(&config))
        .await
        .map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("spawn dmdb connect task failed: {err}"),
            )
        })?
}

/// 在阻塞上下文中建立达梦连接句柄。
pub(crate) fn connect_shared_blocking(config: &DmdbConnConf) -> DmdbResult<DmdbConnectionHandle> {
    let options = config.connect_options();
    let connection = open_odbc_conn(config, options)?;
    Ok(Arc::new(Mutex::new(connection)))
}

/// 按连接模式打开达梦 ODBC 连接。
fn open_odbc_conn(config: &DmdbConnConf, options: ColumnOptions) -> DmdbResult<DynConn> {
    // 连接优先级与工厂侧校验保持一致：完整连接串 > endpoint 拼装连接串 > DSN。
    if let Some(connection_string) = config.connection_string.as_deref().map(str::trim)
        && !connection_string.is_empty()
    {
        return odbc_dyn::open_connection(Some(connection_string), None, options.login_timeout_sec)
            .map_err(|err| {
                dmdb_err(
                    DmdbReason::Database,
                    format!("connect dmdb with connection_string fail: {err}"),
                )
            });
    }

    if !config.endpoint.trim().is_empty() {
        let connection_string = config.generated_connection_string()?;
        return odbc_dyn::open_connection(
            Some(&connection_string),
            None,
            options.login_timeout_sec,
        )
        .map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("connect dmdb with generated connection string fail: {err}"),
            )
        });
    }

    if let Some(dsn) = config.dsn.as_deref().map(str::trim)
        && !dsn.is_empty()
    {
        return odbc_dyn::open_connection(
            None,
            Some((dsn, config.username.trim(), config.password.as_str())),
            options.login_timeout_sec,
        )
        .map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("connect dmdb with dsn fail: {err}"),
            )
        });
    }

    Err(dmdb_err(
        DmdbReason::Config,
        "dmdb.connection_string, dmdb.endpoint or dmdb.dsn must provide at least one",
    ))
}

/// 转义 SQL 标识符，避免字段名或表名中的双引号破坏 SQL。
pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// 转义 SQL 字面量中的单引号。
pub(crate) fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{escape_sql_literal, quote_identifier};

    #[test]
    fn quote_identifier_escapes_double_quote() {
        assert_eq!(quote_identifier("A\"B"), "\"A\"\"B\"");
    }

    #[test]
    fn escape_sql_literal_escapes_single_quote() {
        assert_eq!(escape_sql_literal("O'Reilly"), "O''Reilly");
    }
}
