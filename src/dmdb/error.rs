use orion_error::conversion::ToStructError;
use orion_error::{OrionError, StructError, UnifiedReason};
use serde::Serialize;

pub(crate) type DmdbError = StructError<DmdbReason>;
pub(crate) type DmdbResult<T> = Result<T, DmdbError>;

#[derive(Debug, Clone, PartialEq, Serialize, OrionError)]
pub(crate) enum DmdbReason {
    #[orion_error(identity = "conf.dmdb_config", message = "dmdb config error")]
    Config,
    #[orion_error(identity = "sys.dmdb_connection", message = "dmdb connection error")]
    Connection,
    #[orion_error(identity = "sys.dmdb_database", message = "dmdb database error")]
    Database,
    #[orion_error(identity = "sys.dmdb_transaction", message = "dmdb transaction error")]
    Transaction,
    #[orion_error(identity = "logic.dmdb_cursor", message = "dmdb cursor error")]
    Cursor,
    #[orion_error(identity = "logic.dmdb_checkpoint", message = "dmdb checkpoint error")]
    Checkpoint,
    #[orion_error(identity = "conf.dmdb_parse", message = "dmdb parse error")]
    Parse,
    #[orion_error(identity = "sys.dmdb_io", message = "dmdb io error")]
    Io,
    #[orion_error(identity = "logic.dmdb_time", message = "dmdb time error")]
    Time,
    #[orion_error(identity = "sys.dmdb_runtime", message = "dmdb runtime error")]
    Runtime,
    #[orion_error(identity = "sys.dmdb_lock", message = "dmdb lock error")]
    Lock,
    #[orion_error(transparent)]
    #[allow(dead_code)]
    Uvs(UnifiedReason),
}

pub(crate) fn dmdb_err(reason: DmdbReason, detail: impl Into<String>) -> DmdbError {
    reason.to_err().with_detail(detail)
}
