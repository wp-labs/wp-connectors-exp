//! # ODBC 动态加载模块
//!
//! ## 背景
//!
//! `wp-connectors-labs` 的 dmdb（达梦数据库）功能依赖 `odbc-api` → `odbc-sys` →
//! `libodbc`．由于 `odbc-sys` 在编译时链接 `libodbc.so`/`libodbc.dylib`，导致：
//!
//! - **编译时**：要求系统安装 ODBC 头文件和库（`unixodbc-dev`）
//! - **运行时**：二进制启动时 OS 动态加载器必须找到 `libodbc.so`，否则进程直接崩溃
//!
//! 这意味着即使程序里没有配置 dmdb connector，只要 feature `labs` 启用了，
//! 部署环境就强制要求 ODBC 运行时库．
//!
//! ## 本模块的方案
//!
//! 完全移除对 `odbc-api` / `odbc-sys` 的依赖，改用 `libloading` 在**运行时按需**
//! 加载 ODBC 共享库：
//!
//! 1. 编译时不链接任何 ODBC 符号 → 不需要 ODBC 头文件
//! 2. 启动时不加载 ODBC → 程序正常启动
//! 3. 仅在首次配置 dmdb connector 并尝试建立数据库连接时，才 `dlopen("libodbc.so")`
//! 4. 加载成功 → 缓存函数指针，正常工作
//! 5. 加载失败 → 返回错误，dmdb 功能不可用，**其余功能不受影响**
//!
//! ## 设计要点
//!
//! ### 全局单例
//!
//! ODBC 环境句柄（SQLHENV）是进程级资源，用 `OnceLock<Result<DynEnv, String>>` 实现：
//! - 首次调用 `global_env()` 时初始化，之后永远复用
//! - 初始化失败时缓存错误，后续调用直接返回错误（不需要重试）
//! - `DynEnv` 生命周期等同于进程
//!
//! ### 句柄生命周期
//!
//! 每个包装类型都实现了 `Drop` 自动释放对应的 ODBC 句柄：
//!
//! ```text
//! DynEnv (SQLHENV)      — 全局 OnceLock，进程退出时释放
//!   └─ DynConn (SQLHDBC)  — 每个连接一个，Arc<Mutex<>> 包裹，可跨线程
//!        └─ DynCursor (SQLHSTMT) — 每次 execute 产生一个，Drop 时释放
//! ```
//!
//! ### Send/Sync
//!
//! ODBC 句柄本质是 `*mut c_void`，原生不满足 `Send`/`Sync`．但：
//! - `DynEnv`：全局单例，实现 `Send + Sync`，因为仅在主线程分配且永不移动
//! - `DynConn`：实现 `Send`，通过 `Arc<Mutex<>>` 保证同时只有一个线程访问
//! - `DynCursor`：不实现 `Send`，仅在创建它的阻塞线程中使用
//! - `DynCursorRow`：借用 `DynCursor`，生命周期受限于调用栈
//!
//! ### 错误类型
//!
//! 所有函数返回 `Result<T, String>`，由上层 `common.rs` / `sink.rs` / `source.rs`
//! 统一转换为 `DmdbError`（即 `StructError<DmdbReason>`）．这样避免了在本模块
//! 引入 `orion-error` 依赖．

use libloading::{Library, Symbol};
use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::OnceLock;
use wp_log::error_ctrl;

// ===========================================================================
// ODBC C ABI 类型映射
// ===========================================================================
//
// 以下类型直接映射到 ODBC 3.x 规范的 C 类型．所有指针类型统一用
// `*mut c_void`（即 `SQLHANDLE`）表示，实际使用前通过函数签名约束．
//
// 关键映射：
//   SQLRETURN    → i16   成功=0，成功但有信息=1，无数据=100
//   SQLHANDLE    → *mut c_void  环境/连接/语句 句柄
//   SQLSMALLINT  → i16
//   SQLUSMALLINT → u16
//   SQLINTEGER   → i32
//   SQLPOINTER   → *mut c_void  通用指针参数
//   SQLLEN       → isize  (ODBC 3.x 在 64 位系统上为 64 位有符号整数)

type SqlReturn = i16;
type SqlHandle = *mut c_void;
type SqlSmallInt = i16;
type SqlUSmallInt = u16;
type SqlInteger = i32;
type SqlPointer = *mut c_void;
type SqlLen = isize;

// ===========================================================================
// ODBC 常量
// ===========================================================================
//
// 来源：ODBC 3.x 规范 (ISO/IEC 9075-3)．
// 只定义了本模块实际使用的常量．

// --- 通用返回值 ---
const SQL_SUCCESS: SqlReturn = 0; // 操作成功
const SQL_SUCCESS_WITH_INFO: SqlReturn = 1; // 成功但附带警告信息（如字符串截断）
const SQL_NO_DATA: SqlReturn = 100; // 游标无更多行
const SQL_NULL_DATA: SqlLen = -1; // 列值为 NULL（SQLGetData 的指示器）
const SQL_NTS: SqlSmallInt = -3; // 字符串以 '\0' 结尾（Null-Terminated String）

// --- 句柄类型（传给 SQLAllocHandle / SQLFreeHandle）---
const SQL_HANDLE_ENV: SqlSmallInt = 1; // 环境句柄
const SQL_HANDLE_DBC: SqlSmallInt = 2; // 连接句柄
const SQL_HANDLE_STMT: SqlSmallInt = 3; // 语句句柄

// --- 环境属性 ---
const SQL_ATTR_ODBC_VERSION: SqlInteger = 200; // 设置 ODBC 行为版本
const SQL_OV_ODBC3: SqlPointer = 3 as _; // 使用 ODBC 3.x 行为

// --- 连接属性 ---
const SQL_ATTR_AUTOCOMMIT: SqlInteger = 102; // 自动提交开关
const SQL_AUTOCOMMIT_OFF: SqlPointer = 0 as _; // 关闭自动提交（手动事务）
const SQL_AUTOCOMMIT_ON: SqlPointer = 1 as _; // 开启自动提交

// --- SQLDriverConnect 选项 ---
const SQL_DRIVER_NOPROMPT: SqlUSmallInt = 0; // 不弹出对话框，连接串直接使用

// --- SQLEndTran 完成类型 ---
const SQL_COMMIT: SqlSmallInt = 0;
const SQL_ROLLBACK: SqlSmallInt = 1;

// --- SQLFetchScroll 方向 ---
const SQL_FETCH_NEXT: SqlSmallInt = 1; // 取下一行

// --- SQLGetData 目标类型 ---
const SQL_C_CHAR: SqlSmallInt = 1; // 以 C 字符串（char*）读取
const SQL_C_BINARY: SqlSmallInt = -2; // 以二进制（BYTE*）读取

// ===========================================================================
// ODBC 函数指针类型定义
// ===========================================================================
//
// 每个类型精确对应 ODBC C API 的函数签名．命名规则：`Fn` + 函数名．
// 使用 `unsafe extern "C" fn(...)` 声明，调用时需包裹在 `unsafe { }` 中．
//
// 注意：这些是类型别名，不是函数实现．实际函数地址在运行时通过
// `libloading` 从 `libodbc.so` 中动态解析．

/// `SQLAllocHandle(HandleType, InputHandle, *mut OutputHandle) → SQLRETURN`
type FnAllocHandle = unsafe extern "C" fn(SqlSmallInt, SqlHandle, *mut SqlHandle) -> SqlReturn;

/// `SQLFreeHandle(HandleType, Handle) → SQLRETURN`
type FnFreeHandle = unsafe extern "C" fn(SqlSmallInt, SqlHandle) -> SqlReturn;

/// `SQLSetEnvAttr(EnvHandle, Attribute, ValuePtr, StringLength) → SQLRETURN`
type FnSetEnvAttr =
    unsafe extern "C" fn(SqlHandle, SqlInteger, SqlPointer, SqlInteger) -> SqlReturn;

/// `SQLDriverConnect(DbcHandle, WindowHandle, InConnStr, InLen, OutConnStr,
///                   OutBufLen, *OutLen, DriverCompletion) → SQLRETURN`
///
/// 本实现中 OutConnStr/OutBufLen/OutLen 传 NULL/0/NULL，不关心输出连接串．
type FnDriverConnect = unsafe extern "C" fn(
    SqlHandle,        // ConnectionHandle
    SqlHandle,        // WindowHandle（传 NULL）
    *mut c_char,      // InConnectionString
    SqlSmallInt,      // StringLength1（传 SQL_NTS）
    *mut c_char,      // OutConnectionString（传 NULL）
    SqlSmallInt,      // BufferLength（传 0）
    *mut SqlSmallInt, // StringLength2Ptr（传 NULL）
    SqlUSmallInt,     // DriverCompletion（传 SQL_DRIVER_NOPROMPT）
) -> SqlReturn;

/// `SQLConnect(DbcHandle, ServerName, NameLen1, UserName, NameLen2,
///             Auth, NameLen3) → SQLRETURN`
type FnConnect = unsafe extern "C" fn(
    SqlHandle,   // ConnectionHandle
    *mut c_char, // ServerName (DSN)
    SqlSmallInt, // NameLength1
    *mut c_char, // UserName
    SqlSmallInt, // NameLength2
    *mut c_char, // Authentication (password)
    SqlSmallInt, // NameLength3
) -> SqlReturn;

/// `SQLSetConnectAttr(ConnHandle, Attribute, ValuePtr, StringLength) → SQLRETURN`
type FnSetConnectAttr =
    unsafe extern "C" fn(SqlHandle, SqlInteger, SqlPointer, SqlInteger) -> SqlReturn;

/// `SQLEndTran(HandleType, Handle, CompletionType) → SQLRETURN`
type FnEndTran = unsafe extern "C" fn(SqlSmallInt, SqlHandle, SqlSmallInt) -> SqlReturn;

/// `SQLExecDirect(StmtHandle, StatementText, TextLength) → SQLRETURN`
type FnExecDirect = unsafe extern "C" fn(SqlHandle, *mut c_char, SqlInteger) -> SqlReturn;

/// `SQLNumResultCols(StmtHandle, *mut ColumnCount) → SQLRETURN`
///
/// 用于判断 SQL 执行后是否有结果集（ColumnCount > 0）．
type FnNumResultCols = unsafe extern "C" fn(SqlHandle, *mut SqlSmallInt) -> SqlReturn;

/// `SQLFetchScroll(StmtHandle, FetchOrientation, FetchOffset) → SQLRETURN`
///
/// 移动到结果集的下一行（FetchOrientation = SQL_FETCH_NEXT）．
/// 返回 SQL_NO_DATA 表示无更多行．
type FnFetchScroll = unsafe extern "C" fn(SqlHandle, SqlSmallInt, SqlLen) -> SqlReturn;

/// `SQLGetData(StmtHandle, ColNum, TargetType, TargetPtr, BufLen, *mut StrLenOrInd) → SQLRETURN`
///
/// 读取当前行的指定列数据．支持分块读取（先传 NULL buffer 探测长度，
/// 再分配 buffer 实际读取）．
type FnGetData = unsafe extern "C" fn(
    SqlHandle,    // StatementHandle
    SqlUSmallInt, // ColumnNumber（1-based）
    SqlSmallInt,  // TargetType（SQL_C_CHAR / SQL_C_BINARY）
    SqlPointer,   // TargetValuePtr（buffer 指针）
    SqlLen,       // BufferLength
    *mut SqlLen,  // StrLen_or_IndPtr（输出：实际长度或 SQL_NULL_DATA）
) -> SqlReturn;

/// `SQLFreeStmt(StmtHandle, Option) → SQLRETURN`
///
/// 预留．当前使用 `SQLFreeHandle(SQL_HANDLE_STMT)` 替代．
#[allow(dead_code)]
type FnFreeStmt = unsafe extern "C" fn(SqlHandle, SqlUSmallInt) -> SqlReturn;

/// `SQLGetDiagRec(HandleType, Handle, RecNumber, Sqlstate, NativeError,
///                MessageText, BufferLength, TextLengthPtr) → SQLRETURN`
///
/// 获取 ODBC 诊断记录（错误详情），用于在 API 调用失败时取得原始错误信息．
type FnGetDiagRec = unsafe extern "C" fn(
    SqlSmallInt,      // HandleType
    SqlHandle,        // Handle
    SqlSmallInt,      // RecNumber
    *mut c_char,      // Sqlstate (output, 5 chars + null)
    *mut SqlInteger,  // NativeErrorPtr
    *mut c_char,      // MessageText
    SqlSmallInt,      // BufferLength
    *mut SqlSmallInt, // TextLengthPtr
) -> SqlReturn;

/// `SQLColumns(StmtHandle, CatalogName, NameLen1, SchemaName, NameLen2,
///             TableName, NameLen3, ColumnName, NameLen4) → SQLRETURN`
///
/// 返回指定表的列元数据结果集．本实现一次性收集所有行到 `Vec<ColumnMeta>`．
type FnColumns = unsafe extern "C" fn(
    SqlHandle,   // StatementHandle
    *mut c_char, // CatalogName
    SqlSmallInt, // NameLength1
    *mut c_char, // SchemaName
    SqlSmallInt, // NameLength2
    *mut c_char, // TableName
    SqlSmallInt, // NameLength3
    *mut c_char, // ColumnName
    SqlSmallInt, // NameLength4
) -> SqlReturn;

// ===========================================================================
// 动态加载的 ODBC 函数库
// ===========================================================================

/// 从 `libodbc.so` 动态解析的函数指针集合．
///
/// `#[allow(dead_code)]` 因为 `sql_free_stmt` 预留但未使用．
#[allow(dead_code)]
struct OdbcFuncs {
    sql_alloc_handle: FnAllocHandle,
    sql_free_handle: FnFreeHandle,
    sql_set_env_attr: FnSetEnvAttr,
    sql_driver_connect: FnDriverConnect,
    sql_connect: FnConnect,
    sql_set_connect_attr: FnSetConnectAttr,
    sql_end_tran: FnEndTran,
    sql_exec_direct: FnExecDirect,
    sql_num_result_cols: FnNumResultCols,
    sql_fetch_scroll: FnFetchScroll,
    sql_get_data: FnGetData,
    sql_free_stmt: FnFreeStmt,
    sql_columns: FnColumns,
    sql_get_diag_rec: FnGetDiagRec,
}

/// ODBC 动态库的完整实例：持有 dlopen 句柄 + 解析后的函数表．
///
/// `_lib` 字段必须保留——`Library` 的 `Drop` 实现会 `dlclose`．
/// 一旦 `_lib` 被释放，所有函数指针将变成悬垂指针．
struct OdbcLib {
    _lib: Library,
    funcs: OdbcFuncs,
}

// SAFETY: ODBC 驱动管理器（unixODBC / iODBC / Windows ODBC）在内部使用
// 互斥锁保护全局状态，因此跨线程调用是安全的．`OdbcLib` 仅包含函数指针
// 和 dlopen 句柄，移动或跨线程共享均无数据竞争风险．
unsafe impl Send for OdbcLib {}
unsafe impl Sync for OdbcLib {}

/// 全局 ODBC 实例，使用 `OnceLock` 保证整个进程生命周期只加载一次．
///
/// - `Ok(OdbcLib)` → ODBC 库加载成功，函数指针可用
/// - `Err(String)`  → 加载失败（库不存在或符号缺失），后续调用直接返回错误
static ODBC: OnceLock<Result<OdbcLib, String>> = OnceLock::new();

/// 按需加载 ODBC 动态库并解析函数指针，返回静态引用．
///
/// ## 加载流程
///
/// 1. 根据 `target_os` 选择库文件名：Linux → `libodbc.so`，macOS → `libodbc.dylib`，
///    Windows → `odbc32.dll`
/// 2. 调用 `libloading::Library::new()` 执行 `dlopen`
/// 3. 逐个调用 `lib.get()` 解析 13 个 ODBC 函数符号
/// 4. 将所有函数指针存入 `OdbcFuncs`，包裹在 `OdbcLib` 中返回
///
/// ## 关于 `transmute`
///
/// `libloading` 返回的是泛型 `Symbol<T>`，但我们需要存储不同类型的函数指针．
/// 做法是：
/// 1. 统一以 `Symbol<unsafe extern "C" fn()>` 类型获取符号
/// 2. `into_raw()` 得到 `*mut unsafe extern "C" fn()`（指向函数指针的指针）
/// 3. `.into_raw()` 得到最终的函数地址 `*mut c_void`
/// 4. `transmute` 将其转换为目标函数指针类型
///
/// 这是安全的，因为 `dlsym` 返回的地址确实是目标函数的入口点，
/// 我们只是将其转换为正确的调用签名．
fn load_odbc() -> Result<&'static OdbcLib, &'static str> {
    ODBC.get_or_init(|| {
        // 1. dlopen — 按平台选择库名
        let lib = unsafe {
            let name = if cfg!(target_os = "windows") {
                "odbc32.dll"
            } else if cfg!(target_os = "macos") {
                "libodbc.dylib"
            } else {
                "libodbc.so" // Linux / FreeBSD 等
            };
            Library::new(name).map_err(|e| format!("ODBC library not found ({name}): {e}"))
        }?;

        // 2. 逐个解析函数符号
        macro_rules! load_fn {
            ($lib:expr, $name:expr) => {{
                unsafe {
                    let sym: Symbol<unsafe extern "C" fn()> = $lib
                        .get($name.as_bytes())
                        .map_err(|e| format!("symbol {} not found: {e}", $name))?;
                    // 见上文"关于 transmute"的说明
                    #[allow(clippy::missing_transmute_annotations)]
                    {
                        std::mem::transmute(sym.into_raw().into_raw())
                    }
                }
            }};
        }

        let funcs = OdbcFuncs {
            sql_alloc_handle: load_fn!(lib, "SQLAllocHandle"),
            sql_free_handle: load_fn!(lib, "SQLFreeHandle"),
            sql_set_env_attr: load_fn!(lib, "SQLSetEnvAttr"),
            sql_driver_connect: load_fn!(lib, "SQLDriverConnect"),
            sql_connect: load_fn!(lib, "SQLConnect"),
            sql_set_connect_attr: load_fn!(lib, "SQLSetConnectAttr"),
            sql_end_tran: load_fn!(lib, "SQLEndTran"),
            sql_exec_direct: load_fn!(lib, "SQLExecDirect"),
            sql_num_result_cols: load_fn!(lib, "SQLNumResultCols"),
            sql_fetch_scroll: load_fn!(lib, "SQLFetchScroll"),
            sql_get_data: load_fn!(lib, "SQLGetData"),
            sql_free_stmt: load_fn!(lib, "SQLFreeStmt"),
            sql_columns: load_fn!(lib, "SQLColumns"),
            sql_get_diag_rec: load_fn!(lib, "SQLGetDiagRec"),
        };

        Ok(OdbcLib { _lib: lib, funcs })
    })
    .as_ref()
    .map_err(|e| e.as_str())
}

/// 获取已加载的 ODBC 函数表（内部便捷方法）．
///
/// 调用者不需要关心加载细节——如果 `load_odbc()` 失败，
/// 这里将其转为 `String` 错误，符合本模块统一的错误类型．
fn funcs() -> Result<&'static OdbcFuncs, String> {
    load_odbc().map(|lib| &lib.funcs).map_err(|e| e.to_string())
}

/// 构造错误信息的内部辅助函数．
#[inline]
fn odbc_err(msg: impl Into<String>) -> String {
    msg.into()
}

/// 从 ODBC 驱动获取诊断记录（原始错误日志）．
///
/// 在 ODBC API 调用返回失败时，调用 `SQLGetDiagRec` 获取第一条诊断记录．
/// 包括 SQLSTATE（5 字符错误码）、原生错误码和错误消息文本．
///
/// 如果 handle 为 NULL 或获取失败，返回空字符串．
fn odbc_diag(handle_type: SqlSmallInt, handle: SqlHandle) -> String {
    if handle.is_null() {
        return String::new();
    }
    let f = match funcs() {
        Ok(f) => f,
        Err(_) => return String::new(),
    };

    let mut state = [0u8; 6];
    let mut native: SqlInteger = 0;
    let mut msg_buf = vec![0u8; 512];
    let mut msg_len: SqlSmallInt = 0;

    let ret = unsafe {
        (f.sql_get_diag_rec)(
            handle_type,
            handle,
            1, // RecNumber — 第一条诊断记录
            state.as_mut_ptr() as *mut c_char,
            &mut native,
            msg_buf.as_mut_ptr() as *mut c_char,
            (msg_buf.len() - 1) as SqlSmallInt,
            &mut msg_len,
        )
    };

    if ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO {
        let s = std::str::from_utf8(&state[..5]).unwrap_or("?????");
        let m = std::str::from_utf8(&msg_buf[..msg_len as usize]).unwrap_or("");
        let diag = format!(" | [{s}] {m} (native={native})");
        error_ctrl!("ODBC error{}", diag);
        diag
    } else {
        String::new()
    }
}

// ===========================================================================
// SQL 数据类型常量
// ===========================================================================
//
// ODBC 为每种 SQL 类型定义了整数代码．`SQLColumns` 返回的 DATA_TYPE 列
// 就是这些值．我们用 `DmdbDataType::from_odbc_meta()` 将其映射为 Rust 枚举．

const SQL_CHAR: i16 = 1; // 定长字符串
const SQL_NUMERIC: i16 = 2; // 精确数值
const SQL_DECIMAL: i16 = 3; // 精确数值
const SQL_INTEGER: i16 = 4; // 32 位整数
const SQL_SMALLINT: i16 = 5; // 16 位整数
const SQL_FLOAT: i16 = 6; // 浮点数
const SQL_REAL: i16 = 7; // 单精度浮点
const SQL_DOUBLE: i16 = 8; // 双精度浮点
const SQL_VARCHAR: i16 = 12; // 变长字符串
const SQL_TYPE_DATE: i16 = 91; // DATE 类型
const SQL_TYPE_TIME: i16 = 92; // TIME 类型
const SQL_TYPE_TIMESTAMP: i16 = 93; // TIMESTAMP 类型
const SQL_BIGINT: i16 = -5; // 64 位整数
const SQL_TINYINT: i16 = -6; // 8 位整数
const SQL_BINARY: i16 = -2; // 定长二进制
const SQL_VARBINARY: i16 = -3; // 变长二进制
const SQL_LONGVARBINARY: i16 = -4; // 长二进制
const SQL_LONGVARCHAR: i16 = -1; // 长文本
const SQL_WVARCHAR: i16 = -9; // Unicode 变长字符串
const SQL_WLONGVARCHAR: i16 = -10; // Unicode 长文本

// ===========================================================================
// 自定义数据类型枚举（替代 odbc_api::DataType）
// ===========================================================================

/// 从 ODBC 元数据推导出的列数据类型．
///
/// 仅包含 dmdb 模块实际使用的变体．通过 `from_odbc_meta()` 从
/// `SQLColumns` 的 raw 数据构造．
///
/// 注意：与原 `odbc_api::DataType` 不同，本枚举**不**包含 `Unknown` 变体；
/// 无法识别的类型统一归入 `Other`．
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DmdbDataType {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    /// 精确数值类型，precision=总位数，scale=小数位数
    Numeric {
        precision: u16,
        scale: i16,
    },
    /// 精确数值类型，precision=总位数，scale=小数位数
    Decimal {
        precision: u16,
        scale: i16,
    },
    /// 浮点类型，precision=二进制精度位数
    Float {
        precision: u16,
    },
    Real,
    Double,
    Date,
    /// 时间类型，precision=秒的小数位数（通常 0-6）
    Time {
        precision: u16,
    },
    /// 时间戳类型，precision=秒的小数位数
    Timestamp {
        precision: u16,
    },
    /// 可变长字符串，length=None 表示未指定长度
    Varchar {
        length: Option<usize>,
    },
    Binary {
        length: usize,
    },
    Varbinary {
        length: usize,
    },
    LongVarbinary {
        length: usize,
    },
    /// 无法识别的类型，保守处理
    Other,
}

impl DmdbDataType {
    /// 从 ODBC 元数据接口返回的原始值构造数据类型．
    ///
    /// 参数：
    /// - `sql_type`: `SQLColumns` 的 DATA_TYPE 列（i16）
    /// - `column_size`: `SQLColumns` 的 COLUMN_SIZE 列
    /// - `decimal_digits`: `SQLColumns` 的 DECIMAL_DIGITS 列
    ///
    /// 精度和 scale 直接从 COLUMN_SIZE / DECIMAL_DIGITS 提取，
    /// 不做额外校验．
    pub(crate) fn from_odbc_meta(sql_type: i16, column_size: usize, decimal_digits: i16) -> Self {
        match sql_type {
            SQL_TINYINT => Self::TinyInt,
            SQL_SMALLINT => Self::SmallInt,
            SQL_INTEGER => Self::Integer,
            SQL_BIGINT => Self::BigInt,
            SQL_NUMERIC => Self::Numeric {
                precision: column_size as u16,
                scale: decimal_digits,
            },
            SQL_DECIMAL => Self::Decimal {
                precision: column_size as u16,
                scale: decimal_digits,
            },
            SQL_FLOAT => Self::Float {
                precision: column_size as u16,
            },
            SQL_REAL => Self::Real,
            SQL_DOUBLE => Self::Double,
            SQL_TYPE_DATE => Self::Date,
            SQL_TYPE_TIME => Self::Time {
                precision: decimal_digits as u16,
            },
            SQL_TYPE_TIMESTAMP => Self::Timestamp {
                precision: decimal_digits as u16,
            },
            // 所有字符串类型统一映射为 Varchar
            SQL_VARCHAR | SQL_CHAR | SQL_LONGVARCHAR | SQL_WVARCHAR | SQL_WLONGVARCHAR => {
                Self::Varchar {
                    length: if column_size > 0 {
                        Some(column_size)
                    } else {
                        None
                    },
                }
            }
            SQL_BINARY => Self::Binary {
                length: column_size,
            },
            SQL_VARBINARY => Self::Varbinary {
                length: column_size,
            },
            SQL_LONGVARBINARY => Self::LongVarbinary {
                length: column_size,
            },
            _ => Self::Other,
        }
    }
}

// ===========================================================================
// 连接选项（替代 odbc_api::ConnectionOptions）
// ===========================================================================

/// 建连时的可选参数．
///
/// 当前仅支持登录超时．原 `odbc_api::ConnectionOptions` 还有 `packet_size`，
/// 但 dmdb 从未使用，故省略．
#[derive(Debug, Clone, Copy)]
pub(crate) struct ColumnOptions {
    /// 登录超时秒数，None 表示使用驱动默认值
    pub(crate) login_timeout_sec: Option<u32>,
}

// ===========================================================================
// 列元数据（替代 odbc_api columns 迭代器的行类型）
// ===========================================================================

/// 从 `SQLColumns` 结果集提取的单列元数据．
///
/// 字段顺序对应 ODBC `SQLColumns` 结果集的列号：
/// | 列号 | 字段            | 说明                        |
/// |------|-----------------|-----------------------------|
/// | 3    | `table_name`    | 表名                        |
/// | 2    | `schema_name`   | schema 名（可为 NULL）       |
/// | 4    | `column_name`   | 列名                        |
/// | 6    | `type_name`     | 数据库类型名（如 "VARCHAR"） |
/// | 5    | `sql_data_type` | SQL 数据类型代码            |
/// | 7    | `column_size`   | 列大小                      |
/// | 9    | `decimal_digits`| 小数位数                    |
/// | 17   | `ordinal_position` | 列在表中的顺序位置        |
#[derive(Debug, Clone)]
pub(crate) struct ColumnMeta {
    pub(crate) table_name: String,
    pub(crate) schema_name: Option<String>,
    pub(crate) column_name: String,
    pub(crate) type_name: String,
    pub(crate) sql_data_type: i16,
    pub(crate) column_size: i32,
    pub(crate) decimal_digits: i16,
    pub(crate) ordinal_position: i32,
}

// ===========================================================================
// ODBC 环境句柄 (SQLHENV)
// ===========================================================================

/// ODBC 环境句柄的 RAII 包装．
///
/// 进程内只有一个实例（由 `GLOBAL_ENV` OnceLock 持有）．
/// 所有连接都从这个环境中分配．
pub(crate) struct DynEnv {
    henv: SqlHandle, // SQLHENV raw pointer
}

// SAFETY: DynEnv 仅在主线程（或首次调用 global_env 的线程）创建，
// 之后不再移动，且在 `Arc` 或静态中共享时仅读取其 handle．
// ODBC 驱动管理器内部使用互斥锁保护全局状态．
unsafe impl Send for DynEnv {}
unsafe impl Sync for DynEnv {}

impl DynEnv {
    /// 分配 ODBC 环境句柄并设置为 3.x 行为．
    ///
    /// ODBC 2.x 和 3.x 在 SQLLEN 大小、SQLFetch 行为等方面有差异．
    /// 我们明确设置为 ODBC 3.x 以避免兼容性问题．
    ///
    /// 失败场景：
    /// - ODBC 共享库未安装（libodbc.so 不存在）
    /// - 驱动管理器初始化失败（极少见）
    pub(crate) fn new() -> Result<Self, String> {
        let f = funcs()?;

        // Step 1: 分配环境句柄
        let mut henv: SqlHandle = ptr::null_mut();
        let ret = unsafe { (f.sql_alloc_handle)(SQL_HANDLE_ENV, ptr::null_mut(), &mut henv) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLAllocHandle(ENV) failed: {ret}{}",
                odbc_diag(SQL_HANDLE_ENV, henv)
            )));
        }

        // Step 2: 设置 ODBC 版本为 3.x
        let ret = unsafe { (f.sql_set_env_attr)(henv, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3, 0) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            // 版本设置失败时需要清理已分配的环境句柄
            unsafe { (f.sql_free_handle)(SQL_HANDLE_ENV, henv) };
            return Err(odbc_err(format!(
                "SQLSetEnvAttr(ODBC3) failed: {ret}{}",
                odbc_diag(SQL_HANDLE_ENV, henv)
            )));
        }

        Ok(Self { henv })
    }

    /// 获取原始 SQLHENV 指针，供 `SQLAllocHandle(SQL_HANDLE_DBC)` 使用．
    fn raw(&self) -> SqlHandle {
        self.henv
    }
}

impl Drop for DynEnv {
    fn drop(&mut self) {
        if !self.henv.is_null() {
            // Drop 时 ODBC 库可能已被卸载（进程退出时静态析构顺序不确定）
            // 因此仅尝试释放——如果 funcs() 返回 Err 则跳过
            if let Ok(f) = funcs() {
                unsafe { (f.sql_free_handle)(SQL_HANDLE_ENV, self.henv) };
            }
        }
    }
}

// ===========================================================================
// ODBC 连接句柄 (SQLHDBC)
// ===========================================================================

/// ODBC 连接句柄的 RAII 包装．
///
/// 每个 dmdb Source 或 Sink 持有一个连接，包装在
/// `Arc<Mutex<DynConn>>` 中以便跨 `tokio::spawn_blocking` 传递．
///
/// `DynConn` 实现 `Send`，确保可以移动到阻塞线程中执行 ODBC 调用．
pub struct DynConn {
    hdbc: SqlHandle, // SQLHDBC raw pointer
}

// SAFETY: ODBC 连接句柄可以从一个线程移动到另一个线程（SQLHDBC 本身是
// 指向驱动管理器内部结构的指针，驱动管理器负责线程安全）．
// 但不能同时从两个线程访问同一个句柄——这由上层 `Arc<Mutex<>>` 保证．
unsafe impl Send for DynConn {}

impl DynConn {
    /// 分配一个新的连接句柄（不建立连接）．
    fn alloc(env: &DynEnv) -> Result<Self, String> {
        let f = funcs()?;
        let mut hdbc: SqlHandle = ptr::null_mut();
        let ret = unsafe { (f.sql_alloc_handle)(SQL_HANDLE_DBC, env.raw(), &mut hdbc) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLAllocHandle(DBC) failed: {ret}{}",
                odbc_diag(SQL_HANDLE_DBC, hdbc)
            )));
        }
        Ok(Self { hdbc })
    }

    /// 设置登录超时秒数（建连前调用）．
    ///
    /// 注意：超时值以秒为单位传给驱动，但实际行为取决于驱动实现．
    fn set_login_timeout(&self, timeout_secs: Option<u32>) -> Result<(), String> {
        let f = funcs()?;
        if let Some(secs) = timeout_secs {
            let ret = unsafe {
                (f.sql_set_connect_attr)(
                    self.hdbc,
                    103, // SQL_ATTR_LOGIN_TIMEOUT（标准 ODBC 属性号）
                    secs as SqlPointer,
                    0,
                )
            };
            if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
                return Err(odbc_err(format!(
                    "SQLSetConnectAttr(LOGIN_TIMEOUT) failed: {ret}{}",
                    odbc_diag(SQL_HANDLE_DBC, self.hdbc)
                )));
            }
        }
        Ok(())
    }

    /// 通过连接字符串建立连接（`SQLDriverConnect`）．
    ///
    /// 连接字符串格式：`Driver={DM8 ODBC DRIVER};SERVER=host;TCP_PORT=5236;UID=user;PWD=pass;`
    /// 由 `config.rs::DmdbConnConf::generated_connection_string()` 生成．
    fn driver_connect(&self, conn_str: &str) -> Result<(), String> {
        let f = funcs()?;
        let cstr = to_cstring(conn_str);
        let ret = unsafe {
            (f.sql_driver_connect)(
                self.hdbc,
                ptr::null_mut(),              // WindowHandle
                cstr.as_ptr() as *mut c_char, // InConnectionString
                SQL_NTS,                      // 字符串以 '\0' 结尾
                ptr::null_mut(),              // OutConnectionString（不需要）
                0,                            // BufferLength
                ptr::null_mut(),              // StringLength2Ptr
                SQL_DRIVER_NOPROMPT,          // 不弹对话框
            )
        };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLDriverConnect failed: {ret}{}",
                odbc_diag(SQL_HANDLE_DBC, self.hdbc)
            )));
        }
        Ok(())
    }

    /// 通过 DSN 建立连接（`SQLConnect`）．
    ///
    /// DSN（Data Source Name）需在 `odbc.ini` 中预配置．
    fn connect(&self, dsn: &str, username: &str, password: &str) -> Result<(), String> {
        let f = funcs()?;
        let dsn_c = to_cstring(dsn);
        let user_c = to_cstring(username);
        let pwd_c = to_cstring(password);
        let ret = unsafe {
            (f.sql_connect)(
                self.hdbc,
                dsn_c.as_ptr() as *mut c_char,
                SQL_NTS,
                user_c.as_ptr() as *mut c_char,
                SQL_NTS,
                pwd_c.as_ptr() as *mut c_char,
                SQL_NTS,
            )
        };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLConnect failed: {ret}{}",
                odbc_diag(SQL_HANDLE_DBC, self.hdbc)
            )));
        }
        Ok(())
    }

    /// 开启或关闭自动提交．
    ///
    /// Sink 在执行多行写入前关闭自动提交，写入完成后提交并恢复自动提交，
    /// 以此实现批量事务语义．
    pub(crate) fn set_autocommit(&self, on: bool) -> Result<(), String> {
        let f = funcs()?;
        let value = if on {
            SQL_AUTOCOMMIT_ON
        } else {
            SQL_AUTOCOMMIT_OFF
        };
        let ret = unsafe { (f.sql_set_connect_attr)(self.hdbc, SQL_ATTR_AUTOCOMMIT, value, 0) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLSetConnectAttr(AUTOCOMMIT) failed: {ret}{}",
                odbc_diag(SQL_HANDLE_DBC, self.hdbc)
            )));
        }
        Ok(())
    }

    /// 提交当前事务．
    pub(crate) fn commit(&self) -> Result<(), String> {
        let f = funcs()?;
        let ret = unsafe { (f.sql_end_tran)(SQL_HANDLE_DBC, self.hdbc as SqlHandle, SQL_COMMIT) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLEndTran(COMMIT) failed: {ret}{}",
                odbc_diag(SQL_HANDLE_DBC, self.hdbc as SqlHandle)
            )));
        }
        Ok(())
    }

    /// 回滚当前事务．
    pub(crate) fn rollback(&self) -> Result<(), String> {
        let f = funcs()?;
        let ret = unsafe { (f.sql_end_tran)(SQL_HANDLE_DBC, self.hdbc as SqlHandle, SQL_ROLLBACK) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLEndTran(ROLLBACK) failed: {ret}{}",
                odbc_diag(SQL_HANDLE_DBC, self.hdbc as SqlHandle)
            )));
        }
        Ok(())
    }

    /// 执行 SQL 语句并返回游标．
    ///
    /// 返回 `Some(DynCursor)` 表示有结果集（SELECT / 元数据查询）．
    /// 返回 `None` 表示无结果集（INSERT / UPDATE / DELETE 等 DML）．
    ///
    /// ## query_timeout_secs
    ///
    /// 当前**未实现**，预留参数．ODBC 的 `SQL_ATTR_QUERY_TIMEOUT` 需要在
    /// 分配 STMT 之后、执行之前设置．TODO: 在 `DynCursor::alloc_and_execute`
    /// 中实现．
    pub(crate) fn execute(
        &self,
        sql: &str,
        _query_timeout_secs: Option<usize>,
    ) -> Result<Option<DynCursor>, String> {
        let f = funcs()?;

        // Step 1: 分配语句句柄
        let mut hstmt: SqlHandle = ptr::null_mut();
        let ret =
            unsafe { (f.sql_alloc_handle)(SQL_HANDLE_STMT, self.hdbc as SqlHandle, &mut hstmt) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLAllocHandle(STMT) failed: {ret}{}",
                odbc_diag(SQL_HANDLE_STMT, hstmt)
            )));
        }

        // Step 2: 执行 SQL
        let sql_c = to_cstring(sql);
        let ret = unsafe {
            (f.sql_exec_direct)(hstmt, sql_c.as_ptr() as *mut c_char, SQL_NTS as SqlInteger)
        };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            // 执行失败 → 先取诊断信息，再清理语句句柄
            let diag = odbc_diag(SQL_HANDLE_STMT, hstmt);
            unsafe { (f.sql_free_handle)(SQL_HANDLE_STMT, hstmt) };
            return Err(odbc_err(format!(
                "SQLExecDirect failed: {ret}, sql: {sql}{diag}"
            )));
        }

        // Step 3: 检查是否有结果集（例如 SELECT 有结果集，INSERT 没有）
        let mut ncols: SqlSmallInt = 0;
        let ret = unsafe { (f.sql_num_result_cols)(hstmt, &mut ncols) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            unsafe { (f.sql_free_handle)(SQL_HANDLE_STMT, hstmt) };
            return Ok(None);
        }

        Ok(Some(DynCursor { hstmt }))
    }

    /// 获取表列元数据（封装 `SQLColumns` ODBC API）．
    ///
    /// 返回指定表的全部列信息，按 `ordinal_position` 排序．
    /// 调用方（`source.rs`）负责按 `table_name` 和 `schema_name` 过滤．
    pub(crate) fn columns(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        column_pattern: &str,
    ) -> Result<Vec<ColumnMeta>, String> {
        let f = funcs()?;

        // 分配语句句柄
        let mut hstmt: SqlHandle = ptr::null_mut();
        let ret =
            unsafe { (f.sql_alloc_handle)(SQL_HANDLE_STMT, self.hdbc as SqlHandle, &mut hstmt) };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            return Err(odbc_err(format!(
                "SQLAllocHandle(STMT) failed: {ret}{}",
                odbc_diag(SQL_HANDLE_STMT, hstmt)
            )));
        }

        // 调用 SQLColumns（空字符串表示"不限制"）
        let cat_c = to_cstring_or_null(catalog);
        let sch_c = to_cstring_or_null(schema);
        let tbl_c = to_cstring_or_null(table);
        let col_c = to_cstring_or_null(column_pattern);

        let ret = unsafe {
            (f.sql_columns)(
                hstmt,
                cat_c.as_ptr() as *mut c_char,
                SQL_NTS, // CatalogName
                sch_c.as_ptr() as *mut c_char,
                SQL_NTS, // SchemaName
                tbl_c.as_ptr() as *mut c_char,
                SQL_NTS, // TableName
                col_c.as_ptr() as *mut c_char,
                SQL_NTS, // ColumnName
            )
        };
        if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
            let diag = odbc_diag(SQL_HANDLE_STMT, hstmt);
            unsafe { (f.sql_free_handle)(SQL_HANDLE_STMT, hstmt) };
            return Err(odbc_err(format!("SQLColumns failed: {ret}{diag}")));
        }

        // 遍历结果集，逐行收集列元数据
        let mut rows = Vec::new();
        loop {
            let ret = unsafe { (f.sql_fetch_scroll)(hstmt, SQL_FETCH_NEXT, 0) };
            if ret == SQL_NO_DATA {
                break; // 全部读取完毕
            }
            if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
                let diag = odbc_diag(SQL_HANDLE_STMT, hstmt);
                unsafe { (f.sql_free_handle)(SQL_HANDLE_STMT, hstmt) };
                return Err(odbc_err(format!(
                    "SQLFetchScroll(columns) failed: {ret}{diag}"
                )));
            }

            // 按 SQLColumns 结果集的固定列号读取
            let table_name = read_string_col(hstmt, 3)?; // 表名
            let schema_name = read_optional_string_col(hstmt, 2)?; // schema 名（可为空）
            let column_name = read_string_col(hstmt, 4)?; // 列名
            let type_name = read_string_col(hstmt, 6)?; // 类型名
            let sql_data_type = read_i16_col(hstmt, 5)?; // SQL 类型代码
            let column_size = read_i32_col(hstmt, 7)?.unwrap_or(0); // 列大小
            let decimal_digits = read_i16_col(hstmt, 9)?.unwrap_or(0); // 小数位数
            let ordinal_position = read_i32_col(hstmt, 17)?.unwrap_or(0); // 序数位置

            rows.push(ColumnMeta {
                table_name,
                schema_name,
                column_name,
                type_name,
                sql_data_type: sql_data_type.unwrap_or(0),
                column_size,
                decimal_digits,
                ordinal_position,
            });
        }

        unsafe { (f.sql_free_handle)(SQL_HANDLE_STMT, hstmt) };
        Ok(rows)
    }

    /// 获取原始 SQLHDBC 指针（预留，暂时未用）．
    #[allow(dead_code)]
    pub(crate) fn raw(&self) -> SqlHandle {
        self.hdbc
    }
}

impl Drop for DynConn {
    fn drop(&mut self) {
        if !self.hdbc.is_null()
            && let Ok(f) = funcs()
        {
            unsafe {
                // 连接关闭前先回滚任何未提交的事务（防御性操作）
                (f.sql_end_tran)(SQL_HANDLE_DBC, self.hdbc as SqlHandle, SQL_ROLLBACK);
                (f.sql_free_handle)(SQL_HANDLE_DBC, self.hdbc as SqlHandle);
            };
        }
    }
}

// ===========================================================================
// 游标——语句句柄 (SQLHSTMT)
// ===========================================================================

/// 执行 SQL 后产生的结果集游标．
///
/// 每次 `DynConn::execute()` 创建一个新的 `DynCursor`．
/// `DynCursor` 不实现 `Send`——ODBC 游标操作（`SQLFetchScroll`、`SQLGetData`）
/// 必须在创建它的同一线程中执行，不能移交到其他线程．
pub(crate) struct DynCursor {
    hstmt: SqlHandle, // SQLHSTMT raw pointer
}

impl DynCursor {
    /// 移动到结果集的下一行，返回对该行的借用引用．
    ///
    /// 返回 `None` 表示结果集已耗尽（SQL_NO_DATA）．
    ///
    /// ## 为什么 `DynCursorRow` 借用 `&self`？
    ///
    /// ODBC 游标是**单一当前行**模型：任何时候只有一个当前行．
    /// `SQLFetchScroll` 将游标推进到下一行，之后通过 `SQLGetData` 读取该行的列数据．
    /// `DynCursorRow` 借用 `DynCursor`，确保：
    /// 1. 不能在持有行引用时再次 `next_row()`（借用检查阻止）
    /// 2. 行引用离开作用域后可以继续 `next_row()`
    pub(crate) fn next_row(&mut self) -> Result<Option<DynCursorRow<'_>>, String> {
        let f = funcs()?;
        let ret = unsafe { (f.sql_fetch_scroll)(self.hstmt, SQL_FETCH_NEXT, 0) };
        match ret {
            SQL_SUCCESS | SQL_SUCCESS_WITH_INFO => Ok(Some(DynCursorRow { cursor: self })),
            SQL_NO_DATA => Ok(None),
            _ => Err(odbc_err(format!("SQLFetchScroll failed: {ret}"))),
        }
    }
}

impl Drop for DynCursor {
    fn drop(&mut self) {
        if !self.hstmt.is_null()
            && let Ok(f) = funcs()
        {
            unsafe { (f.sql_free_handle)(SQL_HANDLE_STMT, self.hstmt) };
        }
    }
}

// ===========================================================================
// 游标行——对游标的借用视图
// ===========================================================================

/// 对当前游标行的只读借用，通过 `DynCursor::next_row()` 获取．
///
/// 持有此引用时不能调用 `next_row()`（Rust 借用检查保证）．
/// 离开作用域后可以继续迭代游标．
///
/// 提供 `get_text()` 和 `get_binary()` 两个数据读取方法，
/// 分别对应 ODBC 的 `SQLGetData(..., SQL_C_CHAR)` 和 `SQLGetData(..., SQL_C_BINARY)`．
pub(crate) struct DynCursorRow<'a> {
    cursor: &'a DynCursor,
}

impl DynCursorRow<'_> {
    /// 获取底层语句句柄（内部使用）．
    fn stmt(&self) -> SqlHandle {
        self.cursor.hstmt
    }

    /// 以 UTF-8 文本方式读取第 `col` 列（1-based）．
    ///
    /// 返回值：
    /// - `Ok(true)`  → 列有值，数据已追加到 `buf`
    /// - `Ok(false)` → 列值为 SQL NULL
    /// - `Err(e)`    → 读取失败
    ///
    /// 数据**追加**到 `buf` 末尾（不清除已有内容），调用方负责在读取新行前
    /// 调用 `buf.clear()`．这允许复用同一缓冲区。
    pub(crate) fn get_text(&self, col: u16, buf: &mut Vec<u8>) -> Result<bool, String> {
        self.read_column(col, SQL_C_CHAR, buf)
    }

    pub(crate) fn get_binary(&self, col: u16, buf: &mut Vec<u8>) -> Result<bool, String> {
        self.read_column(col, SQL_C_BINARY, buf)
    }

    /// ODBC `SQLGetData` 的通用封装，支持分块读取．
    ///
    /// ## 读取策略
    ///
    /// ODBC 的长数据（如 CLOB、BLOB）可能需要多次 `SQLGetData` 调用才能完整读取．
    ///
    /// 注意：不使用 NULL buffer 探测长度，因为部分 ODBC Driver Manager
    /// （如 unixODBC）会拒绝 `TargetValuePtr = NULL` 并返回 HY009，
    /// 即便 `BufferLength = 0` 在规范中是合法的．
    ///
    /// 改为直接从较小的 buffer 开始读取，如果数据被截断则循环追加后续分块，
    /// 直到 `SQL_SUCCESS`．
    ///
    /// ## 关于 `unsafe`
    ///
    /// `buf.set_len()` 是不安全的，因为我们直接写入 Vec 的未初始化空间．
    /// 这是必要的——ODBC 驱动需要直接写入我们提供的内存．我们用以下保证：
    /// - `vec.resize()` 先确保容量足够
    /// - 写入后根据 ODBC 报告的长度截断到实际数据长度
    /// - 如果出错，恢复 `buf` 到 `offset`（操作前的长度）
    fn read_column(
        &self,
        col: u16,
        target_type: SqlSmallInt,
        buf: &mut Vec<u8>,
    ) -> Result<bool, String> {
        let f = funcs()?;
        let offset = buf.len();

        // Chunk size for each SQLGetData call. Balances syscall overhead
        // vs. memory waste on small values.
        const CHUNK: usize = 8192;

        loop {
            let pos = buf.len();
            buf.resize(pos + CHUNK, 0);

            let mut len_or_ind: SqlLen = 0;
            let ret = unsafe {
                (f.sql_get_data)(
                    self.stmt(),
                    col as SqlUSmallInt,
                    target_type,
                    buf.as_mut_ptr().add(pos) as SqlPointer,
                    CHUNK as SqlLen,
                    &mut len_or_ind,
                )
            };

            if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
                unsafe { buf.set_len(offset) };
                return Err(odbc_err(format!(
                    "SQLGetData col {col} failed: {ret}{}",
                    odbc_diag(SQL_HANDLE_STMT, self.stmt())
                )));
            }

            // NULL check on first chunk only
            if pos == offset && len_or_ind == SQL_NULL_DATA {
                unsafe { buf.set_len(offset) };
                return Ok(false);
            }

            // For character types the driver reserves one byte for the
            // null terminator within the buffer.
            let max_data = if target_type == SQL_C_CHAR {
                CHUNK - 1
            } else {
                CHUNK
            };

            let chunk_len = if len_or_ind == SQL_NULL_DATA || len_or_ind < 0 {
                // Unknown total length. For final chunk of char data,
                // locate the null terminator byte.
                if ret == SQL_SUCCESS && target_type == SQL_C_CHAR {
                    let written = &buf[pos..];
                    written.iter().position(|&b| b == 0).unwrap_or(max_data)
                } else {
                    max_data
                }
            } else {
                (len_or_ind as usize).min(max_data)
            };

            unsafe { buf.set_len(pos + chunk_len) };

            if ret == SQL_SUCCESS {
                return Ok(true);
            }
            // SQL_SUCCESS_WITH_INFO: more data available, continue loop
        }
    }
}

// ===========================================================================
// 建连入口
// ===========================================================================

/// 全局 ODBC 环境句柄，进程生命周期内只初始化一次．
///
/// 使用 `OnceLock` 保证：
/// - 首次调用 `global_env()` 时初始化（惰性）
/// - 初始化后永远返回同一个环境句柄
/// - 初始化失败时缓存错误，不重试
static GLOBAL_ENV: OnceLock<Result<DynEnv, String>> = OnceLock::new();

/// 获取全局 ODBC 环境句柄的引用．
///
/// 首次调用时分配并初始化 ODBC 环境．之后每次调用返回已缓存的结果．
/// 如果 ODBC 库不存在，返回的 Err 会一直复用，避免重复尝试．
pub(crate) fn global_env() -> Result<&'static DynEnv, String> {
    GLOBAL_ENV
        .get_or_init(DynEnv::new)
        .as_ref()
        .map_err(|e| e.clone())
}

/// 创建新的 ODBC 连接（对外统一入口）．
///
/// 支持两种建连方式（二选一）：
/// - `conn_str`: 传完整 ODBC 连接字符串（`Driver={...};SERVER=...;...`）
/// - `dsn`: 传 (DSN, username, password) 三元组
///
/// 建连前会先通过 `global_env()` 确保 ODBC 环境已初始化．
pub(crate) fn open_connection(
    conn_str: Option<&str>,
    dsn: Option<(&str, &str, &str)>,
    login_timeout_sec: Option<u32>,
) -> Result<DynConn, String> {
    let env = global_env()?;
    let conn = DynConn::alloc(env)?;
    conn.set_login_timeout(login_timeout_sec)?;

    if let Some(cs) = conn_str {
        conn.driver_connect(cs)?;
    } else if let Some((dsn, user, pwd)) = dsn {
        conn.connect(dsn, user, pwd)?;
    } else {
        return Err(odbc_err("no connection parameters provided"));
    }

    Ok(conn)
}

// ===========================================================================
// 工具函数
// ===========================================================================

/// Rust 字符串 → C 字符串（以 '\0' 结尾）．
///
/// 如果字符串包含内部 '\0' 字节，`CString::new()` 会失败．
/// 此时用空字符串替代（ODBC 连接参数不应包含 '\0'，仅防御性处理）．
fn to_cstring(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| CString::new("").unwrap())
}

/// 类似于 `to_cstring`，但空字符串也返回有效 CString（而非 NULL 指针）．
///
/// ODBC 的 `SQLColumns` 等函数中，空字符串表示"不过滤此项"，
/// 与 NULL 指针（表示"不传此参数"）语义不同．
fn to_cstring_or_null(s: &str) -> CString {
    if s.is_empty() {
        CString::new("").unwrap()
    } else {
        to_cstring(s)
    }
}

/// 从 `SQLColumns` 结果集中读取一个字符串列．
///
/// 使用固定 256 字节的初始缓冲区．对于表名、列名、类型名等元数据，
/// 256 字节足够覆盖绝大多数情况．如果驱动返回的元数据超过此长度，
/// 会被截断（ODBC 元数据通常不会超过 128 字节）．
fn read_string_col(hstmt: SqlHandle, col: u16) -> Result<String, String> {
    let f = funcs()?;
    let mut buf = vec![0u8; 256];
    let mut len_or_ind: SqlLen = 0;
    let ret = unsafe {
        (f.sql_get_data)(
            hstmt,
            col,
            SQL_C_CHAR,
            buf.as_mut_ptr() as SqlPointer,
            (buf.len() - 1) as SqlLen, // 留 1 字节给 null terminator
            &mut len_or_ind,
        )
    };
    if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
        return Err(odbc_err(format!(
            "read_string_col({col}) failed: {ret}{}",
            odbc_diag(SQL_HANDLE_STMT, hstmt)
        )));
    }
    if len_or_ind == SQL_NULL_DATA {
        return Ok(String::new());
    }
    let data_len = if len_or_ind < 0 {
        buf.len() - 1
    } else {
        len_or_ind as usize
    };
    // 从 buffer 中提取 C 字符串（可能包含 '\0' 终止符）
    let s = CStr::from_bytes_until_nul(&buf[..=data_len.min(buf.len() - 1)]).unwrap_or(c"");
    Ok(s.to_string_lossy().into_owned())
}

/// 读取可能为 NULL 的字符串列，NULL 返回 `None`．
fn read_optional_string_col(hstmt: SqlHandle, col: u16) -> Result<Option<String>, String> {
    let s = read_string_col(hstmt, col)?;
    if s.is_empty() { Ok(None) } else { Ok(Some(s)) }
}

/// 从 `SQLColumns` 结果集中读取一个 32 位整型列．
///
/// 使用 `SQL_C_SLONG` (C `long`, 32 位在 64 位系统上也是 32 位) 作为目标类型．
fn read_i32_col(hstmt: SqlHandle, col: u16) -> Result<Option<i32>, String> {
    let f = funcs()?;
    let mut val: i32 = 0;
    let mut len_or_ind: SqlLen = 0;
    let ret = unsafe {
        (f.sql_get_data)(
            hstmt,
            col,
            4, // SQL_C_SLONG
            (&mut val as *mut i32) as SqlPointer,
            0, // 固定长度类型，不需要 buffer
            &mut len_or_ind,
        )
    };
    if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
        return Err(odbc_err(format!(
            "read_i32_col({col}) failed: {ret}{}",
            odbc_diag(SQL_HANDLE_STMT, hstmt)
        )));
    }
    if len_or_ind == SQL_NULL_DATA {
        Ok(None)
    } else {
        Ok(Some(val))
    }
}

/// 从 `SQLColumns` 结果集中读取一个 16 位整型列（如 DATA_TYPE）．
///
/// 使用 `SQL_C_SSHORT` (C `short`) 作为目标类型．
fn read_i16_col(hstmt: SqlHandle, col: u16) -> Result<Option<i16>, String> {
    let f = funcs()?;
    let mut val: i16 = 0;
    let mut len_or_ind: SqlLen = 0;
    let ret = unsafe {
        (f.sql_get_data)(
            hstmt,
            col,
            5, // SQL_C_SSHORT
            (&mut val as *mut i16) as SqlPointer,
            0,
            &mut len_or_ind,
        )
    };
    if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
        return Err(odbc_err(format!(
            "read_i16_col({col}) failed: {ret}{}",
            odbc_diag(SQL_HANDLE_STMT, hstmt)
        )));
    }
    if len_or_ind == SQL_NULL_DATA {
        Ok(None)
    } else {
        Ok(Some(val))
    }
}

// ===========================================================================
// 连接串属性值转义
// ===========================================================================

/// 转义 ODBC 连接串中的属性值，兼容 `odbc_api::escape_attribute_value` 的行为．
///
/// ## ODBC 连接串转义规则
///
/// ODBC 连接字符串使用 `KEY=VALUE;` 格式，值中如果包含特殊字符
/// `[]{}()!;?*=,@` 或者值以 `DRIVER=` 开头，需要用 `{}` 包裹，
/// 并将值内的 `}` 转义为 `}}`．
///
/// ## 示例
///
/// ```text
/// "simple"     → "simple"          （无需转义）
/// "a;b"        → "{a;b}"           （分号需要转义）
/// "abc{123}"   → "{abc{123}}}"     （括号和花括号需要转义）
/// ""           → ""                （空字符串不转义）
/// ```
pub(crate) fn escape_attr_value(value: &str) -> String {
    let needs_escape = value.bytes().any(|b| {
        matches!(
            b,
            b'[' | b']' | b'{' | b'}' | b'(' | b')' | b'!' | b';' | b'?' | b'*' | b'=' | b','
        )
    }) || value.to_ascii_uppercase().starts_with("DRIVER=");
    if needs_escape {
        format!("{{{}}}", value.replace('}', "}}"))
    } else {
        value.to_string()
    }
}
