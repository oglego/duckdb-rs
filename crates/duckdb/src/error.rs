use arrow::datatypes::DataType;

use super::Result;
use crate::{
    ffi,
    types::{FromSqlError, Type},
};
use std::{error, ffi::CStr, fmt, path::PathBuf, str};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    Invalid = 0,
    OutOfRange = 1,
    Conversion = 2,
    UnknownType = 3,
    Decimal = 4,
    MismatchType = 5,
    DivideByZero = 6,
    ObjectSize = 7,
    InvalidType = 8,
    Serialization = 9,
    Transaction = 10,
    NotImplemented = 11,
    Expression = 12,
    Catalog = 13,
    Parser = 14,
    Planner = 15,
    Scheduler = 16,
    Executor = 17,
    Constraint = 18,
    Index = 19,
    Stat = 20,
    Connection = 21,
    Syntax = 22,
    Settings = 23,
    Binder = 24,
    Network = 25,
    Optimizer = 26,
    NullPointer = 27,
    IO = 28,
    Interrupt = 29,
    Fatal = 30,
    Internal = 31,
    InvalidInput = 32,
    OutOfMemory = 33,
    Permission = 34,
    ParameterNotResolved = 35,
    ParameterNotAllowed = 36,
    Dependency = 37,
    Http = 38,
    MissingExtension = 39,
    Autoload = 40,
    Sequence = 41,
    InvalidConfiguration = 42,
    #[doc(hidden)]
    Unknown = 999,
}

impl From<ffi::duckdb_error_type> for ErrorCode {
    fn from(code: ffi::duckdb_error_type) -> Self {
        match code {
            ffi::duckdb_error_type_DUCKDB_ERROR_INVALID => ErrorCode::Invalid,
            ffi::duckdb_error_type_DUCKDB_ERROR_OUT_OF_RANGE => ErrorCode::OutOfRange,
            ffi::duckdb_error_type_DUCKDB_ERROR_CONVERSION => ErrorCode::Conversion,
            ffi::duckdb_error_type_DUCKDB_ERROR_UNKNOWN_TYPE => ErrorCode::UnknownType,
            ffi::duckdb_error_type_DUCKDB_ERROR_DECIMAL => ErrorCode::Decimal,
            ffi::duckdb_error_type_DUCKDB_ERROR_MISMATCH_TYPE => ErrorCode::MismatchType,
            ffi::duckdb_error_type_DUCKDB_ERROR_DIVIDE_BY_ZERO => ErrorCode::DivideByZero,
            ffi::duckdb_error_type_DUCKDB_ERROR_OBJECT_SIZE => ErrorCode::ObjectSize,
            ffi::duckdb_error_type_DUCKDB_ERROR_INVALID_TYPE => ErrorCode::InvalidType,
            ffi::duckdb_error_type_DUCKDB_ERROR_SERIALIZATION => ErrorCode::Serialization,
            ffi::duckdb_error_type_DUCKDB_ERROR_TRANSACTION => ErrorCode::Transaction,
            ffi::duckdb_error_type_DUCKDB_ERROR_NOT_IMPLEMENTED => ErrorCode::NotImplemented,
            ffi::duckdb_error_type_DUCKDB_ERROR_EXPRESSION => ErrorCode::Expression,
            ffi::duckdb_error_type_DUCKDB_ERROR_CATALOG => ErrorCode::Catalog,
            ffi::duckdb_error_type_DUCKDB_ERROR_PARSER => ErrorCode::Parser,
            ffi::duckdb_error_type_DUCKDB_ERROR_PLANNER => ErrorCode::Planner,
            ffi::duckdb_error_type_DUCKDB_ERROR_SCHEDULER => ErrorCode::Scheduler,
            ffi::duckdb_error_type_DUCKDB_ERROR_EXECUTOR => ErrorCode::Executor,
            ffi::duckdb_error_type_DUCKDB_ERROR_CONSTRAINT => ErrorCode::Constraint,
            ffi::duckdb_error_type_DUCKDB_ERROR_INDEX => ErrorCode::Index,
            ffi::duckdb_error_type_DUCKDB_ERROR_STAT => ErrorCode::Stat,
            ffi::duckdb_error_type_DUCKDB_ERROR_CONNECTION => ErrorCode::Connection,
            ffi::duckdb_error_type_DUCKDB_ERROR_SYNTAX => ErrorCode::Syntax,
            ffi::duckdb_error_type_DUCKDB_ERROR_SETTINGS => ErrorCode::Settings,
            ffi::duckdb_error_type_DUCKDB_ERROR_BINDER => ErrorCode::Binder,
            ffi::duckdb_error_type_DUCKDB_ERROR_NETWORK => ErrorCode::Network,
            ffi::duckdb_error_type_DUCKDB_ERROR_OPTIMIZER => ErrorCode::Optimizer,
            ffi::duckdb_error_type_DUCKDB_ERROR_NULL_POINTER => ErrorCode::NullPointer,
            ffi::duckdb_error_type_DUCKDB_ERROR_IO => ErrorCode::IO,
            ffi::duckdb_error_type_DUCKDB_ERROR_INTERRUPT => ErrorCode::Interrupt,
            ffi::duckdb_error_type_DUCKDB_ERROR_FATAL => ErrorCode::Fatal,
            ffi::duckdb_error_type_DUCKDB_ERROR_INTERNAL => ErrorCode::Internal,
            ffi::duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT => ErrorCode::InvalidInput,
            ffi::duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY => ErrorCode::OutOfMemory,
            ffi::duckdb_error_type_DUCKDB_ERROR_PERMISSION => ErrorCode::Permission,
            ffi::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_RESOLVED => ErrorCode::ParameterNotResolved,
            ffi::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_ALLOWED => ErrorCode::ParameterNotAllowed,
            ffi::duckdb_error_type_DUCKDB_ERROR_DEPENDENCY => ErrorCode::Dependency,
            ffi::duckdb_error_type_DUCKDB_ERROR_HTTP => ErrorCode::Http,
            ffi::duckdb_error_type_DUCKDB_ERROR_MISSING_EXTENSION => ErrorCode::MissingExtension,
            ffi::duckdb_error_type_DUCKDB_ERROR_AUTOLOAD => ErrorCode::Autoload,
            ffi::duckdb_error_type_DUCKDB_ERROR_SEQUENCE => ErrorCode::Sequence,
            ffi::duckdb_error_type_DUCKDB_INVALID_CONFIGURATION => ErrorCode::InvalidConfiguration,
            _ => ErrorCode::Unknown,
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // This reuses the Debug representation (e.g., "Http")
        // as the Display string.
        write!(f, "{:?}", self)
    }
}

/// Enum listing possible errors from duckdb.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum Error {
    /// An error from an underlying DuckDB call.
    DuckDBFailure(ErrorCode, Option<String>),

    /// Error when the value of a particular column is requested, but it cannot
    /// be converted to the requested Rust type.
    FromSqlConversionFailure(usize, Type, Box<dyn error::Error + Send + Sync + 'static>),

    /// Error when DuckDB gives us an integral value outside the range of the
    /// requested type (e.g., trying to get the value 1000 into a `u8`).
    /// The associated `usize` is the column index,
    /// and the associated `i64` is the value returned by SQLite.
    IntegralValueOutOfRange(usize, i128),

    /// Error converting a string to UTF-8.
    Utf8Error(str::Utf8Error),

    /// Error converting a string to a C-compatible string because it contained
    /// an embedded nul.
    NulError(::std::ffi::NulError),

    /// Error when using SQL named parameters and passing a parameter name not
    /// present in the SQL.
    InvalidParameterName(String),

    /// Error converting a file path to a string.
    InvalidPath(PathBuf),

    /// Error returned when an [`execute`](crate::Connection::execute) call
    /// returns rows.
    ExecuteReturnedResults,

    /// Error when a query that was expected to return at least one row (e.g.,
    /// for [`query_row`](crate::Connection::query_row)) did not return any.
    QueryReturnedNoRows,

    /// Error when a query that was expected to return only one row (e.g.,
    /// for [`query_one`](crate::Connection::query_one)) did return more than one.
    QueryReturnedMoreThanOneRow,

    /// Error when the value of a particular column is requested, but the index
    /// is out of range for the statement.
    InvalidColumnIndex(usize),

    /// Error when the value of a named column is requested, but no column
    /// matches the name for the statement.
    InvalidColumnName(String),

    /// Error when the value of a particular column is requested, but the type
    /// of the result in that column cannot be converted to the requested
    /// Rust type.
    InvalidColumnType(usize, String, Type),

    /// Error when datatype to duckdb type
    ArrowTypeToDuckdbType(String, DataType),

    /// Error when a query that was expected to insert one row did not insert
    /// any or insert many.
    StatementChangedRows(usize),

    /// Error available for the implementors of the
    /// [`ToSql`](crate::types::ToSql) trait.
    ToSqlConversionFailure(Box<dyn error::Error + Send + Sync + 'static>),

    /// Error when the SQL is not a `SELECT`, is not read-only.
    InvalidQuery,

    /// Error when the SQL contains multiple statements.
    MultipleStatement,

    /// Error when the number of bound parameters does not match the number of
    /// parameters in the query. The first `usize` is how many parameters were
    /// given, the 2nd is how many were expected.
    InvalidParameterCount(usize, usize),

    /// Error when a parameter is requested, but the index is out of range
    /// for the statement.
    InvalidParameterIndex(usize),

    /// Append Error
    AppendError,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DuckDBFailure(e1, s1), Self::DuckDBFailure(e2, s2)) => e1 == e2 && s1 == s2,
            (Self::IntegralValueOutOfRange(i1, n1), Self::IntegralValueOutOfRange(i2, n2)) => i1 == i2 && n1 == n2,
            (Self::Utf8Error(e1), Self::Utf8Error(e2)) => e1 == e2,
            (Self::NulError(e1), Self::NulError(e2)) => e1 == e2,
            (Self::InvalidParameterName(n1), Self::InvalidParameterName(n2)) => n1 == n2,
            (Self::InvalidPath(p1), Self::InvalidPath(p2)) => p1 == p2,
            (Self::ExecuteReturnedResults, Self::ExecuteReturnedResults) => true,
            (Self::QueryReturnedNoRows, Self::QueryReturnedNoRows) => true,
            (Self::QueryReturnedMoreThanOneRow, Self::QueryReturnedMoreThanOneRow) => true,
            (Self::InvalidColumnIndex(i1), Self::InvalidColumnIndex(i2)) => i1 == i2,
            (Self::InvalidColumnName(n1), Self::InvalidColumnName(n2)) => n1 == n2,
            (Self::InvalidColumnType(i1, n1, t1), Self::InvalidColumnType(i2, n2, t2)) => {
                i1 == i2 && t1 == t2 && n1 == n2
            }
            (Self::StatementChangedRows(n1), Self::StatementChangedRows(n2)) => n1 == n2,
            (Self::InvalidParameterCount(i1, n1), Self::InvalidParameterCount(i2, n2)) => i1 == i2 && n1 == n2,
            (Self::InvalidParameterIndex(i1), Self::InvalidParameterIndex(i2)) => i1 == i2,
            (..) => false,
        }
    }
}

impl From<str::Utf8Error> for Error {
    #[cold]
    fn from(err: str::Utf8Error) -> Self {
        Self::Utf8Error(err)
    }
}

impl From<::std::ffi::NulError> for Error {
    #[cold]
    fn from(err: ::std::ffi::NulError) -> Self {
        Self::NulError(err)
    }
}

const UNKNOWN_COLUMN: usize = usize::MAX;

/// The conversion isn't precise, but it's convenient to have it
/// to allow use of `get_raw(…).as_…()?` in callbacks that take `Error`.
impl From<FromSqlError> for Error {
    #[cold]
    fn from(err: FromSqlError) -> Self {
        // The error type requires index and type fields, but they aren't known in this
        // context.
        match err {
            FromSqlError::OutOfRange(val) => Self::IntegralValueOutOfRange(UNKNOWN_COLUMN, val),
            #[cfg(feature = "uuid")]
            FromSqlError::InvalidUuidSize(_) => {
                Self::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Blob, Box::new(err))
            }
            FromSqlError::Other(source) => Self::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, source),
            _ => Self::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, Box::new(err)),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::DuckDBFailure(ref err, None) => err.fmt(f),
            Self::DuckDBFailure(_, Some(ref s)) => write!(f, "{s}"),
            Self::FromSqlConversionFailure(i, ref t, ref err) => {
                if i != UNKNOWN_COLUMN {
                    write!(f, "Conversion error from type {t} at index: {i}, {err}")
                } else {
                    err.fmt(f)
                }
            }
            Self::IntegralValueOutOfRange(col, val) => {
                if col != UNKNOWN_COLUMN {
                    write!(f, "Integer {val} out of range at index {col}")
                } else {
                    write!(f, "Integer {val} out of range")
                }
            }
            Self::Utf8Error(ref err) => err.fmt(f),
            Self::NulError(ref err) => err.fmt(f),
            Self::InvalidParameterName(ref name) => write!(f, "Invalid parameter name: {name}"),
            Self::InvalidPath(ref p) => write!(f, "Invalid path: {}", p.to_string_lossy()),
            Self::ExecuteReturnedResults => {
                write!(f, "Execute returned results - did you mean to call query?")
            }
            Self::QueryReturnedNoRows => write!(f, "Query returned no rows"),
            Self::QueryReturnedMoreThanOneRow => write!(f, "Query returned more than one row"),
            Self::InvalidColumnIndex(i) => write!(f, "Invalid column index: {i}"),
            Self::InvalidColumnName(ref name) => write!(f, "Invalid column name: {name}"),
            Self::InvalidColumnType(i, ref name, ref t) => {
                write!(f, "Invalid column type {t} at index: {i}, name: {name}")
            }
            Self::ArrowTypeToDuckdbType(ref name, ref t) => {
                write!(f, "Invalid column type {t} , name: {name}")
            }
            Self::InvalidParameterCount(i1, n1) => {
                write!(f, "Wrong number of parameters passed to query. Got {i1}, needed {n1}")
            }
            Self::InvalidParameterIndex(i) => write!(f, "Invalid parameter index: {i}"),
            Self::StatementChangedRows(i) => write!(f, "Query changed {i} rows"),
            Self::ToSqlConversionFailure(ref err) => err.fmt(f),
            Self::InvalidQuery => write!(f, "Query is not read-only"),
            Self::MultipleStatement => write!(f, "Multiple statements provided"),
            Self::AppendError => write!(f, "Append error"),
        }
    }
}

impl std::error::Error for ErrorCode {}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::DuckDBFailure(ref err, _) => Some(err),
            Self::Utf8Error(ref err) => Some(err),
            Self::NulError(ref err) => Some(err),

            Self::IntegralValueOutOfRange(..)
            | Self::InvalidParameterName(_)
            | Self::ExecuteReturnedResults
            | Self::QueryReturnedNoRows
            | Self::QueryReturnedMoreThanOneRow
            | Self::InvalidColumnIndex(_)
            | Self::InvalidColumnName(_)
            | Self::InvalidColumnType(..)
            | Self::InvalidPath(_)
            | Self::InvalidParameterCount(..)
            | Self::InvalidParameterIndex(_)
            | Self::StatementChangedRows(_)
            | Self::InvalidQuery
            | Self::AppendError
            | Self::ArrowTypeToDuckdbType(..)
            | Self::MultipleStatement => None,
            Self::FromSqlConversionFailure(_, _, ref err) | Self::ToSqlConversionFailure(ref err) => Some(&**err),
        }
    }
}

// These are public but not re-exported by lib.rs, so only visible within crate.

#[inline]
fn error_from_duckdb_code(code: ffi::duckdb_state, message: Option<String>) -> Result<()> {
    Err(Error::DuckDBFailure(ErrorCode::Unknown, message))
}

#[cold]
#[inline]
pub fn result_from_duckdb_appender(code: ffi::duckdb_state, appender: *mut ffi::duckdb_appender) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if (*appender).is_null() {
            Some("appender is null".to_string())
        } else {
            let c_err = ffi::duckdb_appender_error(*appender);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_appender_destroy(appender);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_prepare(code: ffi::duckdb_state, mut prepare: ffi::duckdb_prepared_statement) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if prepare.is_null() {
            Some("prepare is null".to_string())
        } else {
            let c_err = ffi::duckdb_prepare_error(prepare);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_destroy_prepare(&mut prepare);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_arrow(code: ffi::duckdb_state, mut out: ffi::duckdb_arrow) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if out.is_null() {
            Some("out is null".to_string())
        } else {
            let c_err = ffi::duckdb_query_arrow_error(out);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_destroy_arrow(&mut out);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_extract(
    num_statements: ffi::idx_t,
    mut extracted: ffi::duckdb_extracted_statements,
) -> Result<()> {
    if num_statements > 0 {
        return Ok(());
    }
    unsafe {
        let message = if extracted.is_null() {
            Some("extracted statements are null".to_string())
        } else {
            let c_err = ffi::duckdb_extract_statements_error(extracted);
            let message = if c_err.is_null() {
                None
            } else {
                Some(CStr::from_ptr(c_err).to_string_lossy().to_string())
            };
            ffi::duckdb_destroy_extracted(&mut extracted);
            message
        };
        error_from_duckdb_code(ffi::DuckDBError, message)
    }
}
