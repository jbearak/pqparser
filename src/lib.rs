//! pqparser's wasm module: arrow-rs's Parquet reader behind a small C ABI.
//!
//! The module reads one file (each reader instantiates the module once),
//! through `host_read`, which JavaScript provides. `pq_open` reads the
//! footer, `pq_meta` describes it as JSON (the footer's facts, no
//! interpretation), `pq_read` decodes some columns over a window of rows
//! into flat buffers that `pq_directory` points at. What the columns mean
//! (types, the dta profile, missing values) is decided in TypeScript.

use std::cell::RefCell;
use std::fmt::Write as _;
use std::io::{BufReader, Read};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrowPrimitiveType};
use arrow_schema::{DataType, TimeUnit as ArrowUnit};
use base64::Engine as _;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::arrow::ProjectionMask;
use parquet::basic::{Compression, LogicalType, TimeUnit};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::statistics::Statistics;

#[link(wasm_import_module = "env")]
extern "C" {
    /// Copy `len` bytes of the file from `offset` to `ptr`; 0 on success.
    fn host_read(ptr: *mut u8, len: usize, offset: f64) -> i32;
}

/// The file, read through JavaScript.
#[derive(Clone, Copy)]
struct Host {
    len: u64,
}

struct HostRead {
    at: u64,
    end: u64,
}

impl Read for HostRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = buf.len().min((self.end - self.at) as usize);
        if n > 0 {
            if unsafe { host_read(buf.as_mut_ptr(), n, self.at as f64) } != 0 {
                return Err(std::io::Error::other("reading the file failed"));
            }
            self.at += n as u64;
        }
        Ok(n)
    }
}

impl Length for Host {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for Host {
    type T = BufReader<HostRead>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(BufReader::with_capacity(64 << 10, HostRead { at: start.min(self.len), end: self.len }))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if start.checked_add(length as u64).is_none_or(|e| e > self.len) {
            return Err(parquet::errors::ParquetError::EOF(format!(
                "bytes {start} to {} are past the end of the file",
                start + length as u64
            )));
        }
        let mut v = vec![0u8; length];
        if length > 0 && unsafe { host_read(v.as_mut_ptr(), length, start as f64) } != 0 {
            return Err(parquet::errors::ParquetError::General("reading the file failed".into()));
        }
        Ok(v.into())
    }
}

/// What one read leaves for JavaScript, per column.
enum Out {
    Number {
        values: Vec<f64>,
        codes: Vec<u8>,
    },
    BigInt {
        values: Vec<i64>,
        codes: Vec<u8>,
    },
    /// Every row's bytes, end to end, with their ends, nulls and (text
    /// not all ASCII) ends in UTF-16 units; or, with `codes`, the distinct
    /// values the rows use, and a code per row into them (`u32::MAX` for a null).
    Text {
        bytes: Vec<u8>,
        ends: Vec<u32>,
        missing: Vec<u8>,
        utf16: Option<Vec<u32>>,
        codes: Option<Vec<u32>>,
    },
}

struct State {
    file: Host,
    meta: Arc<ParquetMetaData>,
    arrow: Option<ArrowReaderMetadata>,
    outs: Vec<Out>,
    directory: Vec<u32>,
}

thread_local! {
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
    static RESULT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Hand `bytes` to JavaScript (`pq_result` and `pq_result_len`).
fn result(bytes: Vec<u8>) {
    RESULT.with(|r| *r.borrow_mut() = bytes);
}

/// An error: its message, and -1 (the file is damaged) or -2 (the column cannot be read so).
struct Fail(f64, String);

impl From<String> for Fail {
    fn from(message: String) -> Self {
        Fail(-1.0, message)
    }
}

/// Run `f` on the open file; an error's message becomes the result, and its code is returned.
fn with_state(f: impl FnOnce(&mut State) -> Result<f64, Fail>) -> f64 {
    STATE.with(|s| match s.borrow_mut().as_mut() {
        Some(state) => f(state).unwrap_or_else(|Fail(code, message)| {
            result(message.into_bytes());
            code
        }),
        None => {
            result(b"no file is open".to_vec());
            -1.0
        }
    })
}

#[no_mangle]
pub extern "C" fn pq_alloc(n: usize) -> *mut u8 {
    let mut v = Vec::<u8>::with_capacity(n);
    let p = v.as_mut_ptr();
    std::mem::forget(v);
    p
}

/// # Safety
/// `p` came from `pq_alloc(n)`.
#[no_mangle]
pub unsafe extern "C" fn pq_free(p: *mut u8, n: usize) {
    drop(Vec::from_raw_parts(p, 0, n));
}

#[no_mangle]
pub extern "C" fn pq_result() -> *const u8 {
    RESULT.with(|r| r.borrow().as_ptr())
}

#[no_mangle]
pub extern "C" fn pq_result_len() -> usize {
    RESULT.with(|r| r.borrow().len())
}

/// Read the footer (and offset indexes) of a file of `len` bytes; 0 on success.
#[no_mangle]
pub extern "C" fn pq_open(len: f64) -> f64 {
    // A panic's message, for JavaScript to read after the trap.
    std::panic::set_hook(Box::new(|info| {
        let _ = RESULT.try_with(|r| r.try_borrow_mut().map(|mut r| *r = info.to_string().into_bytes()));
    }));
    let file = Host { len: len as u64 };
    match load(&file) {
        Ok(meta) => {
            STATE.with(|s| {
                *s.borrow_mut() = Some(State { file, meta: Arc::new(meta), arrow: None, outs: Vec::new(), directory: Vec::new() })
            });
            0.0
        }
        Err(e) => {
            result(e.into_bytes());
            -1.0
        }
    }
}

/// The footer, and the offset indexes when there are any.
fn load(file: &Host) -> Result<ParquetMetaData, String> {
    let reader =
        |r: ParquetMetaDataReader| r.with_column_index_policy(PageIndexPolicy::Skip).with_offset_index_policy(PageIndexPolicy::Optional);
    let e = match reader(ParquetMetaDataReader::new()).parse_and_finish(file) {
        Ok(meta) => return Ok(meta),
        Err(e) => e.to_string(),
    };
    // Parquet2.jl gives a DECIMAL's precision and scale in its logical
    // type alone, which arrow-rs refuses: they are added and the footer
    // read again.
    if !e.contains("DECIMAL logical type") || file.len < 12 {
        return Err(e);
    }
    let tail = file.get_bytes(file.len - 8, 8).map_err(|e| e.to_string())?;
    let n = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;
    let footer = file.get_bytes(file.len.saturating_sub(8 + n), n as usize).map_err(|e| e.to_string())?;
    let fixed = decimal_fields(&footer).ok_or(e)?;
    let meta = ParquetMetaDataReader::decode_metadata(&fixed).map_err(|e| e.to_string())?;
    let mut r = reader(ParquetMetaDataReader::new_with_metadata(meta));
    r.read_page_indexes(file).map_err(|e| e.to_string())?;
    r.finish().map_err(|e| e.to_string())
}

/// Thrift's compact protocol, as far as `decimal_fields` needs it.
struct Thrift<'a> {
    b: &'a [u8],
    at: usize,
}

impl Thrift<'_> {
    fn byte(&mut self) -> Option<u8> {
        let x = *self.b.get(self.at)?;
        self.at += 1;
        Some(x)
    }

    fn varint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let x = self.byte()?;
            v |= u64::from(x & 0x7f) << shift;
            if x & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    }

    fn i32(&mut self) -> Option<i32> {
        let v = self.varint()? as u32;
        Some((v >> 1) as i32 ^ -((v & 1) as i32))
    }

    /// The next field's id and type, or `None` at the struct's end.
    fn field(&mut self, last: &mut i16) -> Option<Option<(i16, u8)>> {
        let x = self.byte()?;
        if x == 0 {
            return Some(None);
        }
        *last = match x >> 4 {
            0 => self.i32()? as i16,
            delta => *last + delta as i16,
        };
        Some(Some((*last, x & 0x0f)))
    }

    /// Skip a value of type `ty` (`in_field`: a bool's value is in the field header).
    fn skip(&mut self, ty: u8, in_field: bool, depth: usize) -> Option<()> {
        if depth > 64 {
            return None;
        }
        match ty {
            1 | 2 if in_field => {}
            1..=3 => self.at += 1,
            4..=6 => drop(self.varint()?),
            7 => self.at += 8,
            8 => self.at += self.varint()? as usize,
            9 | 10 => {
                let x = self.byte()?;
                let n = if x >> 4 == 15 { self.varint()? } else { u64::from(x >> 4) };
                for _ in 0..n {
                    self.skip(x & 0x0f, false, depth + 1)?;
                }
            }
            11 => {
                let n = self.varint()?;
                if n > 0 {
                    let kv = self.byte()?;
                    for _ in 0..n {
                        self.skip(kv >> 4, false, depth + 1)?;
                        self.skip(kv & 0x0f, false, depth + 1)?;
                    }
                }
            }
            12 => {
                let mut last = 0;
                while let Some((_, t)) = self.field(&mut last)? {
                    self.skip(t, true, depth + 1)?;
                }
            }
            _ => return None,
        }
        (self.at <= self.b.len()).then_some(())
    }
}

/// The footer with each DECIMAL schema element given its precision (field
/// 8) and scale (field 7) from its logical type where it lacks them.
fn decimal_fields(footer: &[u8]) -> Option<Vec<u8>> {
    let mut t = Thrift { b: footer, at: 0 };
    let mut last = 0;
    loop {
        let (id, ty) = t.field(&mut last)??;
        if id != 2 || ty != 9 {
            t.skip(ty, true, 0)?;
            continue;
        }
        let x = t.byte()?;
        let n = if x >> 4 == 15 { t.varint()? } else { u64::from(x >> 4) };
        let mut out = footer[..t.at].to_vec();
        for _ in 0..n {
            let start = t.at;
            let (mut has, mut decimal, mut last) = ([false; 2], None, 0);
            while let Some((id, ty)) = t.field(&mut last)? {
                match (id, ty) {
                    (7 | 8, _) => has[id as usize - 7] = true,
                    (10, 12) => {
                        let mut last = 0;
                        while let Some((id, ty)) = t.field(&mut last)? {
                            if (id, ty) != (5, 12) {
                                t.skip(ty, true, 1)?;
                                continue;
                            }
                            let (mut scale, mut precision, mut last) = (0, 0, 0);
                            while let Some((id, ty)) = t.field(&mut last)? {
                                match (id, ty) {
                                    (1, 5) => scale = t.i32()?,
                                    (2, 5) => precision = t.i32()?,
                                    _ => t.skip(ty, true, 2)?,
                                }
                            }
                            decimal = Some((scale, precision));
                        }
                        continue;
                    }
                    _ => {}
                }
                t.skip(ty, true, 1)?;
            }
            out.extend_from_slice(&footer[start..t.at - 1]);
            if let Some((scale, precision)) = decimal {
                for (k, v) in [(7u8, scale), (8, precision)] {
                    if !has[k as usize - 7] {
                        // A field header in long form: its type, then its id.
                        out.extend_from_slice(&[0x05, k << 1]);
                        let mut z = ((v << 1) ^ (v >> 31)) as u32;
                        while z >= 0x80 {
                            out.push(z as u8 | 0x80);
                            z >>= 7;
                        }
                        out.push(z as u8);
                    }
                }
            }
            out.push(0);
        }
        out.extend_from_slice(&footer[t.at..]);
        return Some(out);
    }
}

// ---------------------------------------------------------------- JSON

fn string(o: &mut String, s: &str) {
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(o, "\\u{:04x}", c as u32);
            }
            c => o.push(c),
        }
    }
    o.push('"');
}

fn number(o: &mut String, x: f64) {
    if x.is_finite() {
        let _ = write!(o, "{x}");
    } else {
        o.push_str("null");
    }
}

fn hex(o: &mut String, bytes: &[u8]) {
    o.push('"');
    for b in bytes {
        let _ = write!(o, "{b:02x}");
    }
    o.push('"');
}

fn unit(u: &TimeUnit) -> &'static str {
    match u {
        TimeUnit::MILLIS => "MILLIS",
        TimeUnit::MICROS => "MICROS",
        TimeUnit::NANOS => "NANOS",
    }
}

/// A logical type as `[name, ...parameters]`.
fn logical(o: &mut String, l: &LogicalType) {
    match l {
        LogicalType::Decimal(d) => {
            let _ = write!(o, "[\"DECIMAL\",{},{}]", d.precision, d.scale);
        }
        LogicalType::Time(t) => {
            let _ = write!(o, "[\"TIME\",\"{}\",{}]", unit(&t.unit), t.is_adjusted_to_u_t_c);
        }
        LogicalType::Timestamp(t) => {
            let _ = write!(o, "[\"TIMESTAMP\",\"{}\",{}]", unit(&t.unit), t.is_adjusted_to_u_t_c);
        }
        LogicalType::Integer(i) => {
            let _ = write!(o, "[\"INT\",{},{}]", i.bit_width, i.is_signed);
        }
        other => {
            let name = format!("{other:?}");
            let name = name.split(|c: char| !c.is_alphanumeric() && c != '_').next().unwrap_or("");
            o.push('[');
            string(o, &name.to_uppercase());
            o.push(']');
        }
    }
}

fn codec(c: Compression) -> &'static str {
    match c {
        Compression::UNCOMPRESSED => "none",
        Compression::SNAPPY => "snappy",
        Compression::GZIP(_) => "gzip",
        Compression::LZO => "lzo",
        Compression::BROTLI(_) => "brotli",
        Compression::LZ4 => "lz4",
        Compression::ZSTD(_) => "zstd",
        Compression::LZ4_RAW => "lz4_raw",
    }
}

/// A statistic: a number, an INT64 as text, bytes as hex, or null.
fn stat(o: &mut String, s: &Statistics, max: bool) {
    macro_rules! pick {
        ($v:expr) => {
            if max {
                $v.max_opt()
            } else {
                $v.min_opt()
            }
        };
    }
    match s {
        Statistics::Boolean(v) => match pick!(v) {
            Some(b) => o.push_str(if *b { "1" } else { "0" }),
            None => o.push_str("null"),
        },
        Statistics::Int32(v) => match pick!(v) {
            Some(x) => number(o, *x as f64),
            None => o.push_str("null"),
        },
        Statistics::Int64(v) => match pick!(v) {
            Some(x) => {
                let _ = write!(o, "\"{x}\"");
            }
            None => o.push_str("null"),
        },
        Statistics::Float(v) => match pick!(v) {
            Some(x) => number(o, *x as f64),
            None => o.push_str("null"),
        },
        Statistics::Double(v) => match pick!(v) {
            Some(x) => number(o, *x),
            None => o.push_str("null"),
        },
        Statistics::ByteArray(v) => match pick!(v) {
            Some(x) => hex(o, x.data()),
            None => o.push_str("null"),
        },
        Statistics::FixedLenByteArray(v) => match pick!(v) {
            Some(x) => hex(o, x.data()),
            None => o.push_str("null"),
        },
        Statistics::Int96(_) => o.push_str("null"),
    }
}

/// `ARROW:schema`'s top-level fields: `[name, dictionary of text, zone]`,
/// or the reason it could not be read.
fn arrow_fields(o: &mut String, text: &str) {
    let fields = base64::engine::general_purpose::STANDARD.decode(text).map_err(|e| e.to_string()).and_then(|bytes| {
        let slice = if bytes.len() > 8 && bytes[..4] == [255; 4] { &bytes[8..] } else { &bytes[..] };
        let message = arrow_ipc::root_as_message(slice).map_err(|e| e.to_string())?;
        let schema = message.header_as_schema().ok_or("the message is not a schema")?;
        arrow_ipc::convert::try_fb_to_schema(schema).map_err(|e| e.to_string())
    });
    match fields {
        Ok(schema) => {
            o.push('[');
            for (k, f) in schema.fields().iter().enumerate() {
                if k > 0 {
                    o.push(',');
                }
                o.push('[');
                string(o, f.name());
                let dictionary = matches!(f.data_type(), DataType::Dictionary(_, v) if matches!(**v, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View));
                let _ = write!(o, ",{dictionary},");
                match f.data_type() {
                    DataType::Timestamp(_, Some(z)) => string(o, z),
                    _ => o.push_str("null"),
                }
                o.push(']');
            }
            o.push(']');
        }
        Err(e) => string(o, &e),
    }
}

/// The footer as JSON: rows, writer, key-value entries, the top-level
/// fields with their leaf's types, `ARROW:schema`'s fields, and per row
/// group its rows and every leaf's chunk.
#[no_mangle]
pub extern "C" fn pq_meta() -> f64 {
    with_state(|state| {
        let meta = &state.meta;
        let fm = meta.file_metadata();
        let schema = fm.schema_descr();
        let mut o = String::from("{\"rows\":");
        number(&mut o, fm.num_rows() as f64);
        o.push_str(",\"createdBy\":");
        string(&mut o, fm.created_by().unwrap_or(""));
        o.push_str(",\"keyValue\":[");
        let mut arrow = None;
        for (k, kv) in fm.key_value_metadata().map_or(&[][..], |v| &v[..]).iter().enumerate() {
            if k > 0 {
                o.push(',');
            }
            o.push('[');
            string(&mut o, &kv.key);
            let _ = write!(o, ",{}]", kv.value.as_ref().map_or(0, String::len));
            if kv.key == "ARROW:schema" && arrow.is_none() {
                arrow = kv.value.as_deref();
            }
        }
        o.push_str("],\"arrow\":");
        match arrow {
            Some(text) => arrow_fields(&mut o, text),
            None => o.push_str("null"),
        }
        // Each top-level field: name, nested, first leaf, logical type,
        // converted type, and (not nested) its physical type, length,
        // precision and scale.
        o.push_str(",\"fields\":[");
        let mut leaf = 0;
        for (k, field) in schema.root_schema().get_fields().iter().enumerate() {
            if k > 0 {
                o.push(',');
            }
            let first = leaf;
            while leaf < schema.num_columns() && schema.get_column_root_idx(leaf) == k {
                leaf += 1;
            }
            let info = field.get_basic_info();
            let nested = field.is_group() || info.repetition() == parquet::basic::Repetition::REPEATED;
            o.push('[');
            string(&mut o, field.name());
            let _ = write!(o, ",{nested},{first},");
            match info.logical_type_ref() {
                Some(l) => logical(&mut o, l),
                None => o.push_str("null"),
            }
            let _ = write!(o, ",\"{}\"", info.converted_type());
            if !nested {
                let c = schema.column(first);
                let _ = write!(o, ",\"{}\",{},{},{}", c.physical_type(), c.type_length(), c.type_precision(), c.type_scale());
            }
            o.push(']');
        }
        // Per row group: rows, then per leaf [codec, encodings, compressed,
        // uncompressed, dictionary page bytes, values, nulls, min, max,
        // whether min and max are the deprecated fields].
        o.push_str("],\"rowGroups\":[");
        for (g, rg) in meta.row_groups().iter().enumerate() {
            if g > 0 {
                o.push(',');
            }
            let _ = write!(o, "[{},[", rg.num_rows());
            for (k, c) in rg.columns().iter().enumerate() {
                if k > 0 {
                    o.push(',');
                }
                let _ = write!(o, "[\"{}\",[", codec(c.compression()));
                for (i, e) in c.encodings().enumerate() {
                    let _ = write!(o, "{}\"{e}\"", if i > 0 { "," } else { "" });
                }
                o.push_str("],");
                number(&mut o, c.compressed_size().max(0) as f64);
                o.push(',');
                number(&mut o, c.uncompressed_size().max(0) as f64);
                o.push(',');
                match c.dictionary_page_offset().filter(|&d| d > 0 && d < c.data_page_offset()) {
                    Some(d) => number(&mut o, (c.data_page_offset() - d) as f64),
                    None => o.push_str("null"),
                }
                o.push(',');
                number(&mut o, c.num_values() as f64);
                o.push(',');
                match c.statistics() {
                    Some(s) => {
                        match s.null_count_opt() {
                            Some(n) => number(&mut o, n as f64),
                            None => o.push_str("null"),
                        }
                        o.push(',');
                        stat(&mut o, s, false);
                        o.push(',');
                        stat(&mut o, s, true);
                        let _ = write!(o, ",{}", s.is_min_max_deprecated());
                    }
                    None => o.push_str("null,null,null,false"),
                }
                o.push(']');
            }
            o.push_str("]]");
        }
        o.push_str("]}");
        result(o.into_bytes());
        Ok(0.0)
    })
}

/// The raw value of key-value entry `k`.
#[no_mangle]
pub extern "C" fn pq_key_value(k: usize) -> f64 {
    with_state(|state| {
        let kv = state.meta.file_metadata().key_value_metadata().and_then(|v| v.get(k));
        result(kv.and_then(|kv| kv.value.clone()).unwrap_or_default().into_bytes());
        Ok(0.0)
    })
}

// ---------------------------------------------------------------- read

/// How a column reads (`pq_read`'s `modes`; 3 is bytes, read as text is).
const NUMBER: u8 = 0;
const BIGINT: u8 = 1;
const TEXT: u8 = 2;

/// The largest safe integer, as JavaScript's `Number.isSafeInteger` has it.
const SAFE: i128 = (1 << 53) - 1;

/// R's `NA_real_`, which `.a` to `.z` carry in bits 32 to 39.
const NA_BITS: u64 = 0x7ff0_0000_0000_07a2;

/// The missing code of a tagged NaN: 2 (`.a`) to 27 (`.z`), else 0.
fn tagged(v: f64) -> u8 {
    let bits = v.to_bits();
    let letter = (bits >> 32) as u8;
    if bits & !(0xff << 32) == NA_BITS && letter.is_ascii_lowercase() {
        letter - b'a' + 2
    } else {
        0
    }
}

struct Column<'a> {
    name: &'a str,
    /// The file row of the column's next value, for messages.
    row: u64,
}

impl Column<'_> {
    fn unsafe_integer(&self, v: i128, i: usize) -> Fail {
        Fail(-2.0, format!(
            "column \"{}\": the value {v} in row {} is beyond the safe integers a JavaScript number holds exactly; read with int64: \"bigint\" to keep every digit",
            self.name,
            self.row + i as u64
        ))
    }
}

/// Numbers and missing codes: 1 (and NaN) for a null.
fn numbers<T: ArrowPrimitiveType>(a: &dyn Array, values: &mut Vec<f64>, codes: &mut Vec<u8>, f: impl Fn(T::Native) -> f64) {
    let a = a.as_primitive::<T>();
    let at = values.len();
    values.extend(a.values().iter().map(|v| f(*v)));
    codes.resize(at + a.len(), 0);
    if let Some(nulls) = a.nulls().filter(|n| n.null_count() > 0) {
        for i in (0..a.len()).filter(|&i| nulls.is_null(i)) {
            values[at + i] = f64::NAN;
            codes[at + i] = 1;
        }
    }
}

/// 64-bit integers as numbers, each checked to be a safe integer.
fn safe_numbers<T: ArrowPrimitiveType>(
    a: &dyn Array,
    values: &mut Vec<f64>,
    codes: &mut Vec<u8>,
    col: &Column,
    f: impl Fn(T::Native) -> i128,
) -> Result<(), Fail> {
    let at = values.len();
    numbers::<T>(a, values, codes, |v| f(v) as f64);
    // A double beyond ±(2^53 - 1) came from an integer beyond it.
    match (at..values.len()).find(|&i| codes[i] == 0 && values[i].abs() > SAFE as f64) {
        Some(i) => Err(col.unsafe_integer(f(a.as_primitive::<T>().value(i - at)), i - at)),
        None => Ok(()),
    }
}

fn bigints<T: ArrowPrimitiveType>(a: &dyn Array, values: &mut Vec<i64>, codes: &mut Vec<u8>, f: impl Fn(T::Native) -> i64) {
    let a = a.as_primitive::<T>();
    let at = values.len();
    values.extend(a.values().iter().map(|v| f(*v)));
    codes.resize(at + a.len(), 0);
    if let Some(nulls) = a.nulls().filter(|n| n.null_count() > 0) {
        for i in (0..a.len()).filter(|&i| nulls.is_null(i)) {
            values[at + i] = 0;
            codes[at + i] = 1;
        }
    }
}

fn pow10(scale: i8) -> f64 {
    10f64.powi(scale as i32)
}

fn add_numbers(a: &dyn Array, values: &mut Vec<f64>, codes: &mut Vec<u8>, col: &Column) -> Result<(), Fail> {
    match a.data_type() {
        DataType::Float64 => {
            let at = values.len();
            numbers::<Float64Type>(a, values, codes, |v| v);
            for i in at..values.len() {
                if codes[i] == 0 && values[i].is_nan() {
                    codes[i] = tagged(values[i]);
                    if codes[i] != 0 {
                        values[i] = f64::NAN;
                    }
                }
            }
        }
        DataType::Float32 => numbers::<Float32Type>(a, values, codes, f64::from),
        // A column of nulls only (pyarrow's `null` type): every row is `.`.
        DataType::Null => {
            values.resize(values.len() + a.len(), f64::NAN);
            codes.resize(codes.len() + a.len(), 1);
        }
        DataType::Boolean => {
            let a = a.as_boolean();
            for i in 0..a.len() {
                let null = a.is_null(i);
                values.push(if null { f64::NAN } else { f64::from(u8::from(a.value(i))) });
                codes.push(u8::from(null));
            }
        }
        DataType::Int8 => numbers::<Int8Type>(a, values, codes, f64::from),
        DataType::Int16 => numbers::<Int16Type>(a, values, codes, f64::from),
        DataType::Int32 => numbers::<Int32Type>(a, values, codes, f64::from),
        DataType::UInt8 => numbers::<UInt8Type>(a, values, codes, f64::from),
        DataType::UInt16 => numbers::<UInt16Type>(a, values, codes, f64::from),
        DataType::UInt32 => numbers::<UInt32Type>(a, values, codes, f64::from),
        DataType::Date32 => numbers::<Date32Type>(a, values, codes, f64::from),
        DataType::Time32(_) => numbers::<Time32MillisecondType>(a, values, codes, f64::from),
        DataType::Decimal32(_, s) => numbers::<Decimal32Type>(a, values, codes, |v| v as f64 / pow10(*s)),
        DataType::Decimal64(_, s) => numbers::<Decimal64Type>(a, values, codes, |v| v as f64 / pow10(*s)),
        DataType::Decimal128(_, s) => numbers::<Decimal128Type>(a, values, codes, |v| v as f64 / pow10(*s)),
        DataType::Int64 => safe_numbers::<Int64Type>(a, values, codes, col, i128::from)?,
        DataType::UInt64 => safe_numbers::<UInt64Type>(a, values, codes, col, i128::from)?,
        DataType::Time64(ArrowUnit::Microsecond) => safe_numbers::<Time64MicrosecondType>(a, values, codes, col, i128::from)?,
        DataType::Time64(_) => safe_numbers::<Time64NanosecondType>(a, values, codes, col, i128::from)?,
        DataType::Timestamp(ArrowUnit::Second, _) => safe_numbers::<TimestampSecondType>(a, values, codes, col, i128::from)?,
        DataType::Timestamp(ArrowUnit::Millisecond, _) => safe_numbers::<TimestampMillisecondType>(a, values, codes, col, i128::from)?,
        DataType::Timestamp(ArrowUnit::Microsecond, _) => safe_numbers::<TimestampMicrosecondType>(a, values, codes, col, i128::from)?,
        DataType::Timestamp(ArrowUnit::Nanosecond, _) => safe_numbers::<TimestampNanosecondType>(a, values, codes, col, i128::from)?,
        t => return Err(Fail(-2.0, format!("column \"{}\": {t} is not read as numbers", col.name))),
    }
    Ok(())
}

fn add_bigints(a: &dyn Array, values: &mut Vec<i64>, codes: &mut Vec<u8>, col: &Column) -> Result<(), Fail> {
    match a.data_type() {
        DataType::Int64 => bigints::<Int64Type>(a, values, codes, |v| v),
        DataType::UInt64 => bigints::<UInt64Type>(a, values, codes, |v| v as i64),
        DataType::Time64(ArrowUnit::Microsecond) => bigints::<Time64MicrosecondType>(a, values, codes, |v| v),
        DataType::Time64(_) => bigints::<Time64NanosecondType>(a, values, codes, |v| v),
        DataType::Timestamp(ArrowUnit::Second, _) => bigints::<TimestampSecondType>(a, values, codes, |v| v),
        DataType::Timestamp(ArrowUnit::Millisecond, _) => bigints::<TimestampMillisecondType>(a, values, codes, |v| v),
        DataType::Timestamp(ArrowUnit::Microsecond, _) => bigints::<TimestampMicrosecondType>(a, values, codes, |v| v),
        DataType::Timestamp(ArrowUnit::Nanosecond, _) => bigints::<TimestampNanosecondType>(a, values, codes, |v| v),
        t => return Err(Fail(-2.0, format!("column \"{}\": {t} is not read as 64-bit integers", col.name))),
    }
    Ok(())
}

fn add_bytes(
    a: &dyn Array,
    bytes: &mut Vec<u8>,
    ends: &mut Vec<u32>,
    missing: &mut Vec<u8>,
    codes: &mut Option<Vec<u32>>,
    col: &Column,
) -> Result<(), Fail> {
    if let Some(d) = a.as_dictionary_opt::<Int32Type>() {
        // Each value the rows use once, however often they use it.
        let values = d.values().as_ref();
        let value = |k: usize| -> Result<&[u8], Fail> {
            Ok(match values.data_type() {
                DataType::Utf8 => values.as_string::<i32>().value(k).as_bytes(),
                DataType::Binary => values.as_binary::<i32>().value(k),
                t => return Err(Fail(-2.0, format!("column \"{}\": a dictionary of {t} is not read as text", col.name))),
            })
        };
        let mut remap = vec![u32::MAX; values.len()];
        let codes = codes.get_or_insert_with(Vec::new);
        for k in d.keys().iter() {
            codes.push(match k {
                None => u32::MAX,
                Some(k) => {
                    let k = k as usize;
                    if remap[k] == u32::MAX {
                        remap[k] = ends.len() as u32;
                        bytes.extend_from_slice(value(k)?);
                        ends.push(bytes.len() as u32);
                    }
                    remap[k]
                }
            });
        }
        return Ok(());
    }
    let mut push = |v: Option<&[u8]>| {
        if let Some(v) = v {
            bytes.extend_from_slice(v);
        }
        missing.push(v.is_none() as u8);
        ends.push(bytes.len() as u32);
    };
    match a.data_type() {
        DataType::Utf8 => a.as_string::<i32>().iter().for_each(|v| push(v.map(str::as_bytes))),
        DataType::LargeUtf8 => a.as_string::<i64>().iter().for_each(|v| push(v.map(str::as_bytes))),
        DataType::Utf8View => a.as_string_view().iter().for_each(|v| push(v.map(str::as_bytes))),
        DataType::Binary => a.as_binary::<i32>().iter().for_each(&mut push),
        DataType::LargeBinary => a.as_binary::<i64>().iter().for_each(&mut push),
        DataType::BinaryView => a.as_binary_view().iter().for_each(&mut push),
        DataType::FixedSizeBinary(_) => a.as_fixed_size_binary().iter().for_each(&mut push),
        t => return Err(Fail(-2.0, format!("column \"{}\": {t} is not read as text", col.name))),
    }
    if bytes.len() > u32::MAX as usize {
        return Err(Fail(-2.0, format!("column \"{}\": the text read is more than 4 GiB", col.name)));
    }
    Ok(())
}

/// Text's end offsets in UTF-16 units; `None` when they are its byte offsets (ASCII).
fn utf16_ends(bytes: &[u8], ends: &[u32]) -> Option<Vec<u32>> {
    if bytes.is_ascii() {
        return None;
    }
    let mut out = Vec::with_capacity(ends.len());
    let (mut from, mut units) = (0usize, 0u32);
    for &e in ends {
        units += String::from_utf8_lossy(&bytes[from..e as usize]).encode_utf16().count() as u32;
        out.push(units);
        from = e as usize;
    }
    Some(out)
}

/// The rows to select from each row group that overlaps rows `start` to `start + count`.
fn selection(meta: &ParquetMetaData, start: u64, count: u64) -> (Vec<usize>, Vec<RowSelector>) {
    let (mut groups, mut select) = (Vec::new(), Vec::new());
    let end = start + count;
    let mut first = 0u64;
    for (g, rg) in meta.row_groups().iter().enumerate() {
        let n = rg.num_rows().max(0) as u64;
        let (a, b) = (first, first + n);
        first = b;
        if b <= start || a >= end || n == 0 {
            continue;
        }
        let (s, e) = (start.max(a) - a, end.min(b) - a);
        groups.push(g);
        if s > 0 {
            select.push(RowSelector::skip(s as usize));
        }
        select.push(RowSelector::select((e - s) as usize));
        if e < n {
            select.push(RowSelector::skip((n - e) as usize));
        }
    }
    (groups, select)
}

/// Read rows `start` to `start + count` of the `n` top-level fields at
/// `fields` (ascending, each once), each as `modes` says; then
/// `pq_directory` says where each one's buffers are. Returns 0, or -1 with
/// the error's message as the result.
///
/// # Safety
/// `fields` and `modes` hold `n` values each.
#[no_mangle]
pub unsafe extern "C" fn pq_read(fields: *const u32, modes: *const u8, n: usize, start: f64, count: f64) -> f64 {
    let fields: Vec<usize> = std::slice::from_raw_parts(fields, n).iter().map(|&f| f as usize).collect();
    let modes = std::slice::from_raw_parts(modes, n).to_vec();
    with_state(|state| {
        state.outs.clear();
        state.directory.clear();
        if state.arrow.is_none() {
            // The Parquet types alone (ARROW:schema could change units),
            // with text stored as a dictionary in every row group read as
            // one: each distinct value is then copied out once.
            let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
            let plain = ArrowReaderMetadata::try_new(state.meta.clone(), options.clone()).map_err(|e| e.to_string())?;
            let descr = state.meta.file_metadata().schema_descr();
            let coded = |root: usize| {
                let leaf = (0..descr.num_columns()).find(|&l| descr.get_column_root_idx(l) == root);
                leaf.is_some_and(|l| state.meta.row_groups().iter().all(|g| g.column(l).dictionary_page_offset().is_some()))
            };
            let fields: Vec<_> = plain
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(k, f)| match f.data_type() {
                    DataType::Utf8 if coded(k) => Arc::new(
                        f.as_ref().clone().with_data_type(DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))),
                    ),
                    _ => f.clone(),
                })
                .collect();
            let schema = Arc::new(arrow_schema::Schema::new(fields));
            state.arrow = Some(ArrowReaderMetadata::try_new(state.meta.clone(), options.with_schema(schema)).map_err(|e| e.to_string())?);
        }
        let arrow = state.arrow.clone().expect("set above");
        let schema = state.meta.file_metadata().schema_descr();
        let names: Vec<&str> = fields.iter().map(|&f| schema.root_schema().get_fields()[f].name()).collect();
        let (start, count) = (start as u64, count as u64);
        let mut outs: Vec<Out> = modes
            .iter()
            .map(|&m| match m {
                NUMBER => Out::Number { values: Vec::with_capacity(count as usize), codes: Vec::with_capacity(count as usize) },
                BIGINT => Out::BigInt { values: Vec::with_capacity(count as usize), codes: Vec::with_capacity(count as usize) },
                _ => Out::Text { bytes: Vec::new(), ends: Vec::new(), missing: Vec::new(), utf16: None, codes: None },
            })
            .collect();
        let (groups, select) = selection(&state.meta, start, count);
        if !groups.is_empty() && count > 0 {
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(state.file, arrow)
                .with_projection(ProjectionMask::roots(schema, fields.iter().copied()))
                .with_row_groups(groups)
                .with_row_selection(RowSelection::from(select))
                .with_batch_size(count.clamp(1, 1 << 16) as usize)
                .build()
                .map_err(|e| e.to_string())?;
            let mut row = start;
            for batch in reader {
                let batch = batch.map_err(|e| e.to_string())?;
                for (k, out) in outs.iter_mut().enumerate() {
                    let a = batch.column(k).as_ref();
                    let col = Column { name: names[k], row };
                    match out {
                        Out::Number { values, codes } => add_numbers(a, values, codes, &col)?,
                        Out::BigInt { values, codes } => add_bigints(a, values, codes, &col)?,
                        Out::Text { bytes, ends, missing, codes, .. } => add_bytes(a, bytes, ends, missing, codes, &col)?,
                    }
                }
                row += batch.num_rows() as u64;
            }
        }
        // The directory: per column, its kind and five (pointer, length)
        // pairs; lengths count elements.
        let p = |v: *const u8| v as usize as u32;
        for (k, out) in outs.iter_mut().enumerate() {
            let mut d = [0u32; 11];
            match out {
                Out::Number { values, codes } => {
                    d[1..5].copy_from_slice(&[p(values.as_ptr().cast()), values.len() as u32, p(codes.as_ptr()), codes.len() as u32])
                }
                Out::BigInt { values, codes } => {
                    d[1..5].copy_from_slice(&[p(values.as_ptr().cast()), values.len() as u32, p(codes.as_ptr()), codes.len() as u32])
                }
                Out::Text { bytes, ends, missing, utf16, codes } => {
                    if modes[k] == TEXT {
                        *utf16 = utf16_ends(bytes, ends);
                    }
                    let (up, ul) = utf16.as_ref().map_or((0, 0), |u| (p(u.as_ptr().cast()), u.len() as u32));
                    let (cp, cl) = codes.as_ref().map_or((0, 0), |c| (p(c.as_ptr().cast()), c.len() as u32));
                    d[1..11].copy_from_slice(&[
                        p(bytes.as_ptr()),
                        bytes.len() as u32,
                        p(ends.as_ptr().cast()),
                        ends.len() as u32,
                        p(missing.as_ptr()),
                        missing.len() as u32,
                        up,
                        ul,
                        cp,
                        cl,
                    ]);
                    if codes.is_some() {
                        d[0] = 4;
                    }
                }
            }
            d[0] = d[0].max(u32::from(modes[k]));
            state.directory.extend_from_slice(&d);
        }
        state.outs = outs;
        Ok(0.0)
    })
}

/// The last read's directory: eleven u32s per column (see `pq_read`).
#[no_mangle]
pub extern "C" fn pq_directory() -> *const u32 {
    STATE.with(|s| s.borrow().as_ref().map_or(std::ptr::null(), |st| st.directory.as_ptr()))
}

/// Free the last read's buffers.
#[no_mangle]
pub extern "C" fn pq_read_end() {
    STATE.with(|s| {
        if let Some(st) = s.borrow_mut().as_mut() {
            st.outs = Vec::new();
            st.directory = Vec::new();
        }
    })
}
