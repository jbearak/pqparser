"""Write test/fixtures/odd/: small files with footers other readers take
and arrow-rs refuses, each made by writing a file with pyarrow 26 and
changing bytes of its footer (Thrift compact protocol), from data made
here. Then `python3 scripts/expected.py test/fixtures/odd`.

    python3 scripts/make_odd.py
"""

import os
import struct

import pyarrow as pa
import pyarrow.parquet as pq

OUT = os.path.join(os.path.dirname(__file__), "..", "test", "fixtures", "odd")
os.makedirs(OUT, exist_ok=True)


def patch(name, table, old, new, times=1, **kw):
    """Write `table`, then replace the `times` copies of `old` in its footer with `new`."""
    path = os.path.join(OUT, name)
    pq.write_table(table, path, **kw)
    b = open(path, "rb").read()
    n = struct.unpack("<I", b[-8:-4])[0]
    footer = b[-8 - n : -8]
    assert footer.count(old) == times, (name, footer.count(old))
    footer = footer.replace(old, new)
    open(path, "wb").write(b[: -8 - n] + footer + struct.pack("<I", len(footer)) + b"PAR1")


# A DATE column, and a TIME column without a converted type, whose logical
# types are one arrow-rs does not know (union id 20, in long form).
patch(
    "unknown_logical_int.parquet",
    pa.table({"n": pa.array([1, 2, None], pa.int64()), "d": pa.array([0, 19_000, None], pa.date32())}),
    bytes([0x4C, 0x6C, 0x00, 0x00]),  # logicalType: DATE {}
    bytes([0x4C, 0x0C, 0x28, 0x00, 0x00]),  # logicalType: field 20 {}
)
patch(
    "unknown_logical_time.parquet",
    pa.table({"n": pa.array([1, 2, None], pa.int64()), "t": pa.array([0, 5_000_000_000, None], pa.time64("ns"))}),
    bytes([0x6C, 0x7C]),  # logicalType: TIME {...}
    bytes([0x6C, 0x0C, 0x28]),  # logicalType: field 20 {...}
)

# A text column "x", and a struct whose int8 member is also "x" and
# annotated UUID, which does not fit INT32.
patch(
    "misfit_same_name.parquet",
    pa.table({"x": ["a", None, "c"], "nest": pa.array([{"x": 1}, {"x": 2}, None], pa.struct([("x", pa.int8())]))}),
    bytes([0xAC, 0x13, 0x08, 0x11, 0x00, 0x00]),  # logicalType: INTEGER {8, signed}
    bytes([0xEC, 0x00, 0x00]),  # logicalType: UUID {}
)

# The same in two structs, which give arrow-rs's same error twice.
patch(
    "misfit_twice.parquet",
    pa.table({
        "x": ["a", None, "c"],
        "one": pa.array([{"x": 1}, {"x": 2}, None], pa.struct([("x", pa.int8())])),
        "two": pa.array([{"x": 3}, None, {"x": 5}], pa.struct([("x", pa.int8())])),
    }),
    bytes([0xAC, 0x13, 0x08, 0x11, 0x00, 0x00]),
    bytes([0xEC, 0x00, 0x00]),
    times=2,
)

# An int8 column whose name has an apostrophe, annotated UUID.
patch(
    "misfit_quote.parquet",
    pa.table({"x'y": pa.array([1, None, 3], pa.int8()), "z": [1.5, 2.5, None]}),
    bytes([0xAC, 0x13, 0x08, 0x11, 0x00, 0x00]),
    bytes([0xEC, 0x00, 0x00]),
)

# 70 structs whose int8 member "x" arrow-rs takes, then one whose "x" is
# annotated UUID, beside a flat column.
patch(
    "misfit_after_many.parquet",
    pa.table({
        "flat": [1.5, None, 3.5],
        **{f"s{k:02}": pa.array([{"x": k}] * 3, pa.struct([("x", pa.int16())])) for k in range(70)},
        "bad": pa.array([{"x": 1}, None, {"x": 3}], pa.struct([("x", pa.int8())])),
    }),
    bytes([0xAC, 0x13, 0x08, 0x11, 0x00, 0x00]),  # logicalType: INTEGER {8, signed}, only on bad.x
    bytes([0xEC, 0x00, 0x00]),
)

# A FIXED_LEN_BYTE_ARRAY(4) annotated UUID, named with " for field '" in it.
patch(
    "uuid_odd_name.parquet",
    pa.table({"x for field 'y": pa.array([bytes(range(k, k + 4)) for k in range(3)], pa.binary(4))}),
    bytes([0x18, 0x0E]) + b"x for field 'y" + bytes([0x00]),
    bytes([0x18, 0x0E]) + b"x for field 'y" + bytes([0x6C, 0xEC, 0x00, 0x00, 0x00]),
)

# A FIXED_LEN_BYTE_ARRAY(4) annotated UUID, which needs 16 bytes.
patch(
    "uuid_four_bytes.parquet",
    pa.table({"uid": pa.array([bytes(range(k, k + 4)) for k in range(3)], pa.binary(4))}),
    bytes([0x18, 0x03]) + b"uid" + bytes([0x00]),  # name "uid", end of element
    bytes([0x18, 0x03]) + b"uid" + bytes([0x6C, 0xEC, 0x00, 0x00, 0x00]),  # logicalType: UUID {}
)
