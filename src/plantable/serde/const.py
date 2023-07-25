MAP_ARROW_DTYPE_TO_SEATABLE_DTYPE = {
    "null": "text",  # Create instance of null type.
    "bool": "text",  # Create instance of boolean type.
    "int8": "text",  # Create instance of signed int8 type.
    "int16": "text",  # Create instance of signed int16 type.
    "int32": "text",  # Create instance of signed int32 type.
    "int64": "text",  # Create instance of signed int64 type.
    "uint8": "text",  # Create instance of unsigned int8 type.
    "uint16": "text",  # Create instance of unsigned uint16 type.
    "uint32": "text",  # Create instance of unsigned uint32 type.
    "uint64": "text",  # Create instance of unsigned uint64 type.
    "float16": "text",  # Create half-precision floating point type.
    "float32": "text",  # Create single-precision floating point type.
    "float64": "text",  # Create double-precision floating point type.
    "time32(unit)": "text",  # Create instance of 32-bit time (time of day) type with unit resolution.
    "time64(unit)": "text",  # Create instance of 64-bit time (time of day) type with unit resolution.
    "timestamp(unit[, tz])": "text",  # Create instance of timestamp type with resolution and optional time zone.
    "date32": "text",  # Create instance of 32-bit date (days since UNIX epoch 1970-01-01).
    "date64": "text",  # Create instance of 64-bit date (milliseconds since UNIX epoch 1970-01-01).
    "duration(unit)": "text",  # Create instance of a duration type with unit resolution.
    "month_day_nano_interval": "text",  # Create instance of an interval type representing months, days and nanoseconds between two dates.
    "binary(int length=-1)": "text",  # Create variable-length or fixed size binary type.
    "string": "text",  # Create UTF8 variable-length string type.
    "utf8": "text",  # Alias for string
    "large_binary": "text",  # Create large variable-length binary type.
    "large_string": "text",  # Create large UTF8 variable-length string type.
    "large_utf8": "text",  # Alias for large_string
    "decimal128(int precision, int scale=0)": "text",  # Create decimal type with precision and scale and 128-bit width.
    "list_(value_type, int list_size=-1)": "text",  # Create ListType instance from child data type or field.
}
