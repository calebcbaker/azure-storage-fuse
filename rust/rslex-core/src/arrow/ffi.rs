use arrow::{
    array::ArrayDataRef,
    datatypes::{DataType, DateUnit, IntervalUnit, TimeUnit},
};
use std::{
    borrow::Cow,
    ffi::CString,
    mem::drop,
    os::raw::{c_char, c_longlong, c_void},
    ptr,
};

// See: https://arrow.apache.org/docs/format/CDataInterface.html
const ARROW_FLAG_NULLABLE: c_longlong = 2;

pub const EMPTY_STRING: &[u8; 1] = b"\x00";
pub const NULL_FORMAT: &[u8; 2] = b"n\x00";
pub const BOOL_FORMAT: &[u8; 2] = b"b\x00";
pub const I8_FORMAT: &[u8; 2] = b"c\x00";
pub const U8_FORMAT: &[u8; 2] = b"C\x00";
pub const I16_FORMAT: &[u8; 2] = b"s\x00";
pub const U16_FORMAT: &[u8; 2] = b"S\x00";
pub const I32_FORMAT: &[u8; 2] = b"i\x00";
pub const U32_FORMAT: &[u8; 2] = b"I\x00";
pub const I64_FORMAT: &[u8; 2] = b"l\x00";
pub const U64_FORMAT: &[u8; 2] = b"L\x00";
pub const F16_FORMAT: &[u8; 2] = b"e\x00";
pub const F32_FORMAT: &[u8; 2] = b"f\x00";
pub const F64_FORMAT: &[u8; 2] = b"g\x00";
pub const BINARY_FORMAT: &[u8; 2] = b"z\x00";
pub const LARGE_BINARY_FORMAT: &[u8; 2] = b"Z\x00";
pub const UTF_FORMAT: &[u8; 2] = b"u\x00";
pub const LARGE_UTF_FORMAT: &[u8; 2] = b"U\x00";
pub const DATE32_FORMAT: &[u8; 4] = b"tdD\x00";
pub const DATE64_FORMAT: &[u8; 4] = b"tdm\x00";
pub const TIME32_SECONDS_FORMAT: &[u8; 4] = b"tts\x00";
pub const TIME32_MILLISECONDS_FORMAT: &[u8; 4] = b"ttm\x00";
pub const TIME64_MICROSECONDS_FORMAT: &[u8; 4] = b"ttu\x00";
pub const TIME64_NANOSECONDS_FORMAT: &[u8; 4] = b"ttn\x00";
pub const DURATION_SECONDS_FORMAT: &[u8; 4] = b"tDs\x00";
pub const DURATION_MILLISECONDS_FORMAT: &[u8; 4] = b"tDm\x00";
pub const DURATION_MICROSECONDS_FORMAT: &[u8; 4] = b"tDu\x00";
pub const DURATION_NANOSECONDS_FORMAT: &[u8; 4] = b"tDn\x00";
pub const INTERVAL_MONTHS_FORMAT: &[u8; 4] = b"tiM\x00";
pub const INTERVAL_DAYTIME_FORMAT: &[u8; 4] = b"tiD\x00";
pub const STRUCT_FORMAT: &[u8; 3] = b"+s\x00";
pub const LIST_FORMAT: &[u8; 3] = b"+l\x00";
pub const LARGE_LIST_FORMAT: &[u8; 3] = b"+L\x00";

fn to_c_array<T>(vec: Vec<T>) -> (*mut *mut T, usize) {
    if vec.is_empty() {
        (ptr::null_mut(), 0)
    } else {
        let mut vec: Vec<_> = vec.into_iter().map(|i| Box::into_raw(Box::new(i))).collect();
        vec.shrink_to_fit();
        let (ptr, length, capacity) = vec.into_raw_parts();
        assert_eq!(length, capacity);
        (ptr, length)
    }
}

unsafe fn from_c_array<T>(array: *mut *mut T, length: usize) -> Option<Vec<Box<T>>> {
    if length == 0 {
        None
    } else {
        Some(
            Vec::from_raw_parts(array, length, length)
                .into_iter()
                .map(|i| Box::from_raw(i))
                .collect(),
        )
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct CDataSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: c_longlong,
    pub n_children: c_longlong,
    pub children: *mut *mut CDataSchema,
    pub dictionary: *mut CDataSchema,
    pub release: Option<unsafe extern "C" fn(schema: *mut CDataSchema)>,
    pub private_data: *mut c_void,
}

impl CDataSchema {
    pub fn new(
        format: *const c_char,
        name: Option<&str>,
        metadata: Option<&str>,
        flags: c_longlong,
        children: Vec<CDataSchema>,
    ) -> CDataSchema {
        let (children_ptr, children_length) = to_c_array(children);
        CDataSchema {
            format,
            name: name.map_or(ptr::null(), |n| CString::new(n).unwrap().into_raw()),
            metadata: metadata.map_or(ptr::null(), |m| CString::new(m).unwrap().into_raw()),
            flags,
            n_children: children_length as c_longlong,
            children: children_ptr,
            dictionary: ptr::null_mut(),
            release: Some(release_schema),
            private_data: ptr::null_mut(),
        }
    }
}

impl Drop for CDataSchema {
    fn drop(&mut self) {
        unsafe {
            if !self.name.is_null() {
                drop(CString::from_raw(self.name as *mut _));
            }

            if !self.metadata.is_null() {
                drop(CString::from_raw(self.metadata as *mut _));
            }

            drop(from_c_array(self.children, self.n_children as usize));
            self.release = None;
        }
    }
}

unsafe extern "C" fn release_schema(schema: *mut CDataSchema) {
    ptr::drop_in_place(schema);
}

fn get_type_id(data_type: &DataType) -> Cow<'static, [u8]> {
    match data_type {
        DataType::Null => Cow::from(NULL_FORMAT as &[u8]),
        DataType::Boolean => Cow::from(BOOL_FORMAT as &[u8]),
        DataType::Int8 => Cow::from(I8_FORMAT as &[u8]),
        DataType::Int16 => Cow::from(I16_FORMAT as &[u8]),
        DataType::Int32 => Cow::from(I32_FORMAT as &[u8]),
        DataType::Int64 => Cow::from(I64_FORMAT as &[u8]),
        DataType::UInt8 => Cow::from(U8_FORMAT as &[u8]),
        DataType::UInt16 => Cow::from(U16_FORMAT as &[u8]),
        DataType::UInt32 => Cow::from(U32_FORMAT as &[u8]),
        DataType::UInt64 => Cow::from(U64_FORMAT as &[u8]),
        DataType::Float16 => Cow::from(F16_FORMAT as &[u8]),
        DataType::Float32 => Cow::from(F32_FORMAT as &[u8]),
        DataType::Float64 => Cow::from(F64_FORMAT as &[u8]),
        DataType::Decimal(precision, scale) => Cow::from(format!("d:{},{}", precision, scale).into_bytes()),
        DataType::Timestamp(unit, tz) => {
            let format_string = match unit {
                TimeUnit::Second => format!("tss:{}", tz.as_ref().map(|v| v.as_str()).unwrap_or("")),
                TimeUnit::Millisecond => format!("tsm:{}", tz.as_ref().map(|v| v.as_str()).unwrap_or("")),
                TimeUnit::Microsecond => format!("tsu:{}", tz.as_ref().map(|v| v.as_str()).unwrap_or("")),
                TimeUnit::Nanosecond => format!("tsn:{}", tz.as_ref().map(|v| v.as_str()).unwrap_or("")),
            };
            Cow::from(format_string.into_bytes())
        },
        DataType::Date32(unit) => match unit {
            DateUnit::Day => Cow::from(DATE32_FORMAT as &[u8]),
            DateUnit::Millisecond => panic!("Date32 values should have day resolution."),
        },
        DataType::Date64(unit) => match unit {
            DateUnit::Day => panic!("Date64 values should have millisecond resolution."),
            DateUnit::Millisecond => Cow::from(DATE64_FORMAT as &[u8]),
        },
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => Cow::from(TIME32_SECONDS_FORMAT as &[u8]),
            TimeUnit::Millisecond => Cow::from(TIME32_MILLISECONDS_FORMAT as &[u8]),
            TimeUnit::Microsecond => panic!("Time32 values should have second or millisecond resolution."),
            TimeUnit::Nanosecond => panic!("Time32 values should have second or millisecond resolution."),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Second => panic!("Time64 values should have microsecond or nanosecond resolution."),
            TimeUnit::Millisecond => panic!("Time64 values should have microsecond or nanosecond resolution."),
            TimeUnit::Microsecond => Cow::from(TIME64_MICROSECONDS_FORMAT as &[u8]),
            TimeUnit::Nanosecond => Cow::from(TIME64_NANOSECONDS_FORMAT as &[u8]),
        },
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => Cow::from(INTERVAL_MONTHS_FORMAT as &[u8]),
            IntervalUnit::DayTime => Cow::from(INTERVAL_DAYTIME_FORMAT as &[u8]),
        },
        DataType::Utf8 => Cow::from(UTF_FORMAT as &[u8]),
        DataType::LargeUtf8 => Cow::from(LARGE_UTF_FORMAT as &[u8]),
        DataType::Binary => Cow::from(BINARY_FORMAT as &[u8]),
        DataType::LargeBinary => Cow::from(LARGE_BINARY_FORMAT as &[u8]),
        DataType::FixedSizeBinary(l) => Cow::from(format!("w{}", l).into_bytes()),
        DataType::List(_) => Cow::from(LIST_FORMAT as &[u8]),
        DataType::LargeList(_) => Cow::from(LARGE_LIST_FORMAT as &[u8]),
        DataType::FixedSizeList(_, size) => Cow::from(format!("+w:{}", size).into_bytes()),
        DataType::Struct(_) => Cow::from(STRUCT_FORMAT as &[u8]),
        DataType::Duration(unit) => {
            let format = match unit {
                TimeUnit::Second => DURATION_SECONDS_FORMAT,
                TimeUnit::Millisecond => DURATION_MILLISECONDS_FORMAT,
                TimeUnit::Microsecond => DURATION_MICROSECONDS_FORMAT,
                TimeUnit::Nanosecond => DURATION_NANOSECONDS_FORMAT,
            };
            Cow::from(format as &[u8])
        },
        DataType::Union(fields) => {
            let mut format_bytes = Vec::from(b"+ud:" as &[u8]);
            let mut is_first = true;
            for field in fields {
                if !is_first {
                    format_bytes.push(b',');
                }

                let nested_type = get_type_id(field.data_type());
                format_bytes.extend_from_slice(nested_type.as_ref());
                is_first = false;
            }

            Cow::from(format_bytes)
        },
        DataType::Dictionary(_, _) => panic!("[get_type_id()] FFI not implemented for Dictionary arrays."),
    }
}

fn create_children_vec(data_type: &DataType) -> Vec<CDataSchema> {
    match data_type {
        DataType::List(data_type) => vec![create_cdata_schema(data_type.data_type(), Some("item"))],
        DataType::LargeList(data_type) => vec![create_cdata_schema(data_type.data_type(), Some("item"))],
        DataType::FixedSizeList(data_type, _) => vec![create_cdata_schema(data_type.data_type(), Some("item"))],
        DataType::Struct(fields) => fields
            .iter()
            .map(|f| create_cdata_schema(f.data_type(), Some(f.name().as_str())))
            .collect(),
        DataType::Union(fields) => fields
            .iter()
            .map(|f| create_cdata_schema(f.data_type(), Some(f.name().as_str())))
            .collect(),
        DataType::Dictionary(_, _) => unimplemented!("[create_children_vec()] FFI not implemented for Dictionary arrays."),
        _ => Vec::new(),
    }
}

pub fn create_cdata_schema(data_type: &DataType, name: Option<&str>) -> CDataSchema {
    let format = match get_type_id(data_type) {
        Cow::Borrowed(bytes) => bytes.as_ptr() as *const c_char,
        Cow::Owned(bytes) => CString::new(bytes).unwrap().into_raw(),
    };
    let children = create_children_vec(data_type);
    CDataSchema::new(format, name, None, ARROW_FLAG_NULLABLE, children)
}

#[repr(C)]
#[derive(Debug)]
pub struct CDataArray {
    pub length: c_longlong,
    pub null_count: c_longlong,
    pub offset: c_longlong,
    pub n_buffers: c_longlong,
    pub n_children: c_longlong,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut CDataArray,
    pub dictionary: *mut CDataArray,
    pub release: Option<unsafe extern "C" fn(array: *mut CDataArray)>,
    pub private_data: *mut c_void,
}

impl Drop for CDataArray {
    fn drop(&mut self) {
        unsafe {
            // We do not release the actual buffers, those are owned by the actual RecordBatch.
            // We only release the array of buffers;
            if self.n_buffers > 0 {
                drop(Vec::from_raw_parts(self.buffers, self.n_buffers as usize, self.n_buffers as usize));
            }

            drop(from_c_array(self.children, self.n_children as usize));
            drop(Box::from_raw(self.private_data as *mut ArrayDataRef));

            self.release = None;
        }
    }
}

unsafe extern "C" fn release_array(array: *mut CDataArray) {
    ptr::drop_in_place(array);
}

pub fn create_cdata_array(array: ArrayDataRef) -> CDataArray {
    let buffers = array.buffers();
    let mut buffers_array: Vec<_> = std::iter::once(array.null_buffer().map_or(ptr::null(), |b| b.as_ptr() as *const c_void))
        .chain(buffers.iter().map(|b| b.as_ptr() as *const c_void))
        .collect();
    buffers_array.shrink_to_fit();
    let (buffers_ptr, buffers_length, buffers_capacity) = buffers_array.into_raw_parts();
    assert_eq!(buffers_length, buffers_capacity);

    let children = array.child_data();
    let mut children_array: Vec<_> = children
        .iter()
        .map(|c| Box::into_raw(Box::new(create_cdata_array(c.clone()))))
        .collect();
    children_array.shrink_to_fit();
    let (children_ptr, children_length, children_capacity) = if !children.is_empty() {
        children_array.into_raw_parts()
    } else {
        (ptr::null_mut(), 0, 0)
    };
    assert_eq!(children_length, children_capacity);

    CDataArray {
        length: array.len() as c_longlong,
        null_count: array.null_count() as c_longlong,
        offset: array.offset() as c_longlong,
        n_buffers: buffers_length as c_longlong,
        n_children: children_length as c_longlong,
        buffers: buffers_ptr,
        children: children_ptr,
        dictionary: ptr::null_mut(),
        release: Some(release_array),
        private_data: Box::into_raw(Box::new(array)) as *mut c_void,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, UInt32Builder},
        datatypes::Field,
    };
    use fluent_assertions::*;
    use std::{ffi::CStr, sync::Arc};

    #[test]
    fn create_cdata_array_with_no_children_returns_correct_properties() {
        let mut source_array_builder = UInt32Builder::new(2);
        source_array_builder.append_value(1).unwrap();
        source_array_builder.append_null().unwrap();
        let source_array = source_array_builder.finish();

        let cdata = create_cdata_array(source_array.data());

        cdata.length.should().equal(&(source_array.len() as c_longlong));
        cdata.offset.should().equal(&(source_array.offset() as c_longlong));
        cdata.null_count.should().equal(&(source_array.null_count() as c_longlong));
        cdata.n_children.should().equal(&0);
        cdata
            .n_buffers
            .should()
            .equal(&(source_array.data_ref().buffers().len() as c_longlong + 1)); // Add 1 to account for null buffer
        cdata.children.should().equal(&(ptr::null_mut::<*mut CDataArray>()));
    }

    #[test]
    fn cdata_array_release_decreases_array_ref_count() {
        let data_weakref = {
            let mut source_array_builder = UInt32Builder::new(2);
            source_array_builder.append_value(1).unwrap();
            source_array_builder.append_null().unwrap();
            let source_array = source_array_builder.finish();

            let data = source_array.data();
            let weak = Arc::downgrade(&data);
            let _ = create_cdata_array(data);
            weak
        };

        data_weakref.strong_count().should().equal(&0);
    }

    fn create_cdata_schema_type_with_no_children_returns_correct_schema(data_type: &DataType, expected_format: &str) {
        let schema = create_cdata_schema(data_type, None);
        schema.n_children.should().equal(&0);
        schema.children.should().equal(&ptr::null_mut());
        schema.metadata.should().equal(&ptr::null());
        schema.dictionary.should().equal(&ptr::null_mut());
        schema.flags.should().equal(&ARROW_FLAG_NULLABLE);
        unsafe {
            CStr::from_ptr(schema.format)
                .should()
                .equal(&CString::new(expected_format).unwrap().as_c_str());
        }
    }

    #[test]
    fn create_cdata_schema_for_bool() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Boolean, "b");
    }

    #[test]
    fn create_cdata_schema_for_int8() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Int8, "c");
    }

    #[test]
    fn create_cdata_schema_for_uint8() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::UInt8, "C");
    }

    #[test]
    fn create_cdata_schema_for_int16() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Int16, "s");
    }

    #[test]
    fn create_cdata_schema_for_uint16() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::UInt16, "S");
    }

    #[test]
    fn create_cdata_schema_for_int32() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Int32, "i");
    }

    #[test]
    fn create_cdata_schema_for_uint32() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::UInt32, "I");
    }

    #[test]
    fn create_cdata_schema_for_int64() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Int64, "l");
    }

    #[test]
    fn create_cdata_schema_for_uint64() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::UInt64, "L");
    }

    #[test]
    fn create_cdata_schema_for_float16() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Float16, "e");
    }

    #[test]
    fn create_cdata_schema_for_float32() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Float32, "f");
    }

    #[test]
    fn create_cdata_schema_for_float64() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Float64, "g");
    }

    #[test]
    fn create_cdata_schema_for_utf() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Utf8, "u");
    }

    #[test]
    fn create_cdata_schema_for_large_utf() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::LargeUtf8, "U");
    }

    #[test]
    fn create_cdata_schema_for_binary() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Binary, "z");
    }

    #[test]
    fn create_cdata_schema_for_large_binary() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::LargeBinary, "Z");
    }

    #[test]
    fn create_cdata_schema_for_second_timestamp_no_tz() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Timestamp(TimeUnit::Second, None), "tss:");
    }

    #[test]
    fn create_cdata_schema_for_millisecond_timestamp_no_tz() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Timestamp(TimeUnit::Millisecond, None), "tsm:");
    }

    #[test]
    fn create_cdata_schema_for_microsecond_timestamp_no_tz() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Timestamp(TimeUnit::Microsecond, None), "tsu:");
    }

    #[test]
    fn create_cdata_schema_for_nanosecond_timestamp_no_tz() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Timestamp(TimeUnit::Nanosecond, None), "tsn:");
    }

    #[test]
    fn create_cdata_schema_for_date32_day() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Date32(DateUnit::Day), "tdD");
    }

    #[test]
    #[should_panic]
    fn create_cdata_schema_for_date32_millisecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Date32(DateUnit::Millisecond), "");
    }

    #[test]
    #[should_panic]
    fn create_cdata_schema_for_date64_day() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Date64(DateUnit::Day), "");
    }

    #[test]
    fn create_cdata_schema_for_date64_millisecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Date64(DateUnit::Millisecond), "tdm");
    }

    #[test]
    fn create_cdata_schema_for_time32_second() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Time32(TimeUnit::Second), "tts");
    }

    #[test]
    fn create_cdata_schema_for_time32_millisecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Time32(TimeUnit::Millisecond), "ttm");
    }

    #[test]
    #[should_panic]
    fn create_cdata_schema_for_time32_microsecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Time64(TimeUnit::Microsecond), "");
    }

    #[test]
    #[should_panic]
    fn create_cdata_schema_for_time32_nanosecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Time64(TimeUnit::Nanosecond), "");
    }

    #[test]
    #[should_panic]
    fn create_cdata_schema_for_time64_second() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Time32(TimeUnit::Second), "");
    }

    #[test]
    #[should_panic]
    fn create_cdata_schema_for_time64_millisecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Time32(TimeUnit::Millisecond), "");
    }

    #[test]
    fn create_cdata_schema_for_time64_microsecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Time64(TimeUnit::Microsecond), "ttu");
    }

    #[test]
    fn create_cdata_schema_for_time64_nanosecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Time64(TimeUnit::Nanosecond), "ttn");
    }

    #[test]
    fn create_cdata_schema_for_interval_year_month() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Interval(IntervalUnit::YearMonth), "tiM");
    }

    #[test]
    fn create_cdata_schema_for_interval_day_time() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Interval(IntervalUnit::DayTime), "tiD");
    }

    #[test]
    fn create_cdata_schema_for_duration_second() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Duration(TimeUnit::Second), "tDs");
    }

    #[test]
    fn create_cdata_schema_for_duration_millisecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Duration(TimeUnit::Millisecond), "tDm");
    }

    #[test]
    fn create_cdata_schema_for_duration_microsecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Duration(TimeUnit::Microsecond), "tDu");
    }

    #[test]
    fn create_cdata_schema_for_duration_nanosecond() {
        create_cdata_schema_type_with_no_children_returns_correct_schema(&DataType::Duration(TimeUnit::Nanosecond), "tDn");
    }

    #[test]
    fn create_cdata_schema_for_struct() {
        unsafe {
            let schema = create_cdata_schema(
                &DataType::Struct(vec![
                    Field::new(
                        "Child",
                        DataType::List(Box::new(Field::new(
                            "Nested List",
                            DataType::Struct(vec![Field::new("Nested List Element", DataType::LargeUtf8, true)]),
                            true,
                        ))),
                        true,
                    ),
                    Field::new("Child2", DataType::Boolean, true),
                ]),
                None,
            );
            schema.n_children.should().equal(&2);
            schema.metadata.should().equal(&ptr::null());
            schema.dictionary.should().equal(&ptr::null_mut());
            schema.flags.should().equal(&ARROW_FLAG_NULLABLE);
            CStr::from_ptr(schema.format)
                .should()
                .equal(&CString::new("+s").unwrap().as_c_str());

            let first_child = &**schema.children;
            CStr::from_ptr(first_child.format)
                .should()
                .equal(&CString::new("+l").unwrap().as_c_str());
        }
    }

    #[test]
    fn create_cdata_schema_for_list() {
        let schema = create_cdata_schema(&DataType::List(Box::new(Field::new("Child", DataType::UInt8, true))), None);
        unsafe {
            CStr::from_ptr(schema.format)
                .should()
                .equal(&CString::new("+l").unwrap().as_c_str());
            schema.n_children.should().equal(&1);

            let child = &**schema.children;
            child.n_children.should().equal(&0);
            CStr::from_ptr(child.name).should().equal(&CString::new("item").unwrap().as_c_str());
            CStr::from_ptr(child.format).should().equal(&CString::new("C").unwrap().as_c_str());
        }
    }

    #[test]
    fn create_cdata_schema_for_large_list() {
        let schema = create_cdata_schema(&DataType::LargeList(Box::new(Field::new("Child", DataType::UInt8, true))), None);
        unsafe {
            CStr::from_ptr(schema.format)
                .should()
                .equal(&CString::new("+L").unwrap().as_c_str());
            schema.n_children.should().equal(&1);

            let child = &**schema.children;
            child.n_children.should().equal(&0);
            CStr::from_ptr(child.name).should().equal(&CString::new("item").unwrap().as_c_str());
            CStr::from_ptr(child.format).should().equal(&CString::new("C").unwrap().as_c_str());
        }
    }

    #[test]
    fn create_cdata_schema_for_fixed_size_list() {
        let schema = create_cdata_schema(
            &DataType::FixedSizeList(Box::new(Field::new("Child", DataType::UInt8, true)), 5),
            None,
        );
        unsafe {
            CStr::from_ptr(schema.format)
                .should()
                .equal(&CString::new("+w:5").unwrap().as_c_str());
            schema.n_children.should().equal(&1);

            let child = &**schema.children;
            child.n_children.should().equal(&0);
            CStr::from_ptr(child.name).should().equal(&CString::new("item").unwrap().as_c_str());
            CStr::from_ptr(child.format).should().equal(&CString::new("C").unwrap().as_c_str());
        }
    }
}
