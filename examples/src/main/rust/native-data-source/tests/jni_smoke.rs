// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Schema};
use jni::objects::{JBooleanArray, JByteArray, JObject, JObjectArray, JString, JValue};
use jni::sys::{jlong, JNI_TRUE};
use jni::{InitArgsBuilder, JNIEnv, JNIVersion, JavaVM};

type TestResult<T> = Result<T, Box<dyn Error>>;
const BRIDGE: &str = "org/apache/spark/sql/datasource/NativeBridge";

struct Classes(PathBuf);

impl Drop for Classes {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Plan {
    state: Vec<u8>,
    partitions: Vec<Vec<u8>>,
}

fn string_array<'local>(
    env: &mut JNIEnv<'local>,
    values: &[&str],
) -> jni::errors::Result<JObjectArray<'local>> {
    let array = env.new_object_array(values.len() as i32, "java/lang/String", JObject::null())?;
    for (i, value) in values.iter().enumerate() {
        let value = env.new_string(value)?;
        env.set_object_array_element(&array, i as i32, value)?;
    }
    Ok(array)
}

fn source(env: &mut JNIEnv<'_>, options: &[(&str, &str)]) -> jni::errors::Result<jlong> {
    let name = env.new_string("rust_sdk_range")?;
    let keys = string_array(
        env,
        &options.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
    )?;
    let values = string_array(
        env,
        &options.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
    )?;
    env.call_static_method(
        BRIDGE,
        "createDataSource",
        "(Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;)J",
        &[
            JValue::Object(name.as_ref()),
            JValue::Object(keys.as_ref()),
            JValue::Object(values.as_ref()),
        ],
    )?
    .j()
}

fn plan(
    env: &mut JNIEnv<'_>,
    options: &[(&str, &str)],
    predicates: &[&str],
    accepted: &[bool],
    limit: Option<(i32, bool)>,
    projection: Option<&[&str]>,
) -> TestResult<Plan> {
    let source = source(env, options)?;
    assert_ne!(source, 0);
    let mut schema = FFI_ArrowSchema::empty();
    let schema_address = &mut schema as *mut FFI_ArrowSchema as jlong;
    env.call_static_method(
        BRIDGE,
        "schema",
        "(JJ)V",
        &[JValue::Long(source), JValue::Long(schema_address)],
    )?;
    let imported = Schema::try_from(&schema)?;
    assert_eq!(imported.field(0).name(), "id");
    assert_eq!(imported.field(0).data_type(), &DataType::Int64);
    assert_eq!(imported.field(1).name(), "label");
    let reader = env
        .call_static_method(
            BRIDGE,
            "createReader",
            "(JJ)J",
            &[JValue::Long(source), JValue::Long(schema_address)],
        )?
        .j()?;
    assert_ne!(reader, 0);
    drop(schema);
    // Reader state and imported schema must not borrow the source handle.
    env.call_static_method(BRIDGE, "closeDataSource", "(J)V", &[JValue::Long(source)])?;

    let predicates = string_array(env, predicates)?;
    let result = env
        .call_static_method(
            BRIDGE,
            "pushPredicates",
            "(J[Ljava/lang/String;)[Z",
            &[JValue::Long(reader), JValue::Object(predicates.as_ref())],
        )?
        .l()?;
    let result = JBooleanArray::from(result);
    assert_eq!(env.get_array_length(&result)? as usize, accepted.len());
    let mut booleans = vec![0; accepted.len()];
    env.get_boolean_array_region(&result, 0, &mut booleans)?;
    assert_eq!(
        booleans
            .iter()
            .map(|value| *value == JNI_TRUE)
            .collect::<Vec<_>>(),
        accepted
    );
    if let Some((limit, complete)) = limit {
        assert_eq!(
            env.call_static_method(
                BRIDGE,
                "pushLimit",
                "(JI)Z",
                &[JValue::Long(reader), JValue::Int(limit)],
            )?
            .z()?,
            complete
        );
    }
    if let Some(projection) = projection {
        let columns = string_array(env, projection)?;
        assert!(env
            .call_static_method(
                BRIDGE,
                "pruneColumns",
                "(J[Ljava/lang/String;)Z",
                &[JValue::Long(reader), JValue::Object(columns.as_ref())],
            )?
            .z()?);
    }
    let partitions = JObjectArray::from(
        env.call_static_method(BRIDGE, "partitions", "(J)[[B", &[JValue::Long(reader)])?
            .l()?,
    );
    let mut partition_bytes = Vec::new();
    for i in 0..env.get_array_length(&partitions)? {
        let partition = JByteArray::from(env.get_object_array_element(&partitions, i)?);
        partition_bytes.push(env.convert_byte_array(&partition)?);
    }
    let state = JByteArray::from(
        env.call_static_method(BRIDGE, "serializeReader", "(J)[B", &[JValue::Long(reader)])?
            .l()?,
    );
    let state = env.convert_byte_array(&state)?;
    // Executor reads below only have the serialized bytes, just as on another JVM.
    env.call_static_method(BRIDGE, "closeReader", "(J)V", &[JValue::Long(reader)])?;
    Ok(Plan {
        state,
        partitions: partition_bytes,
    })
}

fn open_stream(
    env: &mut JNIEnv<'_>,
    plan: &Plan,
    partition: &[u8],
) -> TestResult<ArrowArrayStreamReader> {
    let state = env.byte_array_from_slice(&plan.state)?;
    let partition = env.byte_array_from_slice(partition)?;
    let mut stream = FFI_ArrowArrayStream::empty();
    env.call_static_method(
        BRIDGE,
        "read",
        "([B[BJ)V",
        &[
            JValue::Object(state.as_ref()),
            JValue::Object(partition.as_ref()),
            JValue::Long(&mut stream as *mut FFI_ArrowArrayStream as jlong),
        ],
    )?;
    Ok(ArrowArrayStreamReader::try_new(stream)?)
}

fn exercise_bridge(env: &mut JNIEnv<'_>, library: &Path) -> TestResult<()> {
    let library = env.new_string(library.to_str().ok_or("Non-UTF-8 library path")?)?;
    env.call_static_method(
        BRIDGE,
        "load",
        "(Ljava/lang/String;)V",
        &[JValue::Object(library.as_ref())],
    )?;
    assert_eq!(
        env.call_static_method(BRIDGE, "abiVersion", "()I", &[])?
            .i()?,
        1
    );

    let options = [
        ("start", "10"),
        ("end", "25"),
        ("numpartitions", "3"),
        ("batchsize", "2"),
    ];
    let lower = concat!(
        r#"{"type":"function","name":">=","children":["#,
        r#"{"type":"column","name":["id"]},"#,
        r#"{"type":"literal","dataType":"bigint","value":13}]}"#
    );
    let upper = concat!(
        r#"{"type":"function","name":"<","children":["#,
        r#"{"type":"column","name":["id"]},"#,
        r#"{"type":"literal","dataType":"bigint","value":22}]}"#
    );
    let label = concat!(
        r#"{"type":"function","name":"=","children":["#,
        r#"{"type":"column","name":["label"]},"#,
        r#"{"type":"literal","dataType":"string","value":"row-21"}]}"#
    );
    let filtered = plan(
        env,
        &options,
        &[lower, upper],
        &[true, true],
        Some((5, true)),
        Some(&["label", "id"]),
    )?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for partition in &filtered.partitions {
        let reader = open_stream(env, &filtered, partition)?;
        // Keep all batches alive across later get_next calls and stream destruction.
        batches.extend(reader.collect::<Result<Vec<_>, _>>()?);
    }
    assert!(batches.len() >= 3);
    let mut ids = Vec::new();
    for batch in &batches {
        assert!(batch.num_rows() <= 2);
        assert_eq!(batch.schema().field(0).name(), "label");
        assert_eq!(batch.schema().field(1).name(), "id");
        let labels = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let id = values.value(i);
            assert_eq!(labels.value(i), format!("row-{id}"));
            ids.push(id);
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![13, 14, 15, 16, 17]);

    // Spark must evaluate the label predicate before LIMIT; keep all candidate rows.
    let residual = plan(
        env,
        &options,
        &[lower, upper, label],
        &[true, true, false],
        Some((1, false)),
        Some(&["id"]),
    )?;
    let mut candidates = Vec::new();
    for partition in &residual.partitions {
        for batch in open_stream(env, &residual, partition)? {
            let batch = batch?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            candidates.extend_from_slice(ids.values());
        }
    }
    candidates.sort_unstable();
    assert_eq!(candidates, (13..22).collect::<Vec<i64>>());

    let unconsumed = plan(
        env,
        &[("end", "20"), ("numpartitions", "1"), ("batchsize", "2")],
        &[],
        &[],
        None,
        None,
    )?;
    let mut stream = open_stream(env, &unconsumed, &unconsumed.partitions[0])?;
    let first = stream.next().ok_or("Missing first batch")??;
    drop(stream);
    let first_ids = first
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(first_ids.values().as_ref(), &[0, 1]);

    let empty_projection = plan(
        env,
        &[("end", "7"), ("numpartitions", "2"), ("batchsize", "2")],
        &[],
        &[],
        None,
        Some(&[]),
    )?;
    let mut count = 0;
    for partition in &empty_projection.partitions {
        for batch in open_stream(env, &empty_projection, partition)? {
            let batch = batch?;
            assert_eq!(batch.num_columns(), 0);
            count += batch.num_rows();
        }
    }
    assert_eq!(count, 7);

    let error = source(env, &[("batchsize", "0")]).expect_err("Expected a Java exception");
    assert!(matches!(error, jni::errors::Error::JavaException));
    let exception = env.exception_occurred()?;
    env.exception_clear()?;
    assert!(env.is_instance_of(&exception, "java/lang/RuntimeException")?);
    let message = JString::from(
        env.call_method(exception, "toString", "()Ljava/lang/String;", &[])?
            .l()?,
    );
    let message: String = env.get_string(&message)?.into();
    assert!(message.contains("batchSize must be positive"), "{message}");
    Ok(())
}

#[test]
fn jni_arrow_round_trip() -> TestResult<()> {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let classes = Classes(
        std::env::temp_dir().join(format!("spark-native-jni-{}-{unique}", std::process::id())),
    );
    fs::create_dir(&classes.0)?;
    let javac = std::env::var_os("JAVA_HOME")
        .map(|home| {
            PathBuf::from(home)
                .join("bin")
                .join(format!("javac{}", std::env::consts::EXE_SUFFIX))
        })
        .unwrap_or_else(|| PathBuf::from("javac"));
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/java/org/apache/spark/sql/datasource/NativeBridge.java");
    let compiled = Command::new(javac)
        .arg("-d")
        .arg(&classes.0)
        .arg(fixture)
        .output()?;
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );

    let executable = std::env::current_exe()?;
    let profile_dir = executable
        .parent()
        .and_then(Path::parent)
        .ok_or("Missing profile directory")?;
    let library = profile_dir.join(format!(
        "{}spark_datasource_rust_sdk_range{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    assert!(
        library.is_file(),
        "Run cargo build --workspace first: {}",
        library.display()
    );
    let class_path = format!("-Djava.class.path={}", classes.0.display());
    let args = InitArgsBuilder::new()
        .version(JNIVersion::V8)
        .option(&class_path)
        .option("-Xcheck:jni")
        .build()?;
    let jvm = JavaVM::new(args)?;
    let mut env = jvm.attach_current_thread()?;
    env.ensure_local_capacity(256)?;
    let result = exercise_bridge(&mut env, &library);
    if env.exception_check()? {
        env.exception_describe()?;
        env.exception_clear()?;
    }
    result
}
