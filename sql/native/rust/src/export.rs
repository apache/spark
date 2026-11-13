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

/// Exports one batch-read provider as the JNI methods of NativeBridge ABI v1.
///
/// Call this macro once in a `cdylib`. JNI and Arrow pointer handling stay in
/// the SDK; providers implement the safe [`crate::BatchSource`] trait.
#[macro_export]
macro_rules! export_data_source {
    ($source:ty) => {
        mod __spark_native_datasource_exports {
            use super::*;
            use $crate::bridge::jni::objects::{JByteArray, JClass, JObjectArray, JString};
            use $crate::bridge::jni::sys::{
                jboolean, jbooleanArray, jbyteArray, jint, jlong, jobjectArray,
            };
            use $crate::bridge::jni::JNIEnv;
            use $crate::bridge::{self, Bridge};

            fn bridge() -> &'static Bridge<$source> {
                static BRIDGE: std::sync::OnceLock<Bridge<$source>> = std::sync::OnceLock::new();
                BRIDGE.get_or_init(Bridge::default)
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_abiVersion"]
            pub extern "system" fn abi_version(_env: JNIEnv<'_>, _class: JClass<'_>) -> jint {
                1
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_createDataSource"]
            pub extern "system" fn create_data_source(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                _name: JString<'_>,
                keys: JObjectArray<'_>,
                values: JObjectArray<'_>,
            ) -> jlong {
                bridge::jni_call(&mut env, |env| {
                    bridge().create(bridge::options(env, &keys, &values)?)
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_schema"]
            /// # Safety
            /// Spark must supply writable ArrowSchema storage as specified by ABI v1.
            pub unsafe extern "system" fn schema(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                source: jlong,
                address: jlong,
            ) {
                bridge::jni_call(&mut env, |_| {
                    let schema = bridge().schema(source)?;
                    unsafe { bridge::export_schema(&schema, address) }
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_createReader"]
            /// # Safety
            /// Spark must supply an initialized ArrowSchema that this call may move.
            pub unsafe extern "system" fn create_reader(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                source: jlong,
                address: jlong,
            ) -> jlong {
                bridge::jni_call(&mut env, |_| {
                    let schema = unsafe { bridge::import_schema(address)? };
                    bridge().create_reader(source, schema)
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_pushPredicates"]
            pub extern "system" fn push_predicates(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                reader: jlong,
                predicates: JObjectArray<'_>,
            ) -> jbooleanArray {
                bridge::jni_call(&mut env, |env| {
                    let predicates = bridge::strings(env, &predicates)?;
                    let predicates = bridge::parse_predicates(&predicates)?;
                    let exact = bridge().push_predicates(reader, &predicates)?;
                    bridge::booleans(env, &exact)
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_pushLimit"]
            pub extern "system" fn push_limit(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                reader: jlong,
                limit: jint,
            ) -> jboolean {
                bridge::jni_call(&mut env, |_| {
                    Ok(u8::from(bridge().push_limit(reader, limit.try_into()?)?))
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_pruneColumns"]
            pub extern "system" fn prune_columns(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                reader: jlong,
                columns: JObjectArray<'_>,
            ) -> jboolean {
                bridge::jni_call(&mut env, |env| {
                    Ok(u8::from(
                        bridge().prune_columns(reader, &bridge::strings(env, &columns)?)?,
                    ))
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_partitions"]
            pub extern "system" fn partitions(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                reader: jlong,
            ) -> jobjectArray {
                bridge::jni_call(&mut env, |env| {
                    bridge::byte_arrays(env, &bridge().partitions(reader)?)
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_serializeReader"]
            pub extern "system" fn serialize_reader(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                reader: jlong,
            ) -> jbyteArray {
                bridge::jni_call(&mut env, |env| {
                    Ok(env
                        .byte_array_from_slice(&bridge().serialize_reader(reader)?)?
                        .into_raw())
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_read"]
            /// # Safety
            /// Spark must supply writable ArrowArrayStream storage as specified by ABI v1.
            pub unsafe extern "system" fn read(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                state: JByteArray<'_>,
                partition: JByteArray<'_>,
                address: jlong,
            ) {
                bridge::jni_call(&mut env, |env| {
                    let state = env.convert_byte_array(&state)?;
                    let partition = env.convert_byte_array(&partition)?;
                    let stream = bridge().open_reader(&state, &partition)?;
                    unsafe { bridge::export_stream(stream, address) }
                })
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_closeReader"]
            pub extern "system" fn close_reader(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                reader: jlong,
            ) {
                bridge::jni_call(&mut env, |_| bridge().close_reader(reader))
            }

            #[export_name = "Java_org_apache_spark_sql_datasource_NativeBridge_closeDataSource"]
            pub extern "system" fn close_data_source(
                mut env: JNIEnv<'_>,
                _class: JClass<'_>,
                source: jlong,
            ) {
                bridge::jni_call(&mut env, |_| bridge().close_source(source))
            }
        }
    };
}
