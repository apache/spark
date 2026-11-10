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

#include <stddef.h>

#include "spark_datasource.h"

// Compile the standalone header as C, including its own Arrow declarations.
#define SPARK_NATIVE_ABI_TYPE(type) sizeof(struct type), _Alignof(struct type),
#define SPARK_NATIVE_ABI_FIELD(type, field) offsetof(struct type, field),

static const size_t kLayout[] = {
#include "abi_layout_fields.inc"
};

#undef SPARK_NATIVE_ABI_TYPE
#undef SPARK_NATIVE_ABI_FIELD

const size_t* SparkNativeCAbiLayout(size_t* count) {
  *count = sizeof(kLayout) / sizeof(kLayout[0]);
  return kLayout;
}
