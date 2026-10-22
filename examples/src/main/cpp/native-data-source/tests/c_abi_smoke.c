/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* An independent C client: layouts and function signatures come from Spark's actual header. */
#include "spark_datasource.h"

#include <dlfcn.h>
#include <inttypes.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(condition, message) do { \
  if (!(condition)) { \
    fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, (message)); \
    exit(1); \
  } \
} while (0)

typedef struct Bytes {
  uint8_t* data;
  int64_t length;
} Bytes;

typedef struct ByteList {
  Bytes* values;
  size_t length;
} ByteList;

typedef struct Plan {
  ByteList partitions;
  Bytes state;
} Plan;

static const SparkDsLibrary* library;
static const SparkDsTaskContext* expected_task;
static bool interrupted;
static int interruption_checks;

static void check_status(int status) {
  if (status != 0) {
    const char* error = library->last_error();
    fprintf(stderr, "Native status %d: %s\n", status, error == NULL ? "no message" : error);
    exit(1);
  }
}

static void log_message(int32_t level, const char* message) {
  fprintf(stderr, "Native log %d: %s\n", level, message);
}

static bool is_interrupted(const SparkDsTaskContext* task) {
  CHECK(task == expected_task, "Cancellation callback must receive the original task pointer");
  CHECK(task->stage_id == 71 && task->stage_attempt_number == 2, "Unexpected stage identity");
  CHECK(task->partition_id == 3 && task->attempt_number == 4, "Unexpected task identity");
  CHECK(task->task_attempt_id == INT64_C(12345678901), "Task attempt ID was truncated");
  interruption_checks++;
  return interrupted;
}

static bool unavailable_callback(const SparkDsTaskContext* task) {
  (void) task;
  CHECK(false, "A task callback outside its declared struct size must not be read");
  return false;
}

static SparkDsTaskContext task_context(void) {
  SparkDsTaskContext task = {
    .size = sizeof(SparkDsTaskContext),
    .stage_id = 71,
    .stage_attempt_number = 2,
    .partition_id = 3,
    .attempt_number = 4,
    .task_attempt_id = INT64_C(12345678901),
    .is_interrupted = is_interrupted
  };
  return task;
}

/* Sink callbacks copy opaque bytes during the call, as required by the ABI. */
static void add_bytes(void* sink, const uint8_t* data, int64_t length) {
  ByteList* list = sink;
  CHECK(length >= 0, "A sink received a negative length");
  CHECK(length == 0 || data != NULL, "A nonempty sink value must have data");
  Bytes* values = realloc(list->values, (list->length + 1) * sizeof(Bytes));
  CHECK(values != NULL, "Cannot allocate sink values");
  list->values = values;
  Bytes* value = &list->values[list->length++];
  value->length = length;
  value->data = malloc(length == 0 ? 1 : (size_t) length);
  CHECK(value->data != NULL, "Cannot allocate sink data");
  if (length != 0) {
    memcpy(value->data, data, (size_t) length);
  }
}

static void free_list(ByteList* list) {
  for (size_t i = 0; i < list->length; i++) {
    free(list->values[i].data);
  }
  free(list->values);
}

static void check_bytes(const Bytes* left, const Bytes* right) {
  CHECK(left->length == right->length, "Repeated planning changed the serialized size");
  CHECK(memcmp(left->data, right->data, (size_t) left->length) == 0,
      "Repeated planning changed the serialized data");
}

static void* create_source(const char* const* keys, const char* const* values, int64_t count) {
  SparkDsCreateInfo info = {
    .size = sizeof(SparkDsCreateInfo),
    .name = "cpp_sdk_range",
    .option_keys = keys,
    .option_values = values,
    .num_options = count
  };
  uint64_t capabilities = 0;
  void* source = NULL;
  check_status(library->create(&info, &capabilities, &source));
  CHECK(source != NULL, "create returned a null source");
  CHECK(capabilities == (SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ),
      "The Range source must declare batch and micro-batch reads");
  return source;
}

static void check_capability_discovery(
    const char* const* keys, const char* const* values, int64_t count) {
  /* Analysis may create a source only to inspect its capabilities, then immediately close it. */
  void* source = create_source(keys, values, count);
  library->close(source);
}

static void* create_planner(
    const char* const* keys,
    const char* const* values,
    int64_t option_count) {
  void* source = create_source(keys, values, option_count);
  struct ArrowSchema schema = {0};
  check_status(library->schema(source, &schema));
  CHECK(schema.release != NULL && schema.n_children == 2, "Unexpected source schema");
  CHECK(strcmp(schema.format, "+s") == 0, "The schema must describe a struct");
  CHECK(strcmp(schema.children[0]->name, "id") == 0, "Missing id field");
  CHECK(strcmp(schema.children[0]->format, "l") == 0, "id must be int64");
  CHECK(strcmp(schema.children[1]->name, "label") == 0, "Missing label field");
  CHECK(strcmp(schema.children[1]->format, "u") == 0, "label must be UTF-8");

  void* reader = NULL;
  check_status(library->create_reader(source, &schema, &reader));
  CHECK(reader != NULL, "create_reader returned a null reader");
  if (schema.release != NULL) {
    schema.release(&schema);
  }
  library->close(source);
  return reader;
}

static Plan finish_plan(void* reader) {
  Plan plan = {0};
  check_status(library->partitions(reader, add_bytes, &plan.partitions));
  ByteList repeated = {0};
  check_status(library->partitions(reader, add_bytes, &repeated));
  CHECK(repeated.length == plan.partitions.length, "Repeated planning changed partition count");
  for (size_t i = 0; i < repeated.length; i++) {
    check_bytes(&plan.partitions.values[i], &repeated.values[i]);
  }
  free_list(&repeated);

  ByteList state = {0};
  check_status(library->serialize_reader(reader, add_bytes, &state));
  check_status(library->serialize_reader(reader, add_bytes, &state));
  CHECK(state.length == 2, "Each serialization must emit exactly one state value");
  check_bytes(&state.values[0], &state.values[1]);
  plan.state = state.values[0];
  free(state.values[1].data);
  free(state.values);
  library->close_reader(reader);
  return plan;
}

static Plan plan_scan(
    const char* const* keys,
    const char* const* values,
    int64_t option_count,
    const char* const* predicates,
    const int32_t* expected_uses,
    int64_t predicate_count,
    int32_t limit,
    const char* const* projection,
    int64_t projection_count) {
  void* reader = create_planner(keys, values, option_count);

  if (predicate_count != 0) {
    int32_t* uses = calloc((size_t) predicate_count, sizeof(int32_t));
    CHECK(uses != NULL, "Cannot allocate predicate results");
    check_status(library->push_predicates(reader, predicates, predicate_count, uses));
    for (int64_t i = 0; i < predicate_count; i++) {
      CHECK(uses[i] == expected_uses[i], "Incorrect C predicate-use enum value");
    }
    free(uses);
  }
  if (limit >= 0) {
    CHECK(predicate_count == 0, "Spark only pushes a limit without predicates");
    bool applied = false;
    check_status(library->push_limit(reader, limit, &applied));
    CHECK(applied, "The Range source must apply its global limit");
  }
  if (projection_count >= 0) {
    bool pruned = false;
    check_status(library->prune_columns(reader, projection, projection_count, &pruned));
    CHECK(pruned, "The Range source must apply its projection");
  }

  return finish_plan(reader);
}

static void free_plan(Plan* plan) {
  free_list(&plan->partitions);
  free(plan->state.data);
}

static struct ArrowArrayStream open_stream(
    const Plan* plan, size_t partition, const SparkDsTaskContext* task) {
  struct ArrowArrayStream stream = {0};
  const Bytes* bytes = &plan->partitions.values[partition];
  check_status(library->read(task, plan->state.data, plan->state.length,
      bytes->data, bytes->length, &stream));
  CHECK(stream.release != NULL && stream.get_schema != NULL && stream.get_next != NULL,
      "read returned an incomplete Arrow stream");
  return stream;
}

static void check_batch(
    const struct ArrowSchema* schema,
    const struct ArrowArray* batch,
    int64_t first_id,
    int64_t max_batch_size) {
  CHECK(batch->offset == 0, "The root array must have zero offset");
  CHECK(batch->length > 0 && batch->length <= max_batch_size, "Unexpected batch row count");
  CHECK(batch->n_children == schema->n_children, "Batch and schema have different column counts");
  for (int64_t column = 0; column < schema->n_children; column++) {
    const struct ArrowSchema* field = schema->children[column];
    const struct ArrowArray* array = batch->children[column];
    CHECK(array->offset == 0 && array->length == batch->length, "Unexpected child array layout");
    if (strcmp(field->name, "id") == 0) {
      CHECK(strcmp(field->format, "l") == 0, "Unexpected id type");
      const int64_t* ids = array->buffers[1];
      for (int64_t row = 0; row < batch->length; row++) {
        CHECK(ids[row] == first_id + row, "Unexpected id value");
      }
    } else {
      CHECK(strcmp(field->name, "label") == 0, "Unexpected output field");
      CHECK(strcmp(field->format, "u") == 0, "Unexpected label type");
      const int32_t* offsets = array->buffers[1];
      const char* data = array->buffers[2];
      for (int64_t row = 0; row < batch->length; row++) {
        char expected[64];
        int length = snprintf(expected, sizeof(expected), "row-%" PRId64, first_id + row);
        CHECK(offsets[row + 1] - offsets[row] == length, "Unexpected label length");
        CHECK(memcmp(data + offsets[row], expected, (size_t) length) == 0,
            "Unexpected label value");
      }
    }
  }
}

static void consume_plan(
    const Plan* plan,
    int64_t first_id,
    int64_t expected_rows,
    const char* const* columns,
    int64_t column_count) {
  int64_t rows = 0;
  for (size_t partition = 0; partition < plan->partitions.length; partition++) {
    SparkDsTaskContext task = task_context();
    expected_task = &task;
    interrupted = false;
    struct ArrowArrayStream stream = open_stream(plan, partition, &task);
    struct ArrowSchema schema = {0};
    CHECK(stream.get_schema(&stream, &schema) == 0, "Cannot read the stream schema");
    CHECK(schema.n_children == column_count, "Unexpected projected column count");
    for (int64_t i = 0; i < column_count; i++) {
      CHECK(strcmp(schema.children[i]->name, columns[i]) == 0,
          "Projection did not preserve the requested column order");
    }

    struct ArrowArray* batches = NULL;
    size_t count = 0;
    while (true) {
      struct ArrowArray batch = {0};
      int status = stream.get_next(&stream, &batch);
      if (status != 0) {
        fprintf(stderr, "Arrow error: %s\n", stream.get_last_error(&stream));
      }
      CHECK(status == 0, "Cannot read a batch");
      if (batch.release == NULL) {
        break;
      }
      struct ArrowArray* resized = realloc(batches, (count + 1) * sizeof(struct ArrowArray));
      CHECK(resized != NULL, "Cannot retain batches");
      batches = resized;
      batches[count++] = batch;
    }
    stream.release(&stream);
    CHECK(stream.release == NULL, "Releasing the stream must clear its release callback");

    /* The arrays retain their buffers across later reads and the release of their stream. */
    for (size_t i = 0; i < count; i++) {
      check_batch(&schema, &batches[i], first_id + rows, 2);
      rows += batches[i].length;
      batches[i].release(&batches[i]);
    }
    free(batches);
    schema.release(&schema);
  }
  CHECK(rows == expected_rows, "Unexpected total row count");
}

/* A NUL-terminated copy of a value that a sink received, such as an offset. */
static char* copy_string(const Bytes* bytes) {
  char* string = malloc((size_t) bytes->length + 1);
  CHECK(string != NULL, "Cannot allocate a string");
  memcpy(string, bytes->data, (size_t) bytes->length);
  string[bytes->length] = '\0';
  return string;
}

static void* create_stream_reader(
    const char* const* keys, const char* const* values, int64_t count) {
  void* source = create_source(keys, values, count);
  struct ArrowSchema schema = {0};
  check_status(library->schema(source, &schema));
  void* stream = NULL;
  check_status(library->create_stream_reader(source, &schema, &stream));
  CHECK(stream != NULL, "create_stream_reader returned a null reader");
  if (schema.release != NULL) {
    schema.release(&schema);
  }
  /* Spark closes the source right after it creates the stream reader. */
  library->close(source);
  return stream;
}

static Plan plan_micro_batch(void* stream, const char* start, const char* end) {
  Plan plan = {0};
  check_status(library->stream_partitions(stream, start, end, add_bytes, &plan.partitions));
  ByteList state = {0};
  check_status(library->serialize_stream_reader(stream, add_bytes, &state));
  CHECK(state.length == 1, "serialize_stream_reader must emit exactly one state value");
  plan.state = state.values[0];
  free(state.values);
  return plan;
}

/*
 * A streaming read: the stream reader stays on the driver, and plans each micro-batch between
 * two offsets that the library defines as JSON. The executors read a micro-batch with the read
 * function of batch scans.
 */
static void check_micro_batch_read(
    const char* const* keys, const char* const* values, int64_t count) {
  void* stream = create_stream_reader(keys, values, count);
  ByteList offsets = {0};
  check_status(library->initial_offset(stream, add_bytes, &offsets));
  check_status(library->latest_offset(stream, add_bytes, &offsets));
  CHECK(offsets.length == 2, "Each offset function must emit exactly one offset");
  char* initial = copy_string(&offsets.values[0]);
  char* latest = copy_string(&offsets.values[1]);
  CHECK(strcmp(initial, "{\"next\":10}") == 0, "Unexpected initial offset");
  CHECK(strcmp(latest, "{\"next\":25}") == 0, "Unexpected latest offset");

  /* Two micro-batches, split at an offset as if from the checkpoint of an earlier query. */
  const char* columns[] = {"id", "label"};
  Plan first = plan_micro_batch(stream, initial, "{\"next\":13}");
  consume_plan(&first, 10, 3, columns, 2);
  free_plan(&first);
  Plan second = plan_micro_batch(stream, "{\"next\":13}", latest);
  consume_plan(&second, 13, 12, columns, 2);
  free_plan(&second);
  check_status(library->commit_offset(stream, latest));
  library->close_stream_reader(stream);

  /* An offset that does not decode fails the query, and then every call on its reader. */
  stream = create_stream_reader(keys, values, count);
  ByteList partitions = {0};
  CHECK(library->stream_partitions(stream, initial, "not an offset", add_bytes, &partitions) != 0,
      "An offset that does not decode must fail");
  const char* error = library->last_error();
  CHECK(error != NULL && strstr(error, "cannot decode the offset") != NULL,
      "Missing offset error");
  ByteList offset = {0};
  CHECK(library->latest_offset(stream, add_bytes, &offset) != 0 && offset.length == 0,
      "A failed stream must not be called again");
  library->close_stream_reader(stream);
  free(initial);
  free(latest);
  free_list(&offsets);
}

static void check_independent_planners(
    const char* const* keys,
    const char* const* values,
    int64_t count,
    const char* lower,
    const char* upper) {
  void* first = create_planner(keys, values, count);
  void* second = create_planner(keys, values, count);
  int32_t use = SPARK_DS_PREDICATE_UNUSED;
  check_status(library->push_predicates(first, &upper, 1, &use));
  CHECK(use == SPARK_DS_PREDICATE_COMPLETE, "The first planner did not accept its filter");
  const char* label_only[] = {"label"};
  bool pruned = false;
  check_status(library->prune_columns(first, label_only, 1, &pruned));
  CHECK(pruned, "The first planner did not accept its projection");
  Plan first_plan = finish_plan(first);

  /* The second planner must neither inherit the first filter nor change its serialized plan. */
  use = SPARK_DS_PREDICATE_UNUSED;
  check_status(library->push_predicates(second, &lower, 1, &use));
  CHECK(use == SPARK_DS_PREDICATE_COMPLETE, "The second planner did not accept its filter");
  const char* id_only[] = {"id"};
  pruned = false;
  check_status(library->prune_columns(second, id_only, 1, &pruned));
  CHECK(pruned, "The second planner did not accept its projection");
  Plan second_plan = finish_plan(second);

  /* Both driver planners are already closed; executor reads only get the copied bytes. */
  consume_plan(&second_plan, 13, 12, id_only, 1);
  consume_plan(&first_plan, 10, 12, label_only, 1);
  free_plan(&second_plan);
  free_plan(&first_plan);
}

static void check_cancellation(const Plan* plan) {
  CHECK(plan->partitions.length == 1, "Cancellation test requires one partition");
  SparkDsTaskContext task = task_context();
  expected_task = &task;
  interrupted = false;
  interruption_checks = 0;
  struct ArrowArrayStream stream = open_stream(plan, 0, &task);
  struct ArrowSchema schema = {0};
  CHECK(stream.get_schema(&stream, &schema) == 0, "Cannot read the stream schema");
  struct ArrowArray first = {0};
  CHECK(stream.get_next(&stream, &first) == 0 && first.release != NULL, "Missing first batch");
  CHECK(interruption_checks > 0, "The reader did not check the task callback");
  int previous_checks = interruption_checks;
  interrupted = true;
  struct ArrowArray next = {0};
  CHECK(stream.get_next(&stream, &next) != 0,
      "A cancelled task must fail before its next batch");
  CHECK(interruption_checks > previous_checks, "Cancellation did not call the task callback");
  const char* error = stream.get_last_error(&stream);
  CHECK(error != NULL && strstr(error, "cancel") != NULL, "Missing cancellation error");
  if (next.release != NULL) {
    next.release(&next);
  }
  stream.release(&stream);
  /* Early stream release must not invalidate the batch already handed to its consumer. */
  check_batch(&schema, &first, 0, 2);
  first.release(&first);
  schema.release(&schema);
  interrupted = false;
}

enum StreamOperation {
  STREAM_SCHEMA,
  STREAM_NEXT,
  STREAM_ERROR,
  STREAM_RELEASE
};

typedef struct StreamCall {
  struct ArrowArrayStream* stream;
  enum StreamOperation operation;
  struct ArrowSchema schema;
  struct ArrowArray batch;
  int status;
  char error[256];
} StreamCall;

static void* run_stream_call(void* argument) {
  StreamCall* call = argument;
  switch (call->operation) {
    case STREAM_SCHEMA:
      call->status = call->stream->get_schema(call->stream, &call->schema);
      break;
    case STREAM_NEXT:
      call->status = call->stream->get_next(call->stream, &call->batch);
      break;
    case STREAM_ERROR: {
      const char* error = call->stream->get_last_error(call->stream);
      CHECK(error != NULL, "A stream error must remain available on another thread");
      snprintf(call->error, sizeof(call->error), "%s", error);
      break;
    }
    case STREAM_RELEASE:
      call->stream->release(call->stream);
      break;
  }
  return NULL;
}

static void stream_call_on_thread(StreamCall* call) {
  pthread_t thread;
  CHECK(pthread_create(&thread, NULL, run_stream_call, call) == 0,
      "Cannot create a stream callback thread");
  CHECK(pthread_join(thread, NULL) == 0, "Cannot join a stream callback thread");
}

/* Every host has the fields of version 1: a shorter context is an error, not partly read. */
static void check_short_task_context(const Plan* plan) {
  SparkDsTaskContext task = task_context();
  task.size = offsetof(SparkDsTaskContext, is_interrupted);
  task.is_interrupted = unavailable_callback;
  struct ArrowArrayStream stream = {0};
  const Bytes* bytes = &plan->partitions.values[0];
  CHECK(library->read(&task, plan->state.data, plan->state.length, bytes->data, bytes->length,
      &stream) != 0, "A task context shorter than version 1 must be rejected");
  const char* error = library->last_error();
  CHECK(error != NULL && strstr(error, "version 1") != NULL, "Missing task context error");
  CHECK(stream.release == NULL, "A failed read must not initialize the stream");
}

static void check_cross_thread_stream(const Plan* plan) {
  SparkDsTaskContext task = task_context();
  expected_task = &task;
  interrupted = false;
  interruption_checks = 0;
  struct ArrowArrayStream stream = open_stream(plan, 0, &task);

  /* Callbacks do not overlap, but each worker has a different lifetime and thread-local state. */
  StreamCall schema = {.stream = &stream, .operation = STREAM_SCHEMA};
  stream_call_on_thread(&schema);
  CHECK(schema.status == 0 && schema.schema.release != NULL, "Cannot get schema on a new thread");

  StreamCall first = {.stream = &stream, .operation = STREAM_NEXT};
  stream_call_on_thread(&first);
  CHECK(first.status == 0 && first.batch.release != NULL, "Cannot read a batch on a new thread");
  CHECK(interruption_checks > 0, "The worker did not call the task cancellation callback");

  interrupted = true;
  StreamCall next = {.stream = &stream, .operation = STREAM_NEXT};
  stream_call_on_thread(&next);
  CHECK(next.status != 0, "The worker must observe live task cancellation");
  StreamCall error = {.stream = &stream, .operation = STREAM_ERROR};
  stream_call_on_thread(&error);
  CHECK(strstr(error.error, "cancel") != NULL, "The stream error changed across threads");
  if (next.batch.release != NULL) {
    next.batch.release(&next.batch);
  }

  StreamCall release = {.stream = &stream, .operation = STREAM_RELEASE};
  stream_call_on_thread(&release);
  CHECK(stream.release == NULL, "Cross-thread release must clear the stream callback");
  check_batch(&schema.schema, &first.batch, 0, 2);
  first.batch.release(&first.batch);
  schema.schema.release(&schema.schema);
  interrupted = false;
}

static void check_error_message(void) {
  const char* keys[] = {"batchsize"};
  const char* values[] = {"0"};
  SparkDsCreateInfo info = {
    .size = sizeof(SparkDsCreateInfo),
    .name = "cpp_sdk_range",
    .option_keys = keys,
    .option_values = values,
    .num_options = 1
  };
  uint64_t capabilities = 0;
  void* source = NULL;
  CHECK(library->create(&info, &capabilities, &source) != 0,
      "Invalid options must return an error");
  const char* error = library->last_error();
  CHECK(error != NULL && strstr(error, "batchSize must be positive") != NULL,
      "The C error channel did not preserve the provider's message");
  CHECK(source == NULL, "Failed creation must not return a source handle");
}

static void check_function_table(void) {
  CHECK(library != NULL, "spark_ds_init returned no library");
  CHECK(library->size == sizeof(SparkDsLibrary), "The function table differs from Spark's header");
  CHECK(library->abi_version == SPARK_DS_ABI_VERSION, "Unexpected ABI version");
  CHECK(library->last_error != NULL, "Missing last_error function");
  CHECK(library->create != NULL && library->schema != NULL && library->close != NULL,
      "Missing source functions");
  CHECK(library->create_reader != NULL && library->push_predicates != NULL &&
      library->push_limit != NULL && library->prune_columns != NULL &&
      library->partitions != NULL && library->serialize_reader != NULL &&
      library->close_reader != NULL && library->read != NULL, "Missing batch read functions");
  CHECK(library->create_stream_reader != NULL && library->initial_offset != NULL &&
      library->latest_offset != NULL && library->stream_partitions != NULL &&
      library->serialize_stream_reader != NULL && library->commit_offset != NULL &&
      library->close_stream_reader != NULL, "Missing micro-batch read functions");
  CHECK(library->create_writer == NULL && library->serialize_writer == NULL &&
      library->commit == NULL && library->abort == NULL && library->close_writer == NULL &&
      library->create_data_writer == NULL && library->write == NULL &&
      library->commit_data_writer == NULL && library->abort_data_writer == NULL &&
      library->close_data_writer == NULL, "A read-only source must not export writes");
}

int main(int argc, char** argv) {
  CHECK(argc == 2, "Expected the path to the Range shared library");
  void* module = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
  if (module == NULL) {
    fprintf(stderr, "Cannot load the Range library: %s\n", dlerror());
    return 1;
  }
  void* symbol = dlsym(module, "spark_ds_init");
  CHECK(symbol != NULL, "The library does not export spark_ds_init");
  int (*init)(const SparkDsHost*, const SparkDsLibrary**);
  CHECK(sizeof(init) == sizeof(symbol), "This platform has incompatible function pointers");
  memcpy(&init, &symbol, sizeof(init));
  SparkDsHost host = {
    .size = sizeof(SparkDsHost),
    .abi_version = SPARK_DS_ABI_VERSION,
    .spark_version = "4.4.0-c-abi-test",
    .log = log_message
  };
  CHECK(init(&host, &library) == 0, "spark_ds_init failed");
  check_function_table();

  const char* keys[] = {"start", "end", "numpartitions", "batchsize"};
  const char* values[] = {"10", "25", "3", "2"};
  const char* lower = "{\"type\":\"function\",\"name\":\">=\",\"children\":["
      "{\"type\":\"column\",\"name\":[\"id\"]},"
      "{\"type\":\"literal\",\"dataType\":\"bigint\",\"value\":13}]}";
  const char* upper = "{\"type\":\"function\",\"name\":\"<\",\"children\":["
      "{\"type\":\"column\",\"name\":[\"id\"]},"
      "{\"type\":\"literal\",\"dataType\":\"bigint\",\"value\":22}]}";
  const char* label = "{\"type\":\"function\",\"name\":\"=\",\"children\":["
      "{\"type\":\"column\",\"name\":[\"label\"]},"
      "{\"type\":\"literal\",\"dataType\":\"string\",\"value\":\"row-21\"}]}";
  const char* predicates[] = {lower, upper, label};
  const int32_t uses[] = {
    SPARK_DS_PREDICATE_COMPLETE, SPARK_DS_PREDICATE_COMPLETE, SPARK_DS_PREDICATE_UNUSED
  };
  const char* reordered[] = {"label", "id"};
  const char* id_only[] = {"id"};
  const char* original[] = {"id", "label"};

  check_capability_discovery(keys, values, 4);
  check_capability_discovery(NULL, NULL, 0);
  check_independent_planners(keys, values, 4, lower, upper);
  check_micro_batch_read(keys, values, 4);

  Plan filtered = plan_scan(keys, values, 4, predicates, uses, 2, -1, reordered, 2);
  consume_plan(&filtered, 13, 9, reordered, 2);
  free_plan(&filtered);

  Plan residual = plan_scan(keys, values, 4, predicates, uses, 3, -1, id_only, 1);
  consume_plan(&residual, 13, 9, id_only, 1);
  free_plan(&residual);

  Plan limited = plan_scan(keys, values, 4, NULL, NULL, 0, 5, id_only, 1);
  consume_plan(&limited, 10, 5, id_only, 1);
  free_plan(&limited);

  const char* short_keys[] = {"end", "numpartitions", "batchsize"};
  const char* short_values[] = {"7", "2", "2"};
  Plan empty = plan_scan(short_keys, short_values, 3, NULL, NULL, 0, -1, NULL, 0);
  consume_plan(&empty, 0, 7, NULL, 0);
  free_plan(&empty);

  const char* one_partition[] = {"20", "1", "2"};
  Plan cancellable = plan_scan(short_keys, one_partition, 3, NULL, NULL, 0, -1, NULL, -1);
  consume_plan(&cancellable, 0, 20, original, 2);
  check_short_task_context(&cancellable);
  check_cancellation(&cancellable);
  check_cross_thread_stream(&cancellable);
  free_plan(&cancellable);
  check_error_message();

  CHECK(dlclose(module) == 0, "Cannot close the Range library");
  puts("C ABI, capabilities, planning, streaming, Arrow ownership, and cancellation checks "
      "passed.");
  return 0;
}
