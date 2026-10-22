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

// The function table of version 1 of the ABI for a data source, and SPARK_EXPORT_DATA_SOURCE.

#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include "spark/native/data_source.h"
#include "spark/native/internal/abi.h"
#include "spark/native/internal/arrow.h"
#include "spark/native/internal/bridge.h"
#include "spark/native/internal/serde.h"
#include "spark_datasource.h"

/// Exports a data source through version 1 of Spark's C data source ABI. Use it once in a shared
/// library, at namespace scope:
///
///   SPARK_EXPORT_DATA_SOURCE(RangeSource);
///
/// The types that the source names in DataSource determine its operations: the SDK installs the
/// native callbacks of each operation whose type is not Unsupported, and leaves the others null.
/// Batch and streaming operations share the executor callbacks, read and the data writer
/// callbacks. The only exported symbol is spark_ds_init: build the library with hidden symbol
/// visibility, as spark_add_native_data_source in CMake does, so that the SDK and its
/// dependencies stay private to the library.
#define SPARK_EXPORT_DATA_SOURCE(SourceType)                                     \
  extern "C" SPARK_DS_EXPORT int spark_ds_init(const SparkDsHost* host,          \
                                               const SparkDsLibrary** library) { \
    return ::spark::native::internal::Initialize<SourceType>(host, library);     \
  }                                                                              \
  static_assert(true, "SPARK_EXPORT_DATA_SOURCE requires a semicolon")

namespace spark::native::internal {

template <typename T>
inline constexpr bool kImplemented = !std::is_same_v<T, Unsupported>;

template <typename F>
concept ReaderFactory = requires { typename F::Partition; } &&
                        std::derived_from<F, PartitionReaderFactory<typename F::Partition>> &&
                        Serializable<F> && Serializable<typename F::Partition>;

template <typename B>
concept ScanBuilderOf =
    requires { typename B::Factory; } && std::derived_from<B, ScanBuilder<typename B::Factory>> &&
    std::move_constructible<B> && ReaderFactory<typename B::Factory>;

template <typename M>
concept StreamOf =
    requires {
      typename M::Offset;
      typename M::Factory;
    } && std::derived_from<M, MicroBatchStream<typename M::Offset, typename M::Factory>> &&
    std::move_constructible<M> && Serializable<typename M::Offset> &&
    ReaderFactory<typename M::Factory>;

template <typename F>
concept WriterFactory = requires { typename F::Message; } &&
                        std::derived_from<F, DataWriterFactory<typename F::Message>> &&
                        Serializable<F> && Serializable<typename F::Message>;

template <typename W>
concept BatchWriteOf =
    requires { typename W::Factory; } && std::derived_from<W, BatchWrite<typename W::Factory>> &&
    std::move_constructible<W> && WriterFactory<typename W::Factory>;

template <typename W>
concept StreamingWriteOf = requires { typename W::Factory; } &&
                           std::derived_from<W, StreamingWrite<typename W::Factory>> &&
                           std::move_constructible<W> && WriterFactory<typename W::Factory>;

template <typename T>
struct FactoryOf {
  using type = typename T::Factory;
};

template <>
struct FactoryOf<Unsupported> {
  using type = Unsupported;
};

/// What a source implements, which its declarations determine, as the documentation of
/// DataSource describes them.
template <typename S>
struct SourceTraits {
  static_assert(std::derived_from<S, DataSource>,
                "a data source must derive from spark::native::DataSource");
  static_assert(
      requires(const CreateInfo& info) {
        { S::Make(info) } -> std::same_as<arrow::Result<S>>;
      }, "a data source must define static arrow::Result<S> Make(const CreateInfo& info)");
  static_assert(std::move_constructible<S>, "a data source must be movable");
  static_assert(
      requires {
        { S::Init(HostInfo()) } -> std::same_as<arrow::Status>;
        { S::kInfersSchema } -> std::convertible_to<bool>;
      }, "DataSource::Init and DataSource::kInfersSchema have other types");

  using ScanBuilder = typename S::ScanBuilder;
  using MicroBatchStream = typename S::MicroBatchStream;
  using BatchWrite = typename S::BatchWrite;
  using StreamingWrite = typename S::StreamingWrite;

  static constexpr bool kBatchRead = kImplemented<ScanBuilder>;
  static constexpr bool kMicroBatchRead = kImplemented<MicroBatchStream>;
  static constexpr bool kBatchWrite = kImplemented<BatchWrite>;
  static constexpr bool kStreamingWrite = kImplemented<StreamingWrite>;

  static_assert(!kBatchRead || ScanBuilderOf<ScanBuilder>,
                "DataSource::ScanBuilder must derive from spark::native::ScanBuilder<Factory>, be "
                "movable, and have a factory that derives from PartitionReaderFactory<Partition>, "
                "whose factory and partitions nlohmann::json serializes");
  static_assert(
      !kBatchRead ||
          requires(const S& source, const ScanInfo& info) {
            { source.NewScan(info) } -> std::same_as<arrow::Result<ScanBuilder>>;
          },
      "a source with a ScanBuilder must define arrow::Result<ScanBuilder> NewScan(const ScanInfo&) "
      "const");
  static_assert(!kMicroBatchRead || StreamOf<MicroBatchStream>,
                "DataSource::MicroBatchStream must derive from "
                "spark::native::MicroBatchStream<Offset, Factory>, be movable, and have offsets "
                "and a factory that nlohmann::json serializes");
  static_assert(
      !kMicroBatchRead ||
          requires(const S& source, const MicroBatchStreamInfo& info) {
            { source.NewMicroBatchStream(info) } -> std::same_as<arrow::Result<MicroBatchStream>>;
          },
      "a source with a MicroBatchStream must define arrow::Result<MicroBatchStream> "
      "NewMicroBatchStream(const MicroBatchStreamInfo&) const");
  static_assert(!kBatchWrite || BatchWriteOf<BatchWrite>,
                "DataSource::BatchWrite must derive from spark::native::BatchWrite<Factory>, be "
                "movable, and have a factory that derives from DataWriterFactory<Message>, whose "
                "factory and messages nlohmann::json serializes");
  static_assert(
      !kBatchWrite ||
          requires(const S& source, const WriteInfo& info) {
            { source.NewWrite(info) } -> std::same_as<arrow::Result<BatchWrite>>;
          },
      "a source with a BatchWrite must define arrow::Result<BatchWrite> NewWrite(const WriteInfo&) "
      "const");
  static_assert(!kStreamingWrite || StreamingWriteOf<StreamingWrite>,
                "DataSource::StreamingWrite must derive from "
                "spark::native::StreamingWrite<Factory>, be movable, and have a factory that "
                "derives from DataWriterFactory<Message>, whose factory and messages "
                "nlohmann::json serializes");
  static_assert(
      !kStreamingWrite ||
          requires(const S& source, const StreamingWriteInfo& info) {
            { source.NewStreamingWrite(info) } -> std::same_as<arrow::Result<StreamingWrite>>;
          },
      "a source with a StreamingWrite must define arrow::Result<StreamingWrite> "
      "NewStreamingWrite(const StreamingWriteInfo&) const");

  // Whether each write supports truncation, which Unsupported does not.
  static constexpr bool kBatchTruncate = [] {
    if constexpr (kBatchWrite) {
      return static_cast<bool>(BatchWrite::kSupportsTruncate);
    } else {
      return false;
    }
  }();
  static constexpr bool kStreamingTruncate = [] {
    if constexpr (kStreamingWrite) {
      return static_cast<bool>(StreamingWrite::kSupportsTruncate);
    } else {
      return false;
    }
  }();

  /// The operations that the source implements.
  static constexpr Capabilities kCapabilities = [] {
    Capabilities capabilities = Capabilities::Empty();
    if (kBatchRead) capabilities = capabilities | Capabilities::kBatchRead;
    if (kMicroBatchRead) capabilities = capabilities | Capabilities::kMicroBatchRead;
    if (kBatchWrite) capabilities = capabilities | Capabilities::kBatchWrite;
    if (kStreamingWrite) capabilities = capabilities | Capabilities::kStreamingWrite;
    if (kBatchTruncate || kStreamingTruncate) {
      capabilities = capabilities | Capabilities::kTruncate;
    }
    return capabilities;
  }();

  // The factories of the executors, Unsupported for an operation that the source does not
  // implement.
  using BatchReaderFactory = typename FactoryOf<ScanBuilder>::type;
  using StreamReaderFactory = typename FactoryOf<MicroBatchStream>::type;
  using BatchWriterFactory = typename FactoryOf<BatchWrite>::type;
  using StreamingWriterFactory = typename FactoryOf<StreamingWrite>::type;

  /// Keeps the implemented operations of a declaration, so that a source can narrow its
  /// operations but never add one, and truncation only while a write that supports it remains.
  static constexpr Capabilities Effective(Capabilities declared) {
    Capabilities capabilities = declared & kCapabilities;
    bool truncates = (kBatchTruncate && capabilities.Contains(Capabilities::kBatchWrite)) ||
                     (kStreamingTruncate && capabilities.Contains(Capabilities::kStreamingWrite));
    return truncates ? capabilities : capabilities.Without(Capabilities::kTruncate);
  }
};

/// The native callbacks of a data source.
template <typename S>
class Exports {
 public:
  using Traits = SourceTraits<S>;

  static const SparkDsLibrary* Library() {
    static const SparkDsLibrary library = [] {
      SparkDsLibrary table{};
      table.size = sizeof(SparkDsLibrary);
      table.abi_version = SPARK_DS_ABI_VERSION;
      table.last_error = &LastErrorCallback;
      table.create = &Create;
      if constexpr (S::kInfersSchema) {
        table.schema = &InferSchema;
      }
      table.close = &Close;
      if constexpr (Traits::kBatchRead) {
        table.create_reader = &CreateReader;
        table.push_predicates = &PushPredicates;
        table.push_limit = &PushLimit;
        table.prune_columns = &PruneColumns;
        table.partitions = &Partitions;
        table.serialize_reader = &SerializeReader;
        table.close_reader = &CloseReader;
      }
      if constexpr (Traits::kMicroBatchRead) {
        table.create_stream_reader = &CreateStreamReader;
        table.initial_offset = &InitialOffset;
        table.latest_offset = &LatestOffset;
        table.stream_partitions = &StreamPartitions;
        table.serialize_stream_reader = &SerializeStreamReader;
        table.commit_offset = &CommitOffset;
        table.close_stream_reader = &CloseStreamReader;
      }
      // Batch scans and micro-batches share the executor reader.
      if constexpr (Traits::kBatchRead || Traits::kMicroBatchRead) {
        table.read = &Read;
      }
      // Batch and streaming writes share all the callbacks of writers.
      if constexpr (Traits::kBatchWrite || Traits::kStreamingWrite) {
        table.create_writer = &CreateWriter;
        table.serialize_writer = &SerializeWriter;
        table.commit = &Commit;
        table.abort = &Abort;
        table.close_writer = &CloseWriter;
        table.create_data_writer = &CreateDataWriter;
        table.write = &Write;
        table.commit_data_writer = &CommitDataWriter;
        table.abort_data_writer = &AbortDataWriter;
        table.close_data_writer = &CloseDataWriter;
      }
      return table;
    }();
    return &library;
  }

 private:
  using Source = SourceHandle<S>;
  using Planner = internal::Planner<typename Traits::ScanBuilder>;
  using Stream = StreamReader<typename Traits::MicroBatchStream>;

  template <typename T>
  static arrow::Result<T*> Handle(void* handle) {
    if (handle == nullptr) {
      return arrow::Status::Invalid("null native handle");
    }
    return static_cast<T*>(handle);
  }

  static const char* LastErrorCallback() noexcept { return LastError(); }

  // Data source.

  static int Create(const SparkDsCreateInfo* info, uint64_t* capabilities, void** source) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(capabilities));
      ARROW_RETURN_NOT_OK(RequireOutput(source));
      ARROW_ASSIGN_OR_RAISE(CreateInfo create_info, ReadCreateInfo(info));
      ARROW_ASSIGN_OR_RAISE(S created, S::Make(create_info));
      Capabilities declared = created.capabilities();
      auto handle = std::make_unique<Source>(Source{std::move(created), Capabilities::Empty()});
      handle->capabilities = Traits::Effective(declared);
      *capabilities = handle->capabilities.bits();
      *source = handle.release();
      return arrow::Status::OK();
    });
  }

  static int InferSchema(void* source, struct ArrowSchema* out) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(out));
      ARROW_ASSIGN_OR_RAISE(Source * handle, Handle<Source>(source));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, handle->source.InferSchema());
      if (schema == nullptr) {
        return arrow::Status::Invalid("DataSource::InferSchema returned a null schema");
      }
      ARROW_RETURN_NOT_OK(ValidateSchema(*schema));
      return arrow::ExportSchema(*schema, out);
    });
  }

  static void Close(void* source) noexcept { delete static_cast<Source*>(source); }

  // Batch reader.

  static int CreateReader(void* source, struct ArrowSchema* schema, void** reader) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(reader));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> read_schema, ImportSchema(schema));
      ARROW_RETURN_NOT_OK(ValidateSchema(*read_schema));
      ARROW_ASSIGN_OR_RAISE(Source * handle, Handle<Source>(source));
      if (!handle->capabilities.Contains(Capabilities::kBatchRead)) {
        return arrow::Status::Invalid("source does not support batch reads");
      }
      ARROW_ASSIGN_OR_RAISE(auto builder, handle->source.NewScan(ScanInfo(read_schema)));
      *reader = new Planner(std::move(builder), std::move(read_schema));
      return arrow::Status::OK();
    });
  }

  static int PushPredicates(void* reader, const char* const* predicates, int64_t count,
                            int32_t* uses) noexcept {
    return Call([&]() -> arrow::Status {
      if (count > 0) {
        ARROW_RETURN_NOT_OK(RequireOutput(uses));
      }
      ARROW_ASSIGN_OR_RAISE(Planner * planner, Handle<Planner>(reader));
      ARROW_ASSIGN_OR_RAISE(std::vector<std::string> values, Strings(predicates, count));
      ARROW_ASSIGN_OR_RAISE(std::vector<FilterSupport> support,
                            planner->PushPredicates(ParsePredicates(values)));
      for (size_t index = 0; index < support.size(); ++index) {
        uses[index] = PredicateUse(support[index]);
      }
      return arrow::Status::OK();
    });
  }

  static int PushLimit(void* reader, int32_t limit, bool* applied) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(applied));
      ARROW_ASSIGN_OR_RAISE(Planner * planner, Handle<Planner>(reader));
      if (limit < 0) {
        return arrow::Status::Invalid("a pushed limit cannot be negative: ", limit);
      }
      ARROW_ASSIGN_OR_RAISE(*applied, planner->PushLimit(limit));
      return arrow::Status::OK();
    });
  }

  static int PruneColumns(void* reader, const char* const* columns, int64_t count,
                          bool* pruned) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(pruned));
      ARROW_ASSIGN_OR_RAISE(Planner * planner, Handle<Planner>(reader));
      ARROW_ASSIGN_OR_RAISE(std::vector<std::string> names, Strings(columns, count));
      ARROW_ASSIGN_OR_RAISE(*pruned, planner->PruneColumns(names));
      return arrow::Status::OK();
    });
  }

  static int Partitions(void* reader, SparkDsSink add_partition, void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Planner * planner, Handle<Planner>(reader));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const EncodedPlan> plan, planner->Planned());
      for (const std::string& partition : plan->partitions) {
        ARROW_RETURN_NOT_OK(Emit(add_partition, sink, partition));
      }
      return arrow::Status::OK();
    });
  }

  static int SerializeReader(void* reader, SparkDsSink set_state, void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Planner * planner, Handle<Planner>(reader));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const EncodedPlan> plan, planner->Planned());
      return Emit(set_state, sink, plan->state);
    });
  }

  static void CloseReader(void* reader) noexcept { delete static_cast<Planner*>(reader); }

  // Stream reader.

  static int CreateStreamReader(void* source, struct ArrowSchema* schema, void** reader) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(reader));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> read_schema, ImportSchema(schema));
      ARROW_RETURN_NOT_OK(ValidateSchema(*read_schema));
      ARROW_ASSIGN_OR_RAISE(Source * handle, Handle<Source>(source));
      if (!handle->capabilities.Contains(Capabilities::kMicroBatchRead)) {
        return arrow::Status::Invalid("source does not support micro-batch streaming reads");
      }
      ARROW_ASSIGN_OR_RAISE(auto stream,
                            handle->source.NewMicroBatchStream(MicroBatchStreamInfo(read_schema)));
      *reader = new Stream(std::move(stream), std::move(read_schema));
      return arrow::Status::OK();
    });
  }

  static int InitialOffset(void* reader, SparkDsSink set_offset, void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(std::string offset, stream->InitialOffset());
      return Emit(set_offset, sink, offset);
    });
  }

  static int LatestOffset(void* reader, SparkDsSink set_offset, void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(std::string offset, stream->LatestOffset());
      return Emit(set_offset, sink, offset);
    });
  }

  static int StreamPartitions(void* reader, const char* start, const char* end,
                              SparkDsSink add_partition, void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(std::string from, String(start));
      ARROW_ASSIGN_OR_RAISE(std::string to, String(end));
      ARROW_ASSIGN_OR_RAISE(std::vector<std::string> partitions, stream->Plan(from, to));
      for (const std::string& partition : partitions) {
        ARROW_RETURN_NOT_OK(Emit(add_partition, sink, partition));
      }
      return arrow::Status::OK();
    });
  }

  static int SerializeStreamReader(void* reader, SparkDsSink set_state, void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const std::string> state, stream->Serialized());
      return Emit(set_state, sink, *state);
    });
  }

  static int CommitOffset(void* reader, const char* end) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(std::string offset, String(end));
      return stream->CommitOffset(offset);
    });
  }

  static void CloseStreamReader(void* reader) noexcept { delete static_cast<Stream*>(reader); }

  // Partition reader.

  static int Read(const SparkDsTaskContext* task, const uint8_t* reader_state,
                  int64_t reader_state_length, const uint8_t* partition, int64_t partition_length,
                  struct ArrowArrayStream* out) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(out));
      ARROW_ASSIGN_OR_RAISE(std::string_view state_bytes, Bytes(reader_state, reader_state_length));
      ARROW_ASSIGN_OR_RAISE(std::string_view partition_bytes, Bytes(partition, partition_length));
      ARROW_ASSIGN_OR_RAISE(State state, DecodeState(state_bytes));
      if (state.streaming) {
        if constexpr (Traits::kMicroBatchRead) {
          return OpenPartition<typename Traits::StreamReaderFactory>(task, state, partition_bytes,
                                                                     out);
        } else {
          return arrow::Status::Invalid("source does not support micro-batch streaming reads");
        }
      }
      if constexpr (Traits::kBatchRead) {
        return OpenPartition<typename Traits::BatchReaderFactory>(task, state, partition_bytes,
                                                                  out);
      } else {
        return arrow::Status::Invalid("source does not support batch reads");
      }
    });
  }

  // Writer.

  static int CreateWriter(void* source, struct ArrowSchema* schema, const SparkDsWriteInfo* info,
                          void** writer) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(writer));
      ARROW_ASSIGN_OR_RAISE(WriteRequest request, ReadWriteInfo(info));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> write_schema, ImportSchema(schema));
      ARROW_RETURN_NOT_OK(ValidateSchema(*write_schema));
      ARROW_ASSIGN_OR_RAISE(Source * handle, Handle<Source>(source));
      Capabilities capabilities = handle->capabilities;
      bool truncate = request.mode == WriteMode::kTruncate;
      if (!request.query_id) {
        if constexpr (Traits::kBatchWrite) {
          if (!capabilities.Contains(Capabilities::kBatchWrite)) {
            return arrow::Status::Invalid("source does not support batch writes");
          }
          if (truncate &&
              !(Traits::kBatchTruncate && capabilities.Contains(Capabilities::kTruncate))) {
            return arrow::Status::Invalid("source does not support truncate");
          }
          ARROW_ASSIGN_OR_RAISE(auto write,
                                handle->source.NewWrite(WriteInfo(write_schema, request.mode)));
          using Writer = DriverWriter<typename Traits::BatchWrite, /*kStreaming=*/false>;
          *writer = static_cast<WriterHandle*>(new Writer(std::move(write), write_schema));
          return arrow::Status::OK();
        } else {
          return arrow::Status::Invalid("source does not support batch writes");
        }
      }
      if constexpr (Traits::kStreamingWrite) {
        if (!capabilities.Contains(Capabilities::kStreamingWrite)) {
          return arrow::Status::Invalid("source does not support streaming writes");
        }
        if (truncate &&
            !(Traits::kStreamingTruncate && capabilities.Contains(Capabilities::kTruncate))) {
          return arrow::Status::Invalid(
              "source does not support truncate in streaming writes (the output mode complete)");
        }
        StreamingWriteInfo streaming_info(write_schema, request.mode, *request.query_id);
        ARROW_ASSIGN_OR_RAISE(auto write, handle->source.NewStreamingWrite(streaming_info));
        using Writer = DriverWriter<typename Traits::StreamingWrite, /*kStreaming=*/true>;
        *writer = static_cast<WriterHandle*>(new Writer(std::move(write), write_schema));
        return arrow::Status::OK();
      } else {
        return arrow::Status::Invalid("source does not support streaming writes");
      }
    });
  }

  static int SerializeWriter(void* writer, SparkDsSink set_state, void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(WriterHandle * handle, Handle<WriterHandle>(writer));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const std::string> state, handle->Serialized());
      return Emit(set_state, sink, *state);
    });
  }

  static int Commit(void* writer, int64_t epoch_id, const uint8_t* const* messages,
                    const int64_t* message_lengths, int64_t num_messages) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(WriterHandle * handle, Handle<WriterHandle>(writer));
      ARROW_ASSIGN_OR_RAISE(auto values, Messages(messages, message_lengths, num_messages));
      return handle->Commit(epoch_id, values);
    });
  }

  static int Abort(void* writer, int64_t epoch_id, const uint8_t* const* messages,
                   const int64_t* message_lengths, int64_t num_messages) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(WriterHandle * handle, Handle<WriterHandle>(writer));
      ARROW_ASSIGN_OR_RAISE(auto values, Messages(messages, message_lengths, num_messages));
      return handle->Abort(epoch_id, values);
    });
  }

  static void CloseWriter(void* writer) noexcept { delete static_cast<WriterHandle*>(writer); }

  // Data writer.

  static int CreateDataWriter(const SparkDsTaskContext* task, const uint8_t* writer_state,
                              int64_t writer_state_length, int64_t epoch_id,
                              void** data_writer) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(data_writer));
      ARROW_ASSIGN_OR_RAISE(std::string_view bytes, Bytes(writer_state, writer_state_length));
      ARROW_ASSIGN_OR_RAISE(State state, DecodeState(bytes));
      ARROW_RETURN_NOT_OK(CheckEpoch(state.streaming, epoch_id));
      TaskWriter* writer = nullptr;
      if (state.streaming) {
        if constexpr (Traits::kStreamingWrite) {
          ARROW_ASSIGN_OR_RAISE(writer, OpenTaskWriter<typename Traits::StreamingWriterFactory>(
                                            task, state, epoch_id));
        } else {
          return arrow::Status::Invalid("source does not support streaming writes");
        }
      } else {
        if constexpr (Traits::kBatchWrite) {
          ARROW_ASSIGN_OR_RAISE(writer, OpenTaskWriter<typename Traits::BatchWriterFactory>(
                                            task, state, std::nullopt));
        } else {
          return arrow::Status::Invalid("source does not support batch writes");
        }
      }
      *data_writer = writer;
      return arrow::Status::OK();
    });
  }

  static int Write(void* data_writer, struct ArrowArray* array,
                   struct ArrowSchema* schema) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(TaskWriter * writer, Handle<TaskWriter>(data_writer));
      return writer->Write(array, schema);
    });
  }

  static int CommitDataWriter(void* data_writer, SparkDsSink set_message, void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      if (set_message == nullptr) {
        return arrow::Status::Invalid("null result sink");
      }
      ARROW_ASSIGN_OR_RAISE(TaskWriter * writer, Handle<TaskWriter>(data_writer));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const std::string> message, writer->Commit());
      return Emit(set_message, sink, *message);
    });
  }

  static int AbortDataWriter(void* data_writer) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(TaskWriter * writer, Handle<TaskWriter>(data_writer));
      return writer->Abort();
    });
  }

  static void CloseDataWriter(void* data_writer) noexcept {
    delete static_cast<TaskWriter*>(data_writer);
  }
};

/// Negotiates the ABI version, installs the log of the host, and runs the Init of the source.
template <typename S>
int Initialize(const SparkDsHost* host, const SparkDsLibrary** library) noexcept {
  int result = Call([&]() -> arrow::Status {
    ARROW_RETURN_NOT_OK(RequireOutput(library));
    *library = nullptr;
    ARROW_ASSIGN_OR_RAISE(int64_t size, StructSize(host));
    auto version = FieldOf<int32_t>(host, size, offsetof(SparkDsHost, abi_version));
    if (!version) {
      return arrow::Status::Invalid("host does not provide an ABI version");
    }
    if (*version < SPARK_DS_ABI_VERSION) {
      return arrow::Status::Invalid("host ABI ", *version, " does not support ABI ",
                                    SPARK_DS_ABI_VERSION);
    }
    auto log = FieldOf<HostLog>(host, size, offsetof(SparkDsHost, log));
    host_log.store(log.value_or(nullptr), std::memory_order_release);
    HostInfo info;
    info.abi_version = *version;
    auto spark_version = FieldOf<const char*>(host, size, offsetof(SparkDsHost, spark_version));
    if (spark_version && *spark_version != nullptr) {
      info.spark_version = std::string(*spark_version);
    }
    ARROW_RETURN_NOT_OK(S::Init(info));
    *library = Exports<S>::Library();
    return arrow::Status::OK();
  });
  if (result != 0 && host != nullptr) {
    // Spark only reports that initialization failed, so log why.
    int64_t size;
    std::memcpy(&size, host, sizeof(size));
    auto log = FieldOf<HostLog>(host, size, offsetof(SparkDsHost, log));
    if (log && *log != nullptr) {
      (*log)(SPARK_DS_LOG_ERROR, LastError());
    }
  }
  return result;
}

}  // namespace spark::native::internal
