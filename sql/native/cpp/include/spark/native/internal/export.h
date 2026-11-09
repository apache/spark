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
/// callbacks. Export only spark_ds_init, as spark_add_native_data_source in CMake does, with
/// hidden symbol visibility and a linker version script on Linux, or -exported_symbol on macOS:
/// hidden visibility alone still exports the instantiations of the templates of Arrow and of the
/// standard library, which other libraries of the process could then interpose. The shared Arrow
/// library itself is not private to the library.
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

  /// The operations that the source implements.
  static constexpr Capabilities kCapabilities = [] {
    Capabilities capabilities = Capabilities::Empty();
    if (kBatchRead) capabilities = capabilities | Capabilities::kBatchRead;
    if (kMicroBatchRead) capabilities = capabilities | Capabilities::kMicroBatchRead;
    if constexpr (kBatchWrite) {
      capabilities = capabilities | Capabilities::kBatchWrite;
      if (BatchWrite::kSupportsTruncate) capabilities = capabilities | Capabilities::kBatchTruncate;
    }
    if constexpr (kStreamingWrite) {
      capabilities = capabilities | Capabilities::kStreamingWrite;
      if (StreamingWrite::kSupportsTruncate) {
        capabilities = capabilities | Capabilities::kStreamingTruncate;
      }
    }
    return capabilities;
  }();

  /// Keeps the implemented operations of a declaration, so that a source can narrow its
  /// operations but never add one, and each truncation only with its write.
  static constexpr Capabilities Effective(Capabilities declared) {
    Capabilities capabilities = declared & kCapabilities;
    if (!capabilities.Contains(Capabilities::kBatchWrite)) {
      capabilities = capabilities.Without(Capabilities::kBatchTruncate);
    }
    if (!capabilities.Contains(Capabilities::kStreamingWrite)) {
      capabilities = capabilities.Without(Capabilities::kStreamingTruncate);
    }
    return capabilities;
  }

  /// The native capabilities of a source with these operations. Every stream supports admission
  /// control: its default read limit is all the available data unless it overrides
  /// MicroBatchStream::DefaultReadLimit.
  static constexpr uint64_t Declared(Capabilities capabilities) {
    uint64_t bits = capabilities.bits();
    if (capabilities.Contains(Capabilities::kMicroBatchRead)) {
      bits |= SPARK_DS_ADMISSION_CONTROL;
    }
    return bits;
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
        table.push_limit = &PushLimit;
        table.prune_columns = &PruneColumns;
        table.partitions = &Partitions;
        table.close_reader = &CloseReader;
      }
      if constexpr (Traits::kMicroBatchRead) {
        table.create_stream_reader = &CreateStreamReader;
        table.initial_offset = &InitialOffset;
        table.latest_offset = &LatestOffset;
        table.stream_partitions = &StreamPartitions;
        table.commit_offset = &CommitOffset;
        table.close_stream_reader = &CloseStreamReader;
        table.default_read_limit = &DefaultReadLimit;
      }
      // Batch scans and micro-batches share the executor reader.
      if constexpr (Traits::kBatchRead || Traits::kMicroBatchRead) {
        table.read = &Read;
      }
      // Batch and streaming writes share all the callbacks of writers.
      if constexpr (Traits::kBatchWrite || Traits::kStreamingWrite) {
        table.create_writer = &CreateWriter;
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

  /// Passes the partitions of a plan, and its state, to the sinks of Spark.
  static arrow::Status EmitPlan(const EncodedPlan& plan, SparkDsSink add_partition,
                                SparkDsSink set_state, void* sink) {
    for (const std::string& partition : plan.partitions) {
      ARROW_RETURN_NOT_OK(Emit(add_partition, sink, partition));
    }
    return Emit(set_state, sink, plan.state);
  }

  // Data source.

  static int Create(const SparkDsCreateInfo* info, uint64_t* capabilities, void** source) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(capabilities));
      ARROW_RETURN_NOT_OK(RequireOutput(source));
      // An operation starts on the driver: follow the level of the log of Spark.
      FollowLogLevel();
      ARROW_ASSIGN_OR_RAISE(CreateInfo create_info, ReadCreateInfo(info));
      ARROW_ASSIGN_OR_RAISE(S created, S::Make(create_info));
      Capabilities declared = created.capabilities();
      auto handle = std::make_unique<Source>(Source{std::move(created), Capabilities::Empty()});
      handle->capabilities = Traits::Effective(declared);
      *capabilities = Traits::Declared(handle->capabilities);
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

  static int CreateReader(void* source, struct ArrowSchema* schema, const SparkDsReaderInfo* info,
                          void** reader) noexcept {
    (void)info;
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(reader));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> read_schema, ImportSchema(schema));
      ARROW_RETURN_NOT_OK(ValidateSchema(*read_schema));
      ARROW_ASSIGN_OR_RAISE(Source * handle, Handle<Source>(source));
      if (!handle->capabilities.Contains(Capabilities::kBatchRead)) {
        return arrow::Status::Invalid("source does not support batch reads");
      }
      ARROW_ASSIGN_OR_RAISE(auto builder, handle->source.NewScan(ScanInfo(read_schema)));
      *reader = new Planner(std::move(builder));
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

  static int PruneColumns(void* reader, const char* const* columns, int64_t count) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Planner * planner, Handle<Planner>(reader));
      ARROW_ASSIGN_OR_RAISE(std::vector<std::string> names, Strings(columns, count));
      return planner->PruneColumns(names);
    });
  }

  static int Partitions(void* reader, const SparkDsPartitionsInfo* info, SparkDsSink add_partition,
                        SparkDsSink set_state, void* sink) noexcept {
    (void)info;
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Planner * planner, Handle<Planner>(reader));
      ARROW_ASSIGN_OR_RAISE(EncodedPlan plan, planner->Plan());
      return EmitPlan(plan, add_partition, set_state, sink);
    });
  }

  static void CloseReader(void* reader) noexcept { delete static_cast<Planner*>(reader); }

  // Stream reader.

  static int CreateStreamReader(void* source, struct ArrowSchema* schema,
                                const SparkDsStreamReaderInfo* info, void** reader) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(reader));
      ARROW_ASSIGN_OR_RAISE(std::string checkpoint_location, ReadCheckpointLocation(info));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> read_schema, ImportSchema(schema));
      ARROW_RETURN_NOT_OK(ValidateSchema(*read_schema));
      ARROW_ASSIGN_OR_RAISE(Source * handle, Handle<Source>(source));
      if (!handle->capabilities.Contains(Capabilities::kMicroBatchRead)) {
        return arrow::Status::Invalid("source does not support micro-batch streaming reads");
      }
      MicroBatchStreamInfo stream_info(std::move(read_schema), std::move(checkpoint_location));
      ARROW_ASSIGN_OR_RAISE(auto stream, handle->source.NewMicroBatchStream(stream_info));
      *reader = new Stream(std::move(stream));
      return arrow::Status::OK();
    });
  }

  static int InitialOffset(void* reader, const SparkDsInitialOffsetInfo* info,
                           SparkDsSink set_offset, void* sink) noexcept {
    (void)info;
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(std::string offset, stream->InitialOffset());
      return Emit(set_offset, sink, offset);
    });
  }

  static int DefaultReadLimit(void* reader, SparkDsReadLimit* limit) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(ReadLimit value, stream->DefaultReadLimit());
      return SetReadLimit(limit, value);
    });
  }

  static int LatestOffset(void* reader, const SparkDsLatestOffsetInfo* info, SparkDsSink set_offset,
                          void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(LatestOffsetInfo<std::string> request, ReadLatestOffsetInfo(info));
      ARROW_ASSIGN_OR_RAISE(std::string offset, stream->LatestOffset(request));
      return Emit(set_offset, sink, offset);
    });
  }

  static int StreamPartitions(void* reader, const SparkDsStreamPartitionsInfo* info,
                              SparkDsSink add_partition, SparkDsSink set_state,
                              void* sink) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(SparkDsStreamPartitionsInfo range,
                            Input(info, "SparkDsStreamPartitionsInfo"));
      ARROW_ASSIGN_OR_RAISE(std::string from, String(range.start));
      ARROW_ASSIGN_OR_RAISE(std::string to, String(range.end));
      ARROW_ASSIGN_OR_RAISE(EncodedPlan plan, stream->Plan(from, to));
      return EmitPlan(plan, add_partition, set_state, sink);
    });
  }

  static int CommitOffset(void* reader, const SparkDsCommitOffsetInfo* info) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Stream * stream, Handle<Stream>(reader));
      ARROW_ASSIGN_OR_RAISE(SparkDsCommitOffsetInfo commit, Input(info, "SparkDsCommitOffsetInfo"));
      ARROW_ASSIGN_OR_RAISE(std::string offset, String(commit.end));
      return stream->CommitOffset(offset);
    });
  }

  static void CloseStreamReader(void* reader) noexcept { delete static_cast<Stream*>(reader); }

  // Partition reader.

  static int Read(const SparkDsTaskContext* task, const SparkDsReadInfo* info,
                  struct ArrowArrayStream* out) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(out));
      // A task starts on an executor: follow the level of the log of Spark.
      FollowLogLevel();
      ARROW_ASSIGN_OR_RAISE(SparkDsReadInfo read, Input(info, "SparkDsReadInfo"));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ImportSchema(read.schema));
      ARROW_ASSIGN_OR_RAISE(std::string_view state,
                            Bytes(read.reader_state, read.reader_state_length));
      ARROW_ASSIGN_OR_RAISE(std::string_view partition,
                            Bytes(read.partition, read.partition_length));
      switch (read.kind) {
        case SPARK_DS_READ_BATCH:
          if constexpr (Traits::kBatchRead) {
            return OpenPartition<typename Traits::ScanBuilder::Factory>(task, std::move(schema),
                                                                        state, partition, out);
          } else {
            return arrow::Status::Invalid("source does not support batch reads");
          }
        case SPARK_DS_READ_MICRO_BATCH:
          if constexpr (Traits::kMicroBatchRead) {
            return OpenPartition<typename Traits::MicroBatchStream::Factory>(
                task, std::move(schema), state, partition, out);
          } else {
            return arrow::Status::Invalid("source does not support micro-batch streaming reads");
          }
        default:
          return arrow::Status::Invalid("unsupported kind of read: ", read.kind);
      }
    });
  }

  // Writer.

  static int CreateWriter(void* source, struct ArrowSchema* schema, const SparkDsWriteInfo* info,
                          SparkDsSink set_state, void* sink, void** writer) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(writer));
      if (set_state == nullptr) {
        return arrow::Status::Invalid("null result sink");
      }
      ARROW_ASSIGN_OR_RAISE(WriteRequest request, ReadWriteInfo(info));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> write_schema, ImportSchema(schema));
      ARROW_RETURN_NOT_OK(ValidateSchema(*write_schema));
      ARROW_ASSIGN_OR_RAISE(Source * handle, Handle<Source>(source));
      ARROW_ASSIGN_OR_RAISE(CreatedWriter created,
                            NewWriter(*handle, std::move(write_schema), std::move(request)));
      // The sink is not null, so Spark gets the state of the writer.
      ARROW_RETURN_NOT_OK(Emit(set_state, sink, created.state));
      *writer = created.writer.release();
      return arrow::Status::OK();
    });
  }

  /// Creates a batch write, or the write of a micro-batch of the streaming query of the request,
  /// with the state of its task writers.
  static arrow::Result<CreatedWriter> NewWriter(Source& handle,
                                                std::shared_ptr<arrow::Schema> schema,
                                                WriteRequest request) {
    Capabilities capabilities = handle.capabilities;
    bool truncate = request.mode == WriteMode::kTruncate;
    // The capabilities contain a write only if the source implements it.
    if (!request.query_id) {
      if constexpr (Traits::kBatchWrite) {
        if (capabilities.Contains(Capabilities::kBatchWrite)) {
          if (truncate && !capabilities.Contains(Capabilities::kBatchTruncate)) {
            return arrow::Status::Invalid("source does not support the save mode overwrite");
          }
          ARROW_ASSIGN_OR_RAISE(
              auto write, handle.source.NewWrite(WriteInfo(std::move(schema), request.mode)));
          return DriverWriter<typename Traits::BatchWrite, /*kStreaming=*/false>::Make(
              std::move(write));
        }
      }
      return arrow::Status::Invalid("source does not support batch writes");
    }
    if constexpr (Traits::kStreamingWrite) {
      if (capabilities.Contains(Capabilities::kStreamingWrite)) {
        if (truncate && !capabilities.Contains(Capabilities::kStreamingTruncate)) {
          return arrow::Status::Invalid("source does not support the output mode complete");
        }
        StreamingWriteInfo streaming_info(std::move(schema), request.mode, *request.query_id);
        ARROW_ASSIGN_OR_RAISE(auto write, handle.source.NewStreamingWrite(streaming_info));
        return DriverWriter<typename Traits::StreamingWrite, /*kStreaming=*/true>::Make(
            std::move(write));
      }
    }
    return arrow::Status::Invalid("source does not support streaming writes");
  }

  static int Commit(void* writer, const SparkDsFinishWriteInfo* info) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(WriterHandle * handle, Handle<WriterHandle>(writer));
      ARROW_ASSIGN_OR_RAISE(SparkDsFinishWriteInfo finish, Input(info, "SparkDsFinishWriteInfo"));
      ARROW_ASSIGN_OR_RAISE(auto messages,
                            Messages(finish.messages, finish.message_lengths, finish.num_messages));
      return handle->Commit(finish.epoch_id, messages);
    });
  }

  static int Abort(void* writer, const SparkDsFinishWriteInfo* info) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(WriterHandle * handle, Handle<WriterHandle>(writer));
      ARROW_ASSIGN_OR_RAISE(SparkDsFinishWriteInfo finish, Input(info, "SparkDsFinishWriteInfo"));
      // Spark rejected what create_writer returned, before any task ran. The SDK returns a single
      // state, so this does not happen; the write is destroyed at close, as when its creation
      // fails.
      if (finish.rejected != 0) {
        return arrow::Status::OK();
      }
      ARROW_ASSIGN_OR_RAISE(auto messages,
                            Messages(finish.messages, finish.message_lengths, finish.num_messages));
      return handle->Abort(finish.epoch_id, messages);
    });
  }

  static void CloseWriter(void* writer) noexcept { delete static_cast<WriterHandle*>(writer); }

  // Data writer.

  static int CreateDataWriter(const SparkDsTaskContext* task, const SparkDsDataWriterInfo* info,
                              void** data_writer) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(data_writer));
      // A task starts on an executor: follow the level of the log of Spark.
      FollowLogLevel();
      ARROW_ASSIGN_OR_RAISE(SparkDsDataWriterInfo writer_info,
                            Input(info, "SparkDsDataWriterInfo"));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema,
                            ImportSchema(writer_info.schema));
      ARROW_ASSIGN_OR_RAISE(std::string_view state,
                            Bytes(writer_info.writer_state, writer_info.writer_state_length));
      // The epoch ID tells them apart: it is -1 for a batch write.
      int64_t epoch_id = writer_info.epoch_id;
      TaskWriter* writer = nullptr;
      if (epoch_id == -1) {
        if constexpr (Traits::kBatchWrite) {
          ARROW_ASSIGN_OR_RAISE(writer, OpenTaskWriter<typename Traits::BatchWrite::Factory>(
                                            task, std::move(schema), state, std::nullopt));
        } else {
          return arrow::Status::Invalid("source does not support batch writes");
        }
      } else if (epoch_id >= 0) {
        if constexpr (Traits::kStreamingWrite) {
          ARROW_ASSIGN_OR_RAISE(writer, OpenTaskWriter<typename Traits::StreamingWrite::Factory>(
                                            task, std::move(schema), state, epoch_id));
        } else {
          return arrow::Status::Invalid("source does not support streaming writes");
        }
      } else {
        return arrow::Status::Invalid("invalid epoch ID ", epoch_id);
      }
      *data_writer = writer;
      return arrow::Status::OK();
    });
  }

  static int Write(void* data_writer, struct ArrowArray* array) noexcept {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(TaskWriter * writer, Handle<TaskWriter>(data_writer));
      return writer->Write(array);
    });
  }

  static int CommitDataWriter(void* data_writer, const SparkDsFinishDataWriterInfo* info,
                              SparkDsSink set_message, void* sink) noexcept {
    (void)info;
    return Call([&]() -> arrow::Status {
      if (set_message == nullptr) {
        return arrow::Status::Invalid("null result sink");
      }
      ARROW_ASSIGN_OR_RAISE(TaskWriter * writer, Handle<TaskWriter>(data_writer));
      ARROW_ASSIGN_OR_RAISE(std::string message, writer->Commit());
      return Emit(set_message, sink, message);
    });
  }

  static int AbortDataWriter(void* data_writer, const SparkDsFinishDataWriterInfo* info) noexcept {
    (void)info;
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
    auto log_enabled = FieldOf<HostLogEnabled>(host, size, offsetof(SparkDsHost, is_log_enabled));
    host_log_enabled.store(log_enabled.value_or(nullptr), std::memory_order_release);
    host_log.store(log.value_or(nullptr), std::memory_order_release);
    FollowLogLevel();
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
