#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

import os
import zlib
from itertools import chain
from typing import List, Iterable, BinaryIO, Iterator, Optional
import abc
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname
from functools import cached_property

import grpc

import pyspark.sql.connect.proto as proto
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib


JAR_PREFIX: str = "jars"


class LocalData(metaclass=abc.ABCMeta):
    """
    Payload stored on this machine.
    """

    @cached_property
    @abc.abstractmethod
    def stream(self) -> BinaryIO:
        pass

    @cached_property
    @abc.abstractmethod
    def size(self) -> int:
        pass


class LocalFile(LocalData):
    """
    Payload stored in a local file.
    """

    def __init__(self, path: str):
        self.path = path
        self._size: int
        self._stream: int

    @cached_property
    def size(self) -> int:
        return os.path.getsize(self.path)

    @cached_property
    def stream(self) -> BinaryIO:
        return open(self.path, "rb")


class Artifact:
    """
    Payload stored in memory.
    """

    def __init__(self, path: str, storage: LocalData):
        assert not Path(path).is_absolute(), f"Bad path: {path}"
        self.path = path
        self.storage = storage

    @cached_property
    def size(self) -> int:
        if isinstance(self.storage, LocalData):
            return self.storage.size
        else:
            raise RuntimeError(f"Unsupported storage {type(self.storage)}")


def new_jar_artifact(file_name: str, storage: LocalData) -> Artifact:
    return _new_artifact(JAR_PREFIX, ".jar", file_name, storage)


def _new_artifact(
    prefix: str, required_suffix: str, file_name: str, storage: LocalData
) -> Artifact:
    assert not Path(file_name).is_absolute()
    assert file_name.endswith(required_suffix)
    return Artifact(os.path.join(prefix, file_name), storage)


class ArtifactManager:
    """
    The Artifact Manager is responsible for handling and transferring artifacts from the local
    client to the server (local/remote).

    Parameters
    ----------
    user_id : str, optional
        User ID.
    session_id: str
        An unique identifier of the session which the artifact manager belongs to.
    channel: grpc.Channel
        GRPC Channel instance.
    """

    # Using the midpoint recommendation of 32KiB for chunk size as specified in
    # https://github.com/grpc/grpc.github.io/issues/371.
    CHUNK_SIZE: int = 32 * 1024

    def __init__(self, user_id: Optional[str], session_id: str, channel: grpc.Channel):
        assert user_id is not None
        self._user_context = proto.UserContext(user_id=user_id)
        self._stub = grpc_lib.SparkConnectServiceStub(channel)
        self._session_id = session_id

    def _parse_artifacts(self, path_or_uri: str) -> List[Artifact]:
        # Currently only local files with .jar extension is supported.
        uri = path_or_uri
        if urlparse(path_or_uri).scheme == "":  # Is path?
            uri = Path(path_or_uri).absolute().as_uri()
        parsed = urlparse(uri)
        if parsed.scheme == "file":
            local_path = url2pathname(parsed.path)
            name = Path(local_path).name
            if name.endswith(".jar"):
                artifact = new_jar_artifact(name, LocalFile(local_path))
            else:
                raise RuntimeError(f"Unsupported file format: {local_path}")
            return [artifact]
        raise RuntimeError(f"Unsupported scheme: {uri}")

    def _create_requests(self, *path: str) -> Iterator[proto.AddArtifactsRequest]:
        """Separated for the testing purpose."""
        return self._add_artifacts(chain(*(self._parse_artifacts(p) for p in path)))

    def _retrieve_responses(
        self, requests: Iterator[proto.AddArtifactsRequest]
    ) -> proto.AddArtifactsResponse:
        """Separated for the testing purpose."""
        return self._stub.AddArtifacts(requests)

    def add_artifacts(self, *path: str) -> None:
        """
        Add a single artifact to the session.
        Currently only local files with .jar extension is supported.
        """
        requests: Iterator[proto.AddArtifactsRequest] = self._create_requests(*path)
        response: proto.AddArtifactsResponse = self._retrieve_responses(requests)
        summaries: List[proto.AddArtifactsResponse.ArtifactSummary] = []

        for summary in response.artifacts:
            summaries.append(summary)
            # TODO(SPARK-42658): Handle responses containing CRC failures.

    def _add_artifacts(self, artifacts: Iterable[Artifact]) -> Iterator[proto.AddArtifactsRequest]:
        """
        Add a number of artifacts to the session.
        """

        current_batch: List[Artifact] = []
        current_batch_size = 0

        def add_to_batch(dep: Artifact, size: int) -> None:
            nonlocal current_batch
            nonlocal current_batch_size

            current_batch.append(dep)
            current_batch_size += size

        def write_batch() -> Iterator[proto.AddArtifactsRequest]:
            nonlocal current_batch
            nonlocal current_batch_size

            yield from self._add_batched_artifacts(current_batch)
            current_batch = []
            current_batch_size = 0

        for artifact in artifacts:
            data = artifact.storage
            size = data.size
            if size > ArtifactManager.CHUNK_SIZE:
                # Payload can either be a batch OR a single chunked artifact.
                # Write batch if non-empty before chunking current artifact.
                if len(current_batch) > 0:
                    yield from write_batch()
                yield from self._add_chunked_artifact(artifact)
            else:
                if current_batch_size + size > ArtifactManager.CHUNK_SIZE:
                    yield from write_batch()
                add_to_batch(artifact, size)

        if len(current_batch) > 0:
            yield from write_batch()

    def _add_batched_artifacts(
        self, artifacts: Iterable[Artifact]
    ) -> Iterator[proto.AddArtifactsRequest]:
        """
        Add a batch of artifacts to the stream. All the artifacts in this call are packaged into a
        single :class:`proto.AddArtifactsRequest`.
        """
        artifact_chunks = []

        for artifact in artifacts:
            binary = artifact.storage.stream.read()
            crc32 = zlib.crc32(binary)
            data = proto.AddArtifactsRequest.ArtifactChunk(data=binary, crc=crc32)
            artifact_chunks.append(
                proto.AddArtifactsRequest.SingleChunkArtifact(name=artifact.path, data=data)
            )

        # Write the request once
        yield proto.AddArtifactsRequest(
            session_id=self._session_id,
            user_context=self._user_context,
            batch=proto.AddArtifactsRequest.Batch(artifacts=artifact_chunks),
        )

    def _add_chunked_artifact(self, artifact: Artifact) -> Iterator[proto.AddArtifactsRequest]:
        """
        Add a artifact in chunks to the stream. The artifact's data is spread out over multiple
        :class:`proto.AddArtifactsRequest requests`.
        """
        initial_batch = True
        # Integer division that rounds up to the nearest whole number.
        get_num_chunks = int(
            (artifact.size + (ArtifactManager.CHUNK_SIZE - 1)) / ArtifactManager.CHUNK_SIZE
        )

        # Consume stream in chunks until there is no data left to read.
        for chunk in iter(lambda: artifact.storage.stream.read(ArtifactManager.CHUNK_SIZE), b""):
            if initial_batch:
                # First RPC contains the `BeginChunkedArtifact` payload (`begin_chunk`).
                yield proto.AddArtifactsRequest(
                    session_id=self._session_id,
                    user_context=self._user_context,
                    begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                        name=artifact.path,
                        total_bytes=artifact.size,
                        num_chunks=get_num_chunks,
                        initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(
                            data=chunk, crc=zlib.crc32(chunk)
                        ),
                    ),
                )
                initial_batch = False
            else:
                # Subsequent RPCs contains the `ArtifactChunk` payload (`chunk`).
                yield proto.AddArtifactsRequest(
                    session_id=self._session_id,
                    user_context=self._user_context,
                    chunk=proto.AddArtifactsRequest.ArtifactChunk(
                        data=chunk, crc=zlib.crc32(chunk)
                    ),
                )
