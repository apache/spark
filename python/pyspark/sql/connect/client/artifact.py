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
from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.sql.connect.utils import check_dependencies
from pyspark.sql.connect.logging import logger

check_dependencies(__name__)

import hashlib
import importlib
import io
import sys
import os
import zlib
from itertools import chain
from typing import List, Iterable, BinaryIO, Iterator, Optional, Tuple
import abc
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname
from functools import cached_property

import grpc

import pyspark.sql.connect.proto as proto
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib


JAR_PREFIX: str = "jars"
PYFILE_PREFIX: str = "pyfiles"
ARCHIVE_PREFIX: str = "archives"
FILE_PREFIX: str = "files"
FORWARD_TO_FS_PREFIX: str = "forward_to_fs"
CACHE_PREFIX: str = "cache"


class LocalData(metaclass=abc.ABCMeta):
    """
    Payload stored on this machine.
    """

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

        # Check that the file can be read
        # so that incorrect references can be discovered during Artifact creation,
        # and not at the point of consumption.

        with self.stream():
            pass

    @cached_property
    def size(self) -> int:
        return os.path.getsize(self.path)

    def stream(self) -> BinaryIO:
        return open(self.path, "rb")


class InMemory(LocalData):
    """
    Payload stored in memory.
    """

    def __init__(self, blob: bytes):
        self.blob = blob

    @cached_property
    def size(self) -> int:
        return len(self.blob)

    def stream(self) -> BinaryIO:
        return io.BytesIO(self.blob)


class Artifact:
    def __init__(self, path: str, storage: LocalData):
        assert not Path(path).is_absolute(), f"Bad path: {path}"
        self.path = path
        self.storage = storage

    @cached_property
    def size(self) -> int:
        if isinstance(self.storage, LocalData):
            return self.storage.size
        else:
            raise PySparkRuntimeError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={"operation": f"{self.storage} storage"},
            )


def new_jar_artifact(file_name: str, storage: LocalData) -> Artifact:
    return _new_artifact(JAR_PREFIX, ".jar", file_name, storage)


def new_pyfile_artifact(file_name: str, storage: LocalData) -> Artifact:
    assert any(file_name.endswith(s) for s in (".py", ".zip", ".egg", ".jar"))
    return _new_artifact(PYFILE_PREFIX, "", file_name, storage)


def new_archive_artifact(file_name: str, storage: LocalData) -> Artifact:
    return _new_artifact(ARCHIVE_PREFIX, "", file_name, storage)


def new_file_artifact(file_name: str, storage: LocalData) -> Artifact:
    return _new_artifact(FILE_PREFIX, "", file_name, storage)


def new_cache_artifact(id: str, storage: LocalData) -> Artifact:
    return _new_artifact(CACHE_PREFIX, "", id, storage)


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

    def __init__(
        self,
        user_id: Optional[str],
        session_id: str,
        channel: grpc.Channel,
        metadata: Iterable[Tuple[str, str]],
    ):
        self._user_context = proto.UserContext()
        if user_id is not None:
            self._user_context.user_id = user_id
        self._stub = grpc_lib.SparkConnectServiceStub(channel)
        self._session_id = session_id
        self._metadata = metadata

    def _parse_artifacts(
        self, path_or_uri: str, pyfile: bool, archive: bool, file: bool
    ) -> List[Artifact]:
        # Currently only local files with .jar extension is supported.
        parsed = urlparse(path_or_uri)
        # Check if it is a file from the scheme
        if parsed.scheme == "":
            # Similar with Utils.resolveURI.
            fragment = parsed.fragment
            parsed = urlparse(Path(url2pathname(parsed.path)).absolute().as_uri())
            parsed = parsed._replace(fragment=fragment)

        if parsed.scheme == "file":
            local_path = url2pathname(parsed.path)
            name = Path(local_path).name
            if pyfile and name.endswith(".py"):
                artifact = new_pyfile_artifact(name, LocalFile(local_path))
                importlib.invalidate_caches()
            elif pyfile and (
                name.endswith(".zip") or name.endswith(".egg") or name.endswith(".jar")
            ):
                sys.path.insert(1, local_path)
                artifact = new_pyfile_artifact(name, LocalFile(local_path))
                importlib.invalidate_caches()
            elif archive and (
                name.endswith(".zip")
                or name.endswith(".jar")
                or name.endswith(".tar.gz")
                or name.endswith(".tgz")
                or name.endswith(".tar")
            ):
                assert any(name.endswith(s) for s in (".zip", ".jar", ".tar.gz", ".tgz", ".tar"))

                if parsed.fragment != "":
                    # Minimal fix for the workaround of fragment handling in URI.
                    # This has a limitation - hash(#) in the file name would not work.
                    if "#" in local_path:
                        raise PySparkValueError(
                            errorClass="VALUE_ALLOWED",
                            messageParameters={
                                "arg_name": "artifact path",
                                "disallowed_value": "#",
                            },
                        )
                    name = f"{name}#{parsed.fragment}"

                artifact = new_archive_artifact(name, LocalFile(local_path))
            elif file:
                artifact = new_file_artifact(name, LocalFile(local_path))
            elif name.endswith(".jar"):
                artifact = new_jar_artifact(name, LocalFile(local_path))
            else:
                raise PySparkRuntimeError(
                    errorClass="UNSUPPORTED_OPERATION",
                    messageParameters={"operation": f"{local_path} file format"},
                )
            return [artifact]
        raise PySparkRuntimeError(
            errorClass="UNSUPPORTED_OPERATION",
            messageParameters={"operation": f"{parsed.scheme} scheme"},
        )

    def _parse_forward_to_fs_artifacts(self, local_path: str, dest_path: str) -> List[Artifact]:
        abs_path: Path = Path(local_path).absolute()
        # TODO: Support directory path.
        assert abs_path.is_file(), "local path must be a file path."
        storage = LocalFile(str(abs_path))

        assert Path(dest_path).is_absolute(), "destination FS path must be an absolute path."

        # The `dest_path` is an absolute path, to add the FORWARD_TO_FS_PREFIX,
        # we cannot use `os.path.join`
        artifact_path = FORWARD_TO_FS_PREFIX + dest_path
        return [Artifact(artifact_path, storage)]

    def _create_requests(
        self, *path: str, pyfile: bool, archive: bool, file: bool
    ) -> Iterator[proto.AddArtifactsRequest]:
        """Separated for the testing purpose."""

        # It's crucial that this function is not generator, but only returns generator.
        # This way we are doing artifact parsing within the original caller thread
        # And not during grpc consuming iterator, allowing for much better error reporting.

        artifacts: Iterator[Artifact] = chain(
            *(self._parse_artifacts(p, pyfile=pyfile, archive=archive, file=file) for p in path)
        )

        def generator() -> Iterator[proto.AddArtifactsRequest]:
            try:
                yield from self._add_artifacts(artifacts)
            except Exception as e:
                logger.error(f"Failed to submit addArtifacts request: {e}")
                raise

        return generator()

    def _retrieve_responses(
        self, requests: Iterator[proto.AddArtifactsRequest]
    ) -> proto.AddArtifactsResponse:
        """Separated for the testing purpose."""
        return self._stub.AddArtifacts(requests, metadata=self._metadata)

    def _request_add_artifacts(self, requests: Iterator[proto.AddArtifactsRequest]) -> None:
        response: proto.AddArtifactsResponse = self._retrieve_responses(requests)
        summaries: List[proto.AddArtifactsResponse.ArtifactSummary] = []

        for summary in response.artifacts:
            summaries.append(summary)
            # TODO(SPARK-42658): Handle responses containing CRC failures.

    def add_artifacts(self, *path: str, pyfile: bool, archive: bool, file: bool) -> None:
        """
        Add a single artifact to the session.
        Currently only local files with .jar extension is supported.
        """
        requests: Iterator[proto.AddArtifactsRequest] = self._create_requests(
            *path, pyfile=pyfile, archive=archive, file=file
        )

        self._request_add_artifacts(requests)

    def _add_forward_to_fs_artifacts(self, local_path: str, dest_path: str) -> None:
        requests: Iterator[proto.AddArtifactsRequest] = self._add_artifacts(
            self._parse_forward_to_fs_artifacts(local_path, dest_path)
        )
        self._request_add_artifacts(requests)

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
            with artifact.storage.stream() as stream:
                binary = stream.read()
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
        with artifact.storage.stream() as stream:
            for chunk in iter(lambda: stream.read(ArtifactManager.CHUNK_SIZE), b""):
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

    def is_cached_artifact(self, hash: str) -> bool:
        """
        Ask the server either any artifact with `hash` has been cached at the server side or not.
        """
        artifactName = CACHE_PREFIX + "/" + hash
        request = proto.ArtifactStatusesRequest(
            user_context=self._user_context, session_id=self._session_id, names=[artifactName]
        )
        resp: proto.ArtifactStatusesResponse = self._stub.ArtifactStatus(
            request, metadata=self._metadata
        )
        status = resp.statuses.get(artifactName)
        return status.exists if status is not None else False

    def cache_artifact(self, blob: bytes) -> str:
        """
        Cache the give blob at the session.
        """
        hash = hashlib.sha256(blob).hexdigest()
        if not self.is_cached_artifact(hash):
            requests = self._add_artifacts([new_cache_artifact(hash, InMemory(blob))])
            response: proto.AddArtifactsResponse = self._retrieve_responses(requests)
            summaries: List[proto.AddArtifactsResponse.ArtifactSummary] = []

            for summary in response.artifacts:
                summaries.append(summary)
                # TODO(SPARK-42658): Handle responses containing CRC failures.

        return hash
