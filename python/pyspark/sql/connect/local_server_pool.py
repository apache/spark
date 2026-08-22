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

"""Filesystem-backed state model for local Spark Connect server pools.

This internal foundation owns member identity, directory locking, state-file access, and
claiming. Process lifecycle and server acquisition are layered on top in follow-up changes.
"""

import contextlib
import hashlib
import json
import math
import os
import shutil
import sys
from typing import Any, Dict, List, Optional, Tuple

from pyspark.errors import PySparkValueError


# Environment variables that shape the JVM the launcher boots through
# sbin/start-connect-server.sh -> spark-daemon.sh -> load-spark-env.sh / spark-submit.
# SPARK_CONF_DIR selects the spark-env.sh and spark-defaults.conf that seed the server; the
# rest feed the classpath, heap, and JVM options. This is a curated set, not an exhaustive
# one: the launcher inherits the whole environment, so it names the inputs that most commonly
# differ between runs rather than every variable a server could read.
#
# PATH is a deliberate omission: bin/spark-class prefers ${JAVA_HOME}/bin/java and only falls
# back to the first java on PATH, so two runs with different JDKs first on PATH and no JAVA_HOME
# would share a member. The identity already tracks PATH indirectly through shutil.which for the
# interpreters, and PATH is too volatile to fingerprint whole; a run needing a specific JDK
# should set JAVA_HOME.
_JVM_ENV_VARS = (
    "SPARK_CONF_DIR",
    "JAVA_HOME",
    "SPARK_DIST_CLASSPATH",
    "SPARK_DAEMON_MEMORY",
    "SPARK_DRIVER_MEMORY",
    "SPARK_SUBMIT_OPTS",
    "SPARK_DAEMON_JAVA_OPTS",
)


def pool_fingerprint(master: str, seed_conf: Dict[str, Any]) -> str:
    """The identity of a pool member: a curated set of inputs that shape the server a run would
    have booted for itself. A run only claims members whose fingerprint equals its own, so a
    pre-booted JVM is never handed to a run it would not have produced. The set is curated
    rather than complete because the launcher inherits the full environment (see
    ``_JVM_ENV_VARS``) -- it covers the inputs that most commonly differ between runs.

    Besides the master and the seeded confs, this covers the working directory (unset warehouse
    and Derby metastore locations resolve relative to it), the PySpark installation, the Python
    interpreters the server would run UDFs and Python data sources with, and the environment
    variables that shape the launched JVM.
    """

    def resolved(command: str) -> str:
        # Relative commands resolve through PATH server-side; fold that in so equal command
        # strings cannot stand for different interpreters on different PATHs.
        return shutil.which(command) or command

    # Two server code paths resolve the Python interpreter with opposite precedence, so a run
    # changing only one of these variables would still have booted a different server. Include
    # both resolutions: SparkConnectPlanner.pythonExec (Connect Python UDFs) prefers
    # PYSPARK_PYTHON, while PythonUtils.defaultPythonExec (Python data sources) prefers
    # PYSPARK_DRIVER_PYTHON. Both fall back to python3 and treat an empty value as set, matching
    # the Scala sys.env.getOrElse chains.
    udf_python = resolved(
        os.environ.get("PYSPARK_PYTHON", os.environ.get("PYSPARK_DRIVER_PYTHON", "python3"))
    )
    data_source_python = resolved(
        os.environ.get("PYSPARK_DRIVER_PYTHON", os.environ.get("PYSPARK_PYTHON", "python3"))
    )
    spark_home = os.environ.get("SPARK_HOME")
    identity = [
        master,
        sorted((str(k), str(v)) for k, v in seed_conf.items()),
        os.getcwd(),
        sys.executable,
        udf_python,
        data_source_python,
        os.path.realpath(__file__),
        os.path.realpath(spark_home) if spark_home else "",
        os.environ.get("PYTHONPATH", ""),
        [os.environ.get(var, "") for var in _JVM_ENV_VARS],
    ]
    return hashlib.sha256(json.dumps(identity).encode("utf-8")).hexdigest()


# The end of year 9999 UTC, as a Unix timestamp. ``created`` is a wall-clock ``time.time()``
# reading, so no real clock reaches this for millennia; rejecting values beyond it keeps a
# corrupt far-future timestamp from looking perpetually fresh to age-based reaping in the
# layers above, which measure a member's age as ``time.time() - created``.
_MAX_CREATED = 253402300799


class PoolMember:
    """One published pool server, wrapping its ``server-<uid>.json`` record."""

    def __init__(self, data: Dict[str, Any]):
        record = dict(data)
        for key in ("host", "token", "spark_version", "fingerprint"):
            if not isinstance(record[key], str) or not record[key]:
                raise PySparkValueError(f"{key} must be a nonempty string")
        for key in ("port", "pid"):
            value = record[key]
            if isinstance(value, bool) or not isinstance(value, int):
                raise PySparkValueError(f"{key} must be an integer")
        created = record["created"]
        if isinstance(created, bool) or not isinstance(created, (int, float)):
            raise PySparkValueError("created must be a number")
        created = float(created)
        if not 1 <= record["port"] <= 65535:
            raise PySparkValueError("port is out of range")
        if record["pid"] <= 0:
            raise PySparkValueError("pid must be positive")
        if not math.isfinite(created) or not 0 <= created <= _MAX_CREATED:
            raise PySparkValueError(f"created must be a finite timestamp in [0, {_MAX_CREATED}]")
        self.host: str = record["host"]
        self.port: int = record["port"]
        self.token: str = record["token"]
        self.pid: int = record["pid"]
        self.spark_version: str = record["spark_version"]
        self.fingerprint: str = record["fingerprint"]
        self.created: float = created
        # Set when this process claims the member; the path of its claimed-<pid>-<uid>.json.
        self.claim_path: Optional[str] = None

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> Optional["PoolMember"]:
        """Parse a published member record, returning ``None`` when it is malformed."""
        try:
            return cls(data)
        except (KeyError, TypeError, ValueError, OverflowError):
            return None

    @property
    def url(self) -> str:
        return f"sc://{self.host}:{self.port}"

    def is_usable(self) -> bool:
        """Whether this member has a matching Spark version, live process, and open port. Uses
        the same liveness and reachability probes as the reuse path (see ``local_server``), so
        the pool and reuse discovery agree on when a recorded server is still good."""
        from pyspark.version import __version__
        from pyspark.sql.connect.local_server import _pid_alive, _port_open

        if self.spark_version != __version__ or not _pid_alive(self.pid):
            return False
        return _port_open(self.host, self.port)


class PoolDirectory:
    """Path layout, file access, and the cross-process lock of one pool directory.

    Used as a context manager that holds the directory's exclusive lock:

        directory = PoolDirectory()
        with directory:
            path = directory.pending_path(uid)
            directory.write_json(path, data)
            stored = directory.read_json(path)
            directory.rename(path, directory.server_path(uid))

    Entering the context creates the directory and acquires its lock. Callers then use path
    builders and the locked accessors to enumerate, read, write, rename, or remove state. Exiting
    the context releases the lock.

    Pool operations are infrequent, so one exclusive lock for every state transition is simpler
    than a finer-grained scheme. A context can be entered again after exiting, allowing callers
    to release the lock between polling attempts so other processes can update the directory.
    """

    def __init__(self, path: Optional[str] = None):
        if path is None:
            path = os.environ.get("SPARK_LOCAL_CONNECT_POOL_DIR")
        if path is None:
            from pyspark.sql.connect.local_server import runtime_dir

            path = os.path.join(runtime_dir(), "pool")
        self.path = os.path.abspath(path)
        self._lock_fd: Optional[int] = None

    def __enter__(self) -> "PoolDirectory":
        import fcntl

        # Not reentrant: a nested enter would os.open a second fd and flock(LOCK_EX) would block
        # forever against the fd this process already holds. Fail loudly instead of deadlocking.
        assert self._lock_fd is None, "PoolDirectory is not reentrant"
        os.makedirs(self.path, mode=0o700, exist_ok=True)
        # Re-assert privacy for an existing override directory: state files contain auth tokens,
        # and directory write access would allow replacing them or bypassing the shared lock.
        os.chmod(self.path, 0o700)
        lock_fd = os.open(os.path.join(self.path, ".lock"), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.fchmod(lock_fd, 0o600)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(lock_fd)
            raise
        self._lock_fd = lock_fd
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        assert self._lock_fd is not None
        os.close(self._lock_fd)  # closing releases the lock
        self._lock_fd = None

    def _assert_locked(self) -> None:
        assert self._lock_fd is not None, "PoolDirectory must be used as a context manager"

    # Path builders; these do not touch the filesystem and need no lock.

    def pending_path(self, uid: str) -> str:
        return os.path.join(self.path, f"pending-{uid}.json")

    def conf_path(self, uid: str) -> str:
        return os.path.join(self.path, f"conf-{uid}.json")

    def server_path(self, uid: str) -> str:
        return os.path.join(self.path, f"server-{uid}.json")

    def claimed_path(self, client_pid: int, uid: str) -> str:
        return os.path.join(self.path, f"claimed-{client_pid}-{uid}.json")

    def retired_path(self, uid: str) -> str:
        return os.path.join(self.path, f"retired-{uid}.json")

    def member_dir(self, uid: str) -> str:
        return os.path.join(self.path, f"member-{uid}")

    # uids are generated as ``uuid.uuid4().hex[:12]`` (see the acquisition layer), so a valid
    # uid is a nonempty run of lowercase hex. Validating the shape keeps editor droppings such
    # as ``member-abc.json.swp`` and empty stems like ``server-.json`` from becoming phantom uids.
    _UID_CHARS = frozenset("0123456789abcdef")

    @classmethod
    def _is_uid(cls, uid: str) -> bool:
        return bool(uid) and all(c in cls._UID_CHARS for c in uid)

    @classmethod
    def _split_claimed(cls, stem: str) -> Optional[Tuple[str, str]]:
        """Split a well-formed ``claimed-<pid>-<uid>`` stem (without the ``.json`` suffix) into
        ``(client_pid, uid)`` as strings, or ``None`` otherwise. The pid is returned unparsed:
        ``parse_entry`` classifies over every directory entry and must never raise, and
        ``str.isdigit()`` accepts characters ``int()`` rejects (e.g. superscripts), so the
        ``isascii()`` guard keeps the eventual ``int()`` in ``claiming_pid`` total."""
        if not stem.startswith("claimed-"):
            return None
        client_pid, sep, uid = stem[len("claimed-") :].partition("-")
        if not sep or not (client_pid.isascii() and client_pid.isdigit()) or not cls._is_uid(uid):
            return None
        return client_pid, uid

    @classmethod
    def parse_entry(cls, name: str) -> Tuple[Optional[str], Optional[str]]:
        """The ``(kind, uid)`` of a pool directory entry, ``(None, None)`` for anything
        else (the lock file, editor droppings, entries with a malformed uid, ...)."""
        if name.startswith("member-"):
            uid = name[len("member-") :]
            return ("member", uid) if cls._is_uid(uid) else (None, None)
        if not name.endswith(".json"):
            return None, None
        stem = name[: -len(".json")]
        for kind in ("pending", "conf", "server", "retired"):
            if stem.startswith(kind + "-"):
                uid = stem[len(kind) + 1 :]
                return (kind, uid) if cls._is_uid(uid) else (None, None)
        claimed = cls._split_claimed(stem)
        return ("claimed", claimed[1]) if claimed is not None else (None, None)

    @classmethod
    def claiming_pid(cls, claimed_path: str) -> int:
        """The client pid recorded in a ``claimed-<pid>-<uid>.json`` file name."""
        name = os.path.basename(claimed_path)
        stem = name[: -len(".json")] if name.endswith(".json") else name
        claimed = cls._split_claimed(stem)
        assert claimed is not None, f"not a claimed entry: {claimed_path!r}"
        return int(claimed[0])

    # Locked accessors.

    def uids(self) -> List[str]:
        self._assert_locked()
        seen = []
        for name in self._entries():
            _, uid = self.parse_entry(name)
            if uid is not None and uid not in seen:
                seen.append(uid)
        return seen

    def states(self, uid: str) -> Dict[str, str]:
        """The state entries currently existing for ``uid``, as ``{kind: path}`` with kinds
        ``pending``, ``conf``, ``server``, ``claimed``, ``retired``, and ``member`` (the
        member's directory)."""
        self._assert_locked()
        found: Dict[str, str] = {}
        for name in self._entries():
            kind, entry_uid = self.parse_entry(name)
            if kind is not None and entry_uid == uid:
                # At most one entry per kind. Claiming renames a single file into place (see the
                # claiming layer), so two claimed entries for one uid means the pid a reaper would
                # read via claiming_pid is ambiguous; surface that rather than pick one silently.
                assert kind not in found, f"duplicate {kind} entries for uid {uid}"
                found[kind] = os.path.join(self.path, name)
        return found

    def paths_of_kind(self, kind: str) -> List[Tuple[str, str]]:
        """All ``(uid, path)`` of one state kind."""
        self._assert_locked()
        return [
            (uid, os.path.join(self.path, name))
            for name in self._entries()
            for entry_kind, uid in (self.parse_entry(name),)
            if entry_kind == kind and uid is not None
        ]

    def _entries(self) -> List[str]:
        try:
            return sorted(os.listdir(self.path))
        except OSError:
            return []

    def read_json(self, path: str) -> Optional[Dict[str, Any]]:
        """``None`` for files that are missing or unreadable -- callers treat both like the
        state not existing, and the reaping rules remove unreadable leftovers."""
        self._assert_locked()
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        self._assert_locked()
        # 0600 like the reuse discovery file: server entries hold the auth token.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            os.fchmod(fd, 0o600)
            f.write(json.dumps(data))

    def rename(self, src: str, dst: str) -> None:
        self._assert_locked()
        os.rename(src, dst)

    def remove(self, path: str) -> None:
        self._assert_locked()
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)

    def remove_member_dir(self, uid: str) -> None:
        self._assert_locked()
        shutil.rmtree(self.member_dir(uid), ignore_errors=True)


class ServerPool:
    """Claims members from one pool directory; lifecycle operations are added later."""

    def __init__(self, directory: Optional[PoolDirectory] = None):
        self._directory = directory or PoolDirectory()

    def claim(self, fingerprint: str) -> Optional[PoolMember]:
        """Claim the oldest usable member with this fingerprint, or ``None``. The rename to
        ``claimed-<pid>-<uid>.json`` marks the member as owned by this process; the reaping
        rules use that pid to retire members whose client died without releasing them. The
        caller must hold the directory lock so selection and rename form one transition.

        Ordering is by ``created``, a wall-clock ``time.time()`` reading. It is comparable
        across the independent processes that publish members, which ``time.monotonic()`` is
        not, at the cost that a backward clock step (NTP, suspend/resume) can perturb the order.
        Ties break by the stable ``sorted()`` over the sorted directory listing, so the order is
        well defined but only approximately FIFO, not guaranteed.

        ``is_usable`` runs under the held lock and does blocking network I/O -- up to a 0.5s
        connect for each candidate that is live but not accepting connections. The candidate
        count is bounded by ``spark.local.connect.pool.size``, which is user-tunable, so a large
        pool widens the window the lock is held; the reaping rules keep stale members from
        accumulating without bound."""
        candidates = []
        for uid, path in self._directory.paths_of_kind("server"):
            data = self._directory.read_json(path)
            member = PoolMember.from_data(data) if data is not None else None
            if member is not None and member.fingerprint == fingerprint:
                candidates.append((member, uid, path))
        candidates.sort(key=lambda c: c[0].created)
        for member, uid, path in candidates:
            if not member.is_usable():
                continue  # left for the reaping rules to retire
            claim_path = self._directory.claimed_path(os.getpid(), uid)
            self._directory.rename(path, claim_path)
            member.claim_path = claim_path
            return member
        return None
