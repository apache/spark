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

"""Filesystem-backed state and lifecycle model for local Spark Connect server pools.

This internal foundation owns member identity, directory locking, state-file access, claiming,
reaping, and retirement. Server acquisition is layered on top in a follow-up change.
"""

import contextlib
import hashlib
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from pyspark.errors import PySparkValueError
from pyspark.sql.connect.local_server import (
    Discovery,
    ServerLauncher,
    _is_local_connect_server,
    _pid_alive,
    _port_open,
    _process_command,
    runtime_dir,
)

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


class _PoolStateRecord:
    """Validation shared by JSON-backed pool state records."""

    # The end of year 9999 UTC, as a Unix timestamp. Pool timestamps are wall-clock
    # ``time.time()`` readings, so rejecting larger values keeps corrupt far-future records
    # from looking perpetually fresh to age-based reaping.
    _MAX_TIMESTAMP = 253402300799

    @staticmethod
    def _positive_pid(value: Any) -> Optional[int]:
        """A positive integer process id, or ``None`` for malformed persisted data."""
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            return None
        return value

    @classmethod
    def _timestamp(cls, value: Any) -> Optional[float]:
        """A finite persisted wall-clock timestamp, or ``None`` when malformed."""
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        try:
            timestamp = float(value)
        except OverflowError:
            return None
        if not math.isfinite(timestamp) or not 0 <= timestamp <= cls._MAX_TIMESTAMP:
            return None
        return timestamp


@dataclass(frozen=True)
class PendingState(_PoolStateRecord):
    """Validated fields of a ``pending-<uid>.json`` launch record."""

    attendant_pid: int
    created: float
    fingerprint: str

    @classmethod
    def attendant_pid_from_data(cls, data: Optional[Dict[str, Any]]) -> Optional[int]:
        """Recover a valid attendant pid even when another record field is malformed."""
        return cls._positive_pid(data.get("attendant_pid")) if data is not None else None

    @classmethod
    def from_data(cls, data: Optional[Dict[str, Any]]) -> Optional["PendingState"]:
        if data is None:
            return None
        attendant_pid = cls.attendant_pid_from_data(data)
        created = cls._timestamp(data.get("created"))
        fingerprint = data.get("fingerprint")
        if (
            attendant_pid is None
            or created is None
            or not isinstance(fingerprint, str)
            or not fingerprint
        ):
            return None
        return cls(attendant_pid, created, fingerprint)


@dataclass(frozen=True)
class RetiredState(_PoolStateRecord):
    """Validated fields of a ``retired-<uid>.json`` shutdown record."""

    pid: int
    process_start_id: str
    retired: float
    signalled: bool = False

    @classmethod
    def pid_from_data(cls, data: Optional[Dict[str, Any]]) -> Optional[int]:
        """Recover a valid server pid even when the retirement time is malformed."""
        return cls._positive_pid(data.get("pid")) if data is not None else None

    @staticmethod
    def process_start_id_from_data(data: Optional[Dict[str, Any]]) -> Optional[str]:
        """Recover a process generation identifier from a malformed state record."""
        value = data.get("process_start_id") if data is not None else None
        return value if isinstance(value, str) and value else None

    @classmethod
    def retired_from_data(cls, data: Optional[Dict[str, Any]]) -> Optional[float]:
        """Recover a valid retirement timestamp from a malformed state record."""
        return cls._timestamp(data.get("retired")) if data is not None else None

    @staticmethod
    def signalled_from_data(data: Optional[Dict[str, Any]]) -> Optional[bool]:
        """Recover SIGTERM delivery state, defaulting legacy records to unsignalled."""
        if data is None:
            return None
        value = data.get("signalled", False)
        return value if isinstance(value, bool) else None

    @classmethod
    def from_data(cls, data: Optional[Dict[str, Any]]) -> Optional["RetiredState"]:
        if data is None:
            return None
        pid = cls.pid_from_data(data)
        process_start_id = cls.process_start_id_from_data(data)
        retired = cls.retired_from_data(data)
        signalled = cls.signalled_from_data(data)
        if pid is None or process_start_id is None or retired is None or signalled is None:
            return None
        return cls(pid, process_start_id, retired, signalled)

    def as_data(self) -> Dict[str, Any]:
        return {
            "pid": self.pid,
            "process_start_id": self.process_start_id,
            "retired": self.retired,
            "signalled": self.signalled,
        }


class PoolMember(_PoolStateRecord):
    """One published pool server, wrapping its ``server-<uid>.json`` record."""

    def __init__(self, data: Dict[str, Any]):
        record = dict(data)
        for key in ("host", "token", "spark_version", "fingerprint", "process_start_id"):
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
        if not math.isfinite(created) or not 0 <= created <= self._MAX_TIMESTAMP:
            raise PySparkValueError(
                f"created must be a finite timestamp in [0, {self._MAX_TIMESTAMP}]"
            )
        self.host: str = record["host"]
        self.port: int = record["port"]
        self.token: str = record["token"]
        self.pid: int = record["pid"]
        self.spark_version: str = record["spark_version"]
        self.fingerprint: str = record["fingerprint"]
        self.process_start_id: str = record["process_start_id"]
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

    @classmethod
    def pid_from_data(cls, data: Optional[Dict[str, Any]]) -> Optional[int]:
        """Recover a valid server pid even when another member field is malformed."""
        return cls._positive_pid(data.get("pid")) if data is not None else None

    @staticmethod
    def process_start_id_from_data(data: Optional[Dict[str, Any]]) -> Optional[str]:
        """Recover a process generation identifier from a malformed member record."""
        value = data.get("process_start_id") if data is not None else None
        return value if isinstance(value, str) and value else None

    @staticmethod
    def client_process_start_id_from_data(data: Optional[Dict[str, Any]]) -> Optional[str]:
        """Recover the claiming client's process generation from a claimed record."""
        value = data.get("client_process_start_id") if data is not None else None
        return value if isinstance(value, str) and value else None

    @property
    def url(self) -> str:
        return f"sc://{self.host}:{self.port}"

    def is_usable(self) -> bool:
        """Whether this member has a matching Spark version, live process, and open port. Uses
        the same liveness and reachability probes as the reuse path (see ``local_server``), so
        the pool and reuse discovery agree on when a recorded server is still good."""
        from pyspark.version import __version__

        if (
            self.spark_version != __version__
            or not _pid_alive(self.pid)
            or ServerPool._process_start_id(self.pid) != self.process_start_id
        ):
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

    _STATE_TEMP_PREFIX = ".pool-state-"

    def __init__(self, path: Optional[str] = None):
        if path is None:
            path = os.environ.get("SPARK_LOCAL_CONNECT_POOL_DIR")
        if path is None:
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
        try:
            # A process killed between writing and replacing an atomic state-file update can
            # leave its private temporary file behind. No writer can still be active once this
            # lock is acquired, so these leftovers are always safe to discard.
            for name in self._entries():
                if name.startswith(self._STATE_TEMP_PREFIX):
                    with contextlib.suppress(OSError):
                        os.remove(os.path.join(self.path, name))
        except BaseException:
            os.close(lock_fd)
            self._lock_fd = None
            raise
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
        except FileNotFoundError:
            return []

    def read_json(self, path: str) -> Optional[Dict[str, Any]]:
        """``None`` for files that are missing or malformed.

        Other I/O failures propagate so lifecycle callers retry instead of mistaking temporarily
        unavailable state for a completed transition.
        """
        self._assert_locked()
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """Atomically replace ``path`` with private JSON state.

        ``path`` must be on the same filesystem as this directory. The replacement is atomic
        against process failure; durability across power loss is not promised.
        """
        self._assert_locked()
        # Write through a temporary file so a process dying during a state transition leaves
        # either the old record or the complete new one. Server entries hold an auth token, so
        # the temporary and final files both remain private.
        fd, temp_path = tempfile.mkstemp(prefix=self._STATE_TEMP_PREFIX, dir=self.path)
        try:
            try:
                state_file = os.fdopen(fd, "w")
            except BaseException:
                # fdopen only takes ownership after it returns successfully.
                os.close(fd)
                raise
            with state_file as f:
                os.fchmod(fd, 0o600)
                f.write(json.dumps(data))
            os.replace(temp_path, path)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_path)
            raise

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
    """Claims, reaps, and retires members of one pool directory."""

    # A pending marker older than this belongs to a launch that hung. A launch can spend the
    # maximum in both the script and readiness phases; leave another minute for setup and
    # scheduling so a slow but healthy launch is never stopped by the janitor.
    _LAUNCH_TIMEOUT_SECONDS = ServerLauncher._MAX_STARTUP_SECONDS + 60
    # A retired server still alive after the grace period is hard-killed. With a process handle,
    # tracking is removed only once it is gone, replaced, or successfully signalled. PID-less
    # malformed state uses the give-up age as its bounded recovery window.
    _RETIRE_KILL_AFTER_SECONDS = 30
    _RETIRE_GIVE_UP_AFTER_SECONDS = 600
    # Preserve a failed launch's logs for diagnosis before collecting its unreferenced directory.
    _MEMBER_DIR_GC_AGE_SECONDS = 24 * 3600
    _DEFAULT_IDLE_TIMEOUT_SECONDS = 1800
    _PROCESS_INSPECTION_TIMEOUT_SECONDS = 5
    _PROC_STAT_START_TIME_INDEX = 19
    _ATTENDANT_MODULE = "pyspark.sql.connect.local_server_pool"

    def __init__(self, directory: Optional[PoolDirectory] = None):
        self._directory = directory or PoolDirectory()

    @classmethod
    def _process_start_id(cls, pid: int) -> Optional[str]:
        """An identifier for this generation of ``pid``, or ``None`` if it cannot be read.

        Linux exposes a boot id and a process start tick, which together survive PID reuse and
        distinguish records left across a reboot. Other POSIX systems use ``ps``'s absolute
        start time. The fallback has one-second precision but still closes the long-lived
        stale-record window; signalling performs this check immediately before acting.
        """
        if pid <= 0:
            return None
        if sys.platform.startswith("linux"):
            try:
                with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as boot_id_file:
                    boot_id = boot_id_file.read().strip()
                with open(f"/proc/{pid}/stat", encoding="utf-8") as stat_file:
                    stat = stat_file.read()
            except (OSError, UnicodeError):
                return None
            _, separator, fields_text = stat.rpartition(") ")
            fields = fields_text.split()
            if not boot_id or not separator or len(fields) <= cls._PROC_STAT_START_TIME_INDEX:
                return None
            start_tick = fields[cls._PROC_STAT_START_TIME_INDEX]
            if not (start_tick.isascii() and start_tick.isdigit()):
                return None
            return f"linux:{boot_id}:{start_tick}"

        env = dict(os.environ)
        env["LC_ALL"] = "C"
        try:
            result = subprocess.run(
                ["ps", "-ww", "-p", str(pid), "-o", "lstart="],
                capture_output=True,
                text=True,
                timeout=cls._PROCESS_INSPECTION_TIMEOUT_SECONDS,
                env=env,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        started = " ".join(result.stdout.split())
        return f"ps:{started}" if result.returncode == 0 and started else None

    @classmethod
    def _same_process_generation(
        cls, pid: Optional[int], process_start_id: Optional[str]
    ) -> Optional[bool]:
        """Whether ``pid`` is alive and still has its recorded process generation."""
        if pid is None or not _pid_alive(pid):
            return False
        if process_start_id is None:
            return None
        current_start_id = cls._process_start_id(pid)
        if current_start_id is None:
            return None
        return current_start_id == process_start_id

    @classmethod
    def _same_server_instance(cls, pid: int, process_start_id: str) -> Optional[bool]:
        """Whether ``pid`` is still the recorded Connect server process generation."""
        same_generation = cls._same_process_generation(pid, process_start_id)
        if same_generation is not True:
            return same_generation
        return _is_local_connect_server(pid)

    @staticmethod
    def _signal(pid: int, sig: int) -> bool:
        """Best-effort signal; ``False`` when the process is already gone or not ours."""
        if pid <= 0:
            return False
        try:
            os.kill(pid, sig)
            return True
        except (OSError, OverflowError):
            return False

    @classmethod
    def _signal_server(cls, pid: int, process_start_id: str, sig: int) -> bool:
        """Signal only the recorded generation of the managed Connect server."""
        return cls._same_server_instance(pid, process_start_id) is True and cls._signal(pid, sig)

    @classmethod
    def _idle_timeout(cls) -> int:
        """Seconds an unclaimed member may sit before it is retired.

        Zero or a negative value disables idle retirement. Read the environment wherever
        reaping runs so clients and attendants use the same source of truth.
        """
        try:
            return int(os.environ["SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT"])
        except (KeyError, ValueError):
            return cls._DEFAULT_IDLE_TIMEOUT_SECONDS

    @classmethod
    def _is_pool_attendant(cls, pid: int, uid: str) -> Optional[bool]:
        """Whether ``pid`` is still the pool attendant recorded for ``uid``.

        Returns ``None`` when the process cannot be inspected. A stale pending record can
        outlive its attendant long enough for the pid to be reused, so liveness alone is not
        sufficient before a janitor signals it.
        """
        command = _process_command(pid)
        if command is None:
            return None
        args = command.split()
        try:
            module_index = args.index(cls._ATTENDANT_MODULE)
            uid_index = args.index("--uid")
        except ValueError:
            return False
        return (
            module_index > 0
            and args[module_index - 1] == "-m"
            and "--attend" in args
            and uid_index + 1 < len(args)
            and args[uid_index + 1] == uid
        )

    @staticmethod
    def _attendant_group_alive(pgid: int) -> bool:
        """Whether a recorded attendant process group still has any members."""
        if pgid <= 0 or pgid == os.getpgrp():
            return False
        try:
            os.killpg(pgid, 0)
            return True
        except (ProcessLookupError, OverflowError):
            return False
        except OSError:
            # As with _pid_alive, an existing group we cannot signal still counts as alive.
            return True

    @staticmethod
    def _signal_attendant_group(pid: int, sig: int, *, leader_may_be_dead: bool = False) -> bool:
        """Signal a detached attendant and the launch subprocesses in its process group.

        A process group survives its leader while any child remains, and its id cannot be
        recycled during that time. When the recorded leader is already gone, signal the still
        owned group id directly; ``killpg`` then fails harmlessly if the group is empty.
        """
        if pid <= 0 or pid == os.getpgrp():
            return False
        try:
            try:
                if os.getpgid(pid) != pid:
                    return False
                if leader_may_be_dead and _pid_alive(pid):
                    # The caller observed a dead leader, but this pid now belongs to a live
                    # process. It was reused between checks, so do not signal its group.
                    return False
            except ProcessLookupError:
                if not leader_may_be_dead:
                    return False
            os.killpg(pid, sig)
            return True
        except (OSError, OverflowError):
            return False

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

        ``is_usable`` runs under the held lock and does blocking process inspection and network
        I/O -- potentially one ``ps`` and up to a 0.5s connect for each candidate. The candidate
        count is bounded by ``spark.local.connect.pool.size``, which is user-tunable, so a large
        pool widens the window the lock is held; the reaping rules keep stale members from
        accumulating without bound."""
        candidates = []
        for uid, path in self._directory.paths_of_kind("server"):
            data = self._directory.read_json(path)
            member = PoolMember.from_data(data) if data is not None else None
            if member is not None and member.fingerprint == fingerprint:
                assert data is not None
                candidates.append((member, uid, path, data))
        candidates.sort(key=lambda c: c[0].created)
        for member, uid, path, data in candidates:
            if not member.is_usable():
                continue  # left for the reaping rules to retire
            client_process_start_id = self._process_start_id(os.getpid())
            if client_process_start_id is not None:
                claimed_data = dict(data)
                claimed_data["client_process_start_id"] = client_process_start_id
                # Persist ownership before the atomic rename. If this process dies between the
                # write and rename, the extra field is harmless on the still-unclaimed record.
                self._directory.write_json(path, claimed_data)
            claim_path = self._directory.claimed_path(os.getpid(), uid)
            self._directory.rename(path, claim_path)
            member.claim_path = claim_path
            return member
        return None

    def janitor(self) -> None:
        """Reap leftovers of launches, clients, and attendants that died uncleanly. Every
        rule is idempotent, so successive passes from any process are safe."""
        for uid in self._directory.uids():
            self.reap(uid)

    def reap(self, uid: str) -> bool:
        """Apply the reaping rules to one member; ``True`` when nothing of it remains.
        Shared by the janitor (all members) and by each attendant supervising its own member.
        """
        states = self._directory.states(uid)
        if "conf" in states and "pending" not in states:
            # A later state proves the attendant consumed the seed. A conf-only record can be
            # left if its spawning client dies before starting or recording the attendant; use
            # the launch deadline to avoid accumulating those records forever.
            later_state = any(kind in states for kind in ("server", "claimed", "retired"))
            try:
                conf_expired = (
                    time.time() - os.path.getmtime(states["conf"]) > self._LAUNCH_TIMEOUT_SECONDS
                )
            except FileNotFoundError:
                conf_expired = True
            if later_state or conf_expired:
                self._directory.remove(states["conf"])
                states = self._directory.states(uid)
        had_retired = "retired" in states
        if "pending" in states:
            self._reap_pending(uid, states["pending"])
        states = self._directory.states(uid)
        if "server" in states:
            self._reap_server(uid, states["server"])
        states = self._directory.states(uid)
        if "claimed" in states:
            self._reap_claimed(uid, states["claimed"])
        states = self._directory.states(uid)
        if had_retired and "retired" in states:
            self._reap_retired(uid, states["retired"])

        remaining = self._directory.states(uid)
        if set(remaining) == {"member"}:
            # Nothing references the member directory anymore. The age gate keeps the logs
            # of a freshly failed launch around long enough to be looked at.
            try:
                expired = (
                    time.time() - os.path.getmtime(remaining["member"])
                    > self._MEMBER_DIR_GC_AGE_SECONDS
                )
            except FileNotFoundError:
                expired = True
            if expired:
                self._directory.remove_member_dir(uid)
                remaining = self._directory.states(uid)
        return not remaining

    def _reap_pending(self, uid: str, path: str) -> None:
        """A launch whose attendant died or hung: kill the attendant and whatever server
        spark-daemon.sh may have recorded for it, and withdraw the launch's bookkeeping so
        refills stop counting it."""
        data = self._directory.read_json(path)
        pending = PendingState.from_data(data)
        parsed_pid = pending.attendant_pid if pending is not None else None
        created = pending.created if pending is not None else None
        if pending is None and data is not None:
            # Preserve an independently valid pid when another field is corrupt.
            parsed_pid = PendingState.attendant_pid_from_data(data)
        age = time.time() - created if created is not None else self._LAUNCH_TIMEOUT_SECONDS + 1
        attendant_pid = parsed_pid if parsed_pid is not None else -1
        attendant_alive = _pid_alive(attendant_pid)
        if not attendant_alive:
            if pending is not None and not self._signal_attendant_group(
                attendant_pid, signal.SIGKILL, leader_may_be_dead=True
            ):
                # Keep the only launch-group handle when signalling failed but descendants
                # remain. If the pid was reused by a live process, withdraw the stale record
                # without signalling it.
                if not _pid_alive(attendant_pid) and self._attendant_group_alive(attendant_pid):
                    return
            self.abort_launch(uid)
        elif age > self._LAUNCH_TIMEOUT_SECONDS:
            is_attendant = self._is_pool_attendant(attendant_pid, uid)
            if is_attendant is None:
                return
            if is_attendant and not self._signal_attendant_group(attendant_pid, signal.SIGKILL):
                # Keep the record when an attendant that still appears live could not be
                # stopped, or when its leader exited during the attempt but children remain.
                if _pid_alive(attendant_pid) or self._attendant_group_alive(attendant_pid):
                    return
            self.abort_launch(uid)

    def abort_launch(self, uid: str) -> None:
        """Withdraw a failed launch and retire any server it started before failing."""
        states = self._directory.states(uid)
        pending_path = states.get("pending")
        server_path = states.get("server")
        if "retired" in states:
            # A previous abort can die after retiring the server but before removing the pending
            # marker. The retired record owns the server's process-generation identity; never
            # replace it with the weaker attendant record on the recovery pass.
            if pending_path is not None:
                self._directory.remove(pending_path)
            self._directory.remove(self._directory.conf_path(uid))
            return
        if server_path is not None:
            data = self._directory.read_json(server_path)
            server_pid, process_start_id = self._recover_server_handle(uid, data)
        else:
            server_pid = self._recorded_daemon_pid(uid)
            process_start_id = None
        retirement_source = server_path or pending_path
        retired_source = False
        if server_pid is not None and retirement_source is not None:
            # Keep shutdown state so a half-started JVM that ignores SIGTERM is escalated.
            self._retire(retirement_source, server_pid, process_start_id)
            retired_source = True
        if pending_path is not None and (not retired_source or pending_path != retirement_source):
            self._directory.remove(pending_path)
        self._directory.remove(self._directory.conf_path(uid))

    def _reap_server(self, uid: str, path: str) -> None:
        """A ready member that is unusable (dead, unreachable, version-mismatched after an
        upgrade, or an unreadable record) or has sat unclaimed past the idle timeout: retire
        it."""
        data = self._directory.read_json(path)
        member = PoolMember.from_data(data) if data is not None else None
        server_pid, process_start_id = self._recover_server_handle(uid, data)
        idle = self._idle_timeout()
        expired = member is not None and idle > 0 and time.time() - member.created > idle
        if member is None or expired or not member.is_usable():
            self._retire(path, server_pid, process_start_id)

    def _reap_claimed(self, uid: str, path: str) -> None:
        """A claimed member whose client died without releasing it (e.g. SIGKILL), or whose
        server died under its client: retire it. Claims of this live process are its own."""
        data = self._directory.read_json(path)
        server_pid, process_start_id = self._recover_server_handle(uid, data)
        client_pid = self._directory.claiming_pid(path)
        client_process_start_id = PoolMember.client_process_start_id_from_data(data)
        if client_process_start_id is None:
            # Records written by older clients have no process generation. Preserve their
            # liveness-only behavior rather than risking retirement of a live claim.
            client_alive = client_pid == os.getpid() or _pid_alive(client_pid)
        else:
            client_alive = (
                self._same_process_generation(client_pid, client_process_start_id) is not False
            )
        if not client_alive or self._same_process_generation(server_pid, process_start_id) is False:
            self._retire(path, server_pid, process_start_id)

    def _reap_retired(self, uid: str, path: str) -> None:
        """A retiring member: drop it once its server is gone, hard-kill the server if it
        hangs in shutdown, and stop tracking a record whose process generation was reused."""
        data = self._directory.read_json(path)
        record_pid = RetiredState.pid_from_data(data)
        record_process_start_id = RetiredState.process_start_id_from_data(data)
        retired = RetiredState.retired_from_data(data)
        signalled = RetiredState.signalled_from_data(data)
        server_pid = self._recover_server_pid(uid, record_pid)
        # A generation id is meaningful only with the pid from the same record. The daemon pid
        # file has no companion generation id, so never pair a recovered daemon pid with
        # unrelated record data.
        process_start_id = record_process_start_id if record_pid is not None else None
        now = time.time()
        if server_pid is None:
            # A daemon pid may appear after a partial publication, so retain the state for one
            # recovery window. After that there is no process handle left to act on or observe.
            if retired is None or retired > now:
                self._directory.write_json(
                    path,
                    {
                        "retired": now,
                        "signalled": False,
                    },
                )
            elif now - retired > self._RETIRE_GIVE_UP_AFTER_SECONDS:
                self._remove_retired(uid, path)
            return
        if not _pid_alive(server_pid):
            self._remove_retired(uid, path)
            return
        if process_start_id is None:
            # A pid without its persisted generation cannot safely be adopted: the pid may have
            # been recycled to an unrelated local Connect server. Retain the handle until it dies
            # rather than synthesizing authority to signal its current owner.
            return
        current_start_id = self._process_start_id(server_pid)
        if current_start_id is None:
            return
        if current_start_id != process_start_id:
            # The original server is gone and its PID now belongs to another process. Forget the
            # stale record without signalling the new owner.
            self._remove_retired(uid, path)
            return
        if retired is None or retired > now:
            # A crash while _retire rewrites the atomically renamed state can leave its old
            # payload. Restore a shutdown clock while preserving its process identity.
            self._directory.write_json(
                path,
                RetiredState(
                    server_pid,
                    process_start_id,
                    now,
                    signalled=signalled is True,
                ).as_data(),
            )
            return
        age = now - retired
        if age > self._RETIRE_GIVE_UP_AFTER_SECONDS:
            # Drop tracking only after proving the process was replaced or successfully issuing
            # the hard kill. Transient inspection or signalling failures remain retryable.
            is_server = self._same_server_instance(server_pid, process_start_id)
            if is_server is None or (is_server and not self._signal(server_pid, signal.SIGKILL)):
                return
            self._remove_retired(uid, path)
        elif age > self._RETIRE_KILL_AFTER_SECONDS:
            is_server = self._same_server_instance(server_pid, process_start_id)
            if is_server is False:
                self._remove_retired(uid, path)
            elif is_server is True:
                self._signal(server_pid, signal.SIGKILL)
        elif signalled is not True:
            # Retry a SIGTERM that was not confirmed during retirement. Persist success without
            # refreshing the retirement clock so repeated passes remain free and escalation is
            # still measured from the original transition.
            if self._signal_server(server_pid, process_start_id, signal.SIGTERM):
                self._directory.write_json(
                    path,
                    RetiredState(
                        server_pid,
                        process_start_id,
                        retired,
                        signalled=True,
                    ).as_data(),
                )

    def _remove_retired(self, uid: str, path: str) -> None:
        self._directory.remove(path)
        self._directory.remove_member_dir(uid)

    def _retire(
        self,
        state_path: str,
        server_pid: Optional[int],
        process_start_id: Optional[str],
    ) -> None:
        """Move a member into the retired state: signal its server and track the shutdown so
        :meth:`_reap_retired` can escalate if the JVM hangs."""
        _, uid = self._directory.parse_entry(os.path.basename(state_path))
        assert uid is not None
        signalled = False
        if server_pid is not None and process_start_id is not None:
            signalled = self._signal_server(server_pid, process_start_id, signal.SIGTERM)
        retired_path = self._directory.retired_path(uid)
        # Rename instead of removing the old state so a crash cannot leave a live server with
        # no state. If rewriting is interrupted, _reap_retired preserves the recoverable pid.
        self._directory.rename(state_path, retired_path)
        retired_data: Dict[str, Any] = {
            "retired": time.time(),
            "signalled": signalled,
        }
        if server_pid is not None:
            retired_data["pid"] = server_pid
        if process_start_id is not None:
            retired_data["process_start_id"] = process_start_id
        self._directory.write_json(retired_path, retired_data)

    def _recover_server_handle(
        self, uid: str, data: Optional[Dict[str, Any]]
    ) -> Tuple[Optional[int], Optional[str]]:
        """Recover a pid and only the process identity paired with that pid's record."""
        record_pid = PoolMember.pid_from_data(data)
        if record_pid is None:
            return self._recorded_daemon_pid(uid), None
        return record_pid, PoolMember.process_start_id_from_data(data)

    def _recover_server_pid(self, uid: str, record_pid: Optional[int]) -> Optional[int]:
        """Use a record pid when present, otherwise fall back to the daemon pid file.

        Full member validation intentionally rejects corrupt records, including out-of-range
        timestamps. Its independently valid pid remains paired with the record's generation id;
        a daemon pid has no generation id and is used only to retain state while it remains live.
        """
        return record_pid if record_pid is not None else self._recorded_daemon_pid(uid)

    def _recorded_daemon_pid(self, uid: str) -> Optional[int]:
        """The positive server pid recorded by spark-daemon.sh, if readable."""
        discovery = Discovery(os.path.join(self._directory.member_dir(uid), "connect-local.json"))
        return _PoolStateRecord._positive_pid(discovery.daemon_pid())

    def release(self, member: PoolMember) -> None:
        """Retire this process's claimed member; the shutdown completes in the background,
        watched by the member's attendant with the janitor as backstop.

        This method acquires the pool-directory lock and must not be called while the same pool
        directory is already locked, including through a different ``PoolDirectory`` instance.
        """
        assert member.claim_path is not None
        kind, uid = self._directory.parse_entry(os.path.basename(member.claim_path))
        assert kind == "claimed" and uid is not None
        if self._directory.claiming_pid(member.claim_path) != os.getpid():
            # A forked child inherits module globals and atexit handlers, but it must not retire
            # the server still claimed by its parent process.
            return
        with self._directory:
            # A janitor or concurrent purge may already have moved or removed the claim.
            # Release is idempotent with respect to that completed lifecycle transition.
            if self._directory.states(uid).get("claimed") == member.claim_path:
                self._retire(member.claim_path, member.pid, member.process_start_id)


# The member this client process has claimed, if any. A later acquisition layer populates it;
# keeping the idempotent release path here makes lifecycle ownership explicit.
_claimed_member: Optional[PoolMember] = None


def release_pooled_local_connect_server() -> None:
    """Retire this process's claimed pooled server; safe to call when there is none. The
    server winds down in the background while this client moves on."""
    global _claimed_member
    member = _claimed_member
    if member is not None:
        assert member.claim_path is not None
        directory = PoolDirectory(os.path.dirname(member.claim_path))
        ServerPool(directory).release(member)
        if _claimed_member is member:
            _claimed_member = None
