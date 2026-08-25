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
from pyspark.sql.connect.local_server import _is_local_connect_server, _pid_alive

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
class RetiredState(_PoolStateRecord):
    """Validated fields of a ``retired-<uid>.json`` shutdown record."""

    pid: int
    process_start_id: str
    retired: float

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
    def from_data(cls, data: Optional[Dict[str, Any]]) -> Optional["RetiredState"]:
        if data is None:
            return None
        pid = cls.pid_from_data(data)
        process_start_id = cls.process_start_id_from_data(data)
        retired = cls._timestamp(data.get("retired"))
        if pid is None or process_start_id is None or retired is None:
            return None
        return cls(pid, process_start_id, retired)

    def as_data(self) -> Dict[str, Any]:
        return {
            "pid": self.pid,
            "process_start_id": self.process_start_id,
            "retired": self.retired,
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

    @property
    def url(self) -> str:
        return f"sc://{self.host}:{self.port}"

    def is_usable(self) -> bool:
        """Whether this member has a matching Spark version, live process, and open port. Uses
        the same liveness and reachability probes as the reuse path (see ``local_server``), so
        the pool and reuse discovery agree on when a recorded server is still good."""
        from pyspark.sql.connect.local_server import _pid_alive, _port_open
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
        except (FileNotFoundError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        self._assert_locked()
        # Write through a temporary file so a process dying during a state transition leaves
        # either the old record or the complete new one. Server entries hold an auth token, so
        # the temporary and final files both remain private.
        fd, temp_path = tempfile.mkstemp(prefix=self._STATE_TEMP_PREFIX, dir=self.path)
        try:
            with os.fdopen(fd, "w") as f:
                os.fchmod(fd, 0o600)
                f.write(json.dumps(data))
            os.replace(temp_path, path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.close(fd)
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
    """Claims and retires members of one pool directory."""

    # A retired server still alive after the grace period is hard-killed. After the give-up
    # age, tracking is removed only once the process is gone, replaced, or successfully
    # signalled.
    _RETIRE_KILL_AFTER_SECONDS = 30
    _RETIRE_GIVE_UP_AFTER_SECONDS = 600
    _PROCESS_INSPECTION_TIMEOUT_SECONDS = 5
    _PROC_STAT_START_TIME_INDEX = 19

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
    def _same_server_instance(cls, pid: int, process_start_id: str) -> Optional[bool]:
        """Whether ``pid`` is still the recorded Connect server process generation."""
        current_start_id = cls._process_start_id(pid)
        if current_start_id is None:
            return None
        if current_start_id != process_start_id:
            return False
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

    def reap(self, uid: str) -> bool:
        """Advance a retiring member and report whether nothing of it remains."""
        states = self._directory.states(uid)
        if "retired" in states:
            self._reap_retired(uid, states["retired"])
        return not self._directory.states(uid)

    def _reap_retired(self, uid: str, path: str) -> None:
        """A retiring member: drop it once its server is gone, hard-kill the server if it
        hangs in shutdown, and stop tracking a record whose process generation was reused."""
        data = self._directory.read_json(path)
        retired_state = RetiredState.from_data(data)
        record_pid = RetiredState.pid_from_data(data)
        record_process_start_id = RetiredState.process_start_id_from_data(data)
        server_pid = (
            retired_state.pid
            if retired_state is not None
            else self._recover_server_pid(uid, record_pid)
        )
        process_start_id = (
            retired_state.process_start_id if retired_state is not None else record_process_start_id
        )
        retired = retired_state.retired if retired_state is not None else None
        now = time.time()
        if server_pid is None:
            # Without a recoverable process handle, retain the state for a later read instead of
            # declaring a potentially live server gone.
            return
        if not _pid_alive(server_pid):
            self._remove_retired(uid, path)
            return
        if process_start_id is None:
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
                path, RetiredState(server_pid, process_start_id, now).as_data()
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

    def _remove_retired(self, uid: str, path: str) -> None:
        self._directory.remove(path)
        self._directory.remove_member_dir(uid)

    def _retire(self, state_path: str, server_pid: int, process_start_id: str) -> None:
        """Move a member into the retired state: signal its server and track the shutdown so
        :meth:`_reap_retired` can escalate if the JVM hangs."""
        _, uid = self._directory.parse_entry(os.path.basename(state_path))
        assert uid is not None
        self._signal_server(server_pid, process_start_id, signal.SIGTERM)
        retired_path = self._directory.retired_path(uid)
        # Rename instead of removing the old state so a crash cannot leave a live server with
        # no state. If rewriting is interrupted, _reap_retired preserves the recoverable pid.
        self._directory.rename(state_path, retired_path)
        self._directory.write_json(
            retired_path, RetiredState(server_pid, process_start_id, time.time()).as_data()
        )

    def _recover_server_pid(self, uid: str, record_pid: Optional[int]) -> Optional[int]:
        """Prefer the daemon pid file, then a pid recovered from a malformed state record.

        Full member validation intentionally rejects corrupt records, including out-of-range
        timestamps. Reaping still needs the independently valid pid so retirement does not
        discard the only handle to a live JVM.
        """
        daemon_pid = self._recorded_daemon_pid(uid)
        if daemon_pid is not None:
            return daemon_pid
        return record_pid

    def _recorded_daemon_pid(self, uid: str) -> Optional[int]:
        """The positive server pid recorded by spark-daemon.sh, if readable."""
        from pyspark.sql.connect.local_server import Discovery

        discovery = Discovery(os.path.join(self._directory.member_dir(uid), "connect-local.json"))
        return _PoolStateRecord._positive_pid(discovery.daemon_pid())

    def release(self, member: PoolMember) -> None:
        """Retire this process's claimed member; the shutdown completes in the background,
        ready for a later lifecycle pass to finish."""
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
