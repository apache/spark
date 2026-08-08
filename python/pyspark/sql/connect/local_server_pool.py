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
import os
import shutil
import socket
import sys
from typing import Any, Dict, List, Optional, Tuple

_DEFAULT_POOL_SIZE = 2


def _pool_size(opts: Dict[str, Any]) -> int:
    """The number of warm or in-flight members to keep per fingerprint; at least one.
    Malformed values fall back to the default rather than failing session creation over a
    tuning knob.
    """
    value = opts.get(
        "spark.local.connect.pool.size", os.environ.get("SPARK_LOCAL_CONNECT_POOL_SIZE")
    )
    try:
        return max(1, int(value)) if value is not None else _DEFAULT_POOL_SIZE
    except (TypeError, ValueError):
        return _DEFAULT_POOL_SIZE


def pool_fingerprint(master: str, seed_conf: Dict[str, Any]) -> str:
    """The identity of a pool member: everything that shapes the server a run would have
    booted for itself. A run only claims members whose fingerprint equals its own, so a
    pre-booted JVM is never handed to a run it would not have produced.

    Besides the master and the seeded confs, this covers the working directory (unset
    warehouse and Derby metastore locations resolve relative to it) and the Python executable
    (the server runs Python UDFs with the interpreter environment it inherited from its
    spawner).
    """
    identity = [
        master,
        sorted((str(k), str(v)) for k, v in seed_conf.items()),
        os.getcwd(),
        sys.executable,
        os.environ.get("PYSPARK_PYTHON", ""),
    ]
    return hashlib.sha256(json.dumps(identity).encode("utf-8")).hexdigest()[:16]


def _pid_alive(pid: int) -> bool:
    """Whether ``pid`` is running. A process we cannot signal counts as alive. Linux zombies
    count as terminated: they remain signalable until their parent reaps them, but cannot own
    or serve a pool member. POSIX only, like everything in this module.
    """
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        pass
    if sys.platform.startswith("linux"):
        try:
            with open(f"/proc/{pid}/stat", encoding="utf-8") as stat_file:
                fields = stat_file.read().rpartition(")")[2].split()
        except FileNotFoundError:
            return False
        except OSError:
            pass
        else:
            if fields and fields[0] == "Z":
                return False
    return True


class PoolMember:
    """One published pool server, wrapping its ``server-<uid>.json`` record."""

    def __init__(self, data: Dict[str, Any]):
        self.data = data
        # Set when this process claims the member; the path of its claimed-<pid>-<uid>.json.
        self.claim_path: Optional[str] = None

    @property
    def pid(self) -> int:
        return int(self.data["pid"])

    @property
    def token(self) -> str:
        return self.data["token"]

    @property
    def created(self) -> float:
        return float(self.data.get("created", 0))

    @property
    def fingerprint(self) -> Optional[str]:
        return self.data.get("fingerprint")

    @property
    def url(self) -> str:
        return f"sc://{self.data['host']}:{self.data['port']}"

    def is_usable(self) -> bool:
        """Whether this member can serve a run: complete record, matching Spark version, live
        process, and accepting connections."""
        from pyspark.version import __version__

        if not all(k in self.data for k in ("host", "port", "token", "pid", "spark_version")):
            return False
        if self.data["spark_version"] != __version__ or not _pid_alive(self.pid):
            return False
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            return sock.connect_ex((self.data["host"], int(self.data["port"]))) == 0


class PoolDirectory:
    """Path layout, file access, and the cross-process lock of one pool directory.

    Used as a context manager that holds the directory's exclusive lock::

        with PoolDirectory() as directory:
            ...read and write state files...

    Acquire runs once per client process, so the simplicity of one exclusive lock for every
    access beats any finer-grained scheme; the rename in ``ServerPool.claim`` is for tidiness,
    not lock-free atomicity. The context is reentrant per instance in the sense that it can be
    entered again after exiting, which the acquire loop does once per poll so that attendants
    get their turn at publishing.
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

        os.makedirs(self.path, mode=0o700, exist_ok=True)
        self._lock_fd = os.open(os.path.join(self.path, ".lock"), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
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

    @staticmethod
    def parse_entry(name: str) -> Tuple[Optional[str], Optional[str]]:
        """The ``(kind, uid)`` of a pool directory entry, ``(None, None)`` for anything
        else (the lock file, editor droppings, ...)."""
        if name.startswith("member-"):
            return "member", name[len("member-") :]
        if not name.endswith(".json"):
            return None, None
        stem = name[: -len(".json")]
        for kind in ("pending", "conf", "server", "retired"):
            if stem.startswith(kind + "-"):
                return kind, stem[len(kind) + 1 :]
        if stem.startswith("claimed-"):
            client_pid, sep, uid = stem[len("claimed-") :].partition("-")
            if sep and client_pid.isdigit():
                return "claimed", uid
        return None, None

    @staticmethod
    def claiming_pid(claimed_path: str) -> int:
        """The client pid recorded in a ``claimed-<pid>-<uid>.json`` file name."""
        return int(os.path.basename(claimed_path)[len("claimed-") :].split("-", 1)[0])

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
    """Claiming, refilling, and reaping the members of one pool directory."""

    def __init__(self, directory: Optional[PoolDirectory] = None):
        self._directory = directory or PoolDirectory()

    def claim(self, fingerprint: str) -> Optional[PoolMember]:
        """Claim the oldest usable member with this fingerprint, or ``None``. The rename to
        ``claimed-<pid>-<uid>.json`` marks the member as owned by this process; the reaping
        rules use that pid to retire members whose client died without releasing them."""
        candidates = []
        for uid, path in self._directory.paths_of_kind("server"):
            data = self._directory.read_json(path)
            if data is not None and data.get("fingerprint") == fingerprint:
                candidates.append((PoolMember(data), uid, path))
        candidates.sort(key=lambda c: c[0].created)
        for member, uid, path in candidates:
            if not member.is_usable():
                continue  # left for the reaping rules to retire
            claim_path = self._directory.claimed_path(os.getpid(), uid)
            self._directory.rename(path, claim_path)
            member.claim_path = claim_path
            return member
        return None
