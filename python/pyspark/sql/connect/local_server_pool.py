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
import socket
import sys
from typing import Any, Dict, List, Optional, Tuple


def pool_fingerprint(master: str, seed_conf: Dict[str, Any]) -> str:
    """The identity of a pool member: everything that shapes the server a run would have
    booted for itself. A run only claims members whose fingerprint equals its own, so a
    pre-booted JVM is never handed to a run it would not have produced.

    Besides the master and the seeded confs, this covers the working directory (unset
    warehouse and Derby metastore locations resolve relative to it), the client Python
    executable, and the Python executable the Connect server selects for Python UDFs.
    """
    server_python = os.environ.get(
        "PYSPARK_PYTHON", os.environ.get("PYSPARK_DRIVER_PYTHON", "python3")
    )
    identity = [
        master,
        sorted((str(k), str(v)) for k, v in seed_conf.items()),
        os.getcwd(),
        sys.executable,
        server_python,
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
    except OverflowError:
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
        normalized = dict(data)
        for key in ("host", "token", "spark_version", "fingerprint"):
            if not isinstance(normalized[key], str):
                raise ValueError(f"{key} must be a string")
        normalized["port"] = int(normalized["port"])
        normalized["pid"] = int(normalized["pid"])
        normalized["created"] = float(normalized["created"])
        if not 0 <= normalized["port"] <= 65535:
            raise ValueError("port is out of range")
        if not math.isfinite(normalized["created"]):
            raise ValueError("created must be finite")
        self.data = normalized
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
    def pid(self) -> int:
        return int(self.data["pid"])

    @property
    def token(self) -> str:
        return self.data["token"]

    @property
    def created(self) -> float:
        return float(self.data["created"])

    @property
    def fingerprint(self) -> str:
        return self.data["fingerprint"]

    @property
    def url(self) -> str:
        return f"sc://{self.data['host']}:{self.data['port']}"

    def is_usable(self) -> bool:
        """Whether this member has a matching Spark version, live process, and open port."""
        from pyspark.version import __version__

        if self.data["spark_version"] != __version__ or not _pid_alive(self.pid):
            return False
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.5)
                return sock.connect_ex((self.data["host"], self.data["port"])) == 0
        except (OSError, UnicodeError):
            return False


class PoolDirectory:
    """Path layout, file access, and the cross-process lock of one pool directory.

    Used as a context manager that holds the directory's exclusive lock::

        with PoolDirectory() as directory:
            ...read and write state files...

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
    """Claims members from one pool directory; lifecycle operations are added later."""

    def __init__(self, directory: Optional[PoolDirectory] = None):
        self._directory = directory or PoolDirectory()

    def claim(self, fingerprint: str) -> Optional[PoolMember]:
        """Claim the oldest usable member with this fingerprint, or ``None``. The rename to
        ``claimed-<pid>-<uid>.json`` marks the member as owned by this process; the reaping
        rules use that pid to retire members whose client died without releasing them."""
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
