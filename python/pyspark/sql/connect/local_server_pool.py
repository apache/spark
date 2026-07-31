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
import socket
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

# A pending marker older than this belongs to a launch that hung; the janitor kills it.
# Deliberately above the local-server startup timeout so a slow-but-healthy launch is never shot.
_LAUNCH_TIMEOUT = 180
# A retired server still alive this long after retirement is hard-killed; one that survives
# even that (e.g. not ours to signal) is dropped from tracking after the give-up age.
_RETIRE_KILL_AFTER = 30
_RETIRE_GIVE_UP = 600
# Unreferenced member directories older than this are removed. The age gate keeps the logs of
# a just-failed launch around long enough to be looked at.
_DIR_GC_AGE = 24 * 3600

_DEFAULT_IDLE_TIMEOUT = 1800


def _idle_timeout() -> int:
    """Seconds an unclaimed member may sit before it is retired; 0 or negative disables idle
    retirement. Read from the environment wherever reaping runs -- clients and attendants
    alike -- so there is exactly one source of truth for it.
    """
    try:
        return int(os.environ["SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT"])
    except (KeyError, ValueError):
        return _DEFAULT_IDLE_TIMEOUT


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


def _signal(pid: int, sig: int) -> bool:
    """Best-effort signal; ``False`` when the process is already gone or not ours."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, sig)
        return True
    except (OSError, OverflowError):
        return False


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
    """Claims, reaps, and retires members of one pool directory."""

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
        if "pending" in states:
            self._reap_pending(uid, states["pending"])
        if "server" in states:
            self._reap_server(states["server"])
        if "claimed" in states:
            self._reap_claimed(states["claimed"])
        if "retired" in states:
            self._reap_retired(uid, states["retired"])

        remaining = self._directory.states(uid)
        if set(remaining) == {"member"}:
            # Nothing references the member directory anymore. The age gate keeps the logs
            # of a freshly failed launch around long enough to be looked at.
            try:
                expired = time.time() - os.path.getmtime(remaining["member"]) > _DIR_GC_AGE
            except OSError:
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
        attendant_pid = int(data["attendant_pid"]) if data else -1
        age = time.time() - float(data["created"]) if data else _LAUNCH_TIMEOUT + 1
        if not _pid_alive(attendant_pid) or age > _LAUNCH_TIMEOUT:
            _signal(attendant_pid, signal.SIGTERM)
            self.abort_launch(uid)

    def abort_launch(self, uid: str) -> None:
        """Withdraw a failed launch and stop any server it started before failing."""
        self._kill_recorded_daemon(uid)
        self._directory.remove(self._directory.pending_path(uid))
        self._directory.remove(self._directory.conf_path(uid))

    def _reap_server(self, path: str) -> None:
        """A ready member that is unusable (dead, unreachable, version-mismatched after an
        upgrade, or an unreadable record) or has sat unclaimed past the idle timeout: retire
        it."""
        data = self._directory.read_json(path)
        member = PoolMember.from_data(data) if data is not None else None
        idle = _idle_timeout()
        expired = member is not None and idle > 0 and time.time() - member.created > idle
        if member is None or expired or not member.is_usable():
            self._retire(path, member.pid if member is not None else -1)

    def _reap_claimed(self, path: str) -> None:
        """A claimed member whose client died without releasing it (e.g. SIGKILL), or whose
        server died under its client: retire it. Claims of this live process are its own."""
        data = self._directory.read_json(path)
        member = PoolMember.from_data(data) if data is not None else None
        server_pid = member.pid if member is not None else -1
        client_pid = self._directory.claiming_pid(path)
        if client_pid == os.getpid():
            return
        if not _pid_alive(client_pid) or not _pid_alive(server_pid):
            self._retire(path, server_pid)

    def _reap_retired(self, uid: str, path: str) -> None:
        """A retiring member: drop it once its server is gone, hard-kill the server if it
        hangs in shutdown, and eventually stop tracking one that survives even that (it is
        not ours to signal; nothing more can be done)."""
        data = self._directory.read_json(path)
        server_pid = int(data["pid"]) if data else -1
        age = time.time() - float(data["retired"]) if data else _RETIRE_GIVE_UP + 1
        if not _pid_alive(server_pid) or age > _RETIRE_GIVE_UP:
            self._directory.remove(path)
            self._directory.remove_member_dir(uid)
        elif age > _RETIRE_KILL_AFTER:
            _signal(server_pid, signal.SIGKILL)

    def _retire(self, state_path: str, server_pid: int) -> None:
        """Move a member into the retired state: signal its server and track the shutdown so
        :meth:`_reap_retired` can escalate if the JVM hangs."""
        _, uid = self._directory.parse_entry(os.path.basename(state_path))
        assert uid is not None
        _signal(server_pid, signal.SIGTERM)
        self._directory.remove(state_path)
        self._directory.write_json(
            self._directory.retired_path(uid), {"pid": server_pid, "retired": time.time()}
        )

    def _kill_recorded_daemon(self, uid: str) -> None:
        """Kill the server pid that spark-daemon.sh recorded in the member directory, if
        any. This is how a half-started JVM is reaped when its launch fails or its attendant
        dies before publishing."""
        from pyspark.sql.connect.local_server import Discovery

        discovery = Discovery(os.path.join(self._directory.member_dir(uid), "connect-local.json"))
        daemon_pid = discovery.daemon_pid()
        if daemon_pid is not None:
            _signal(daemon_pid, signal.SIGTERM)

    def release(self, member: PoolMember) -> None:
        """Retire this process's claimed member; the shutdown completes in the background,
        watched by the member's attendant with the janitor as backstop."""
        assert member.claim_path is not None
        with self._directory:
            self._retire(member.claim_path, member.pid)

    def purge(self) -> int:
        """Force-stop every member -- ready, in-flight, or claimed -- and empty the pool
        directory; the escape hatch back to a clean slate. Returns the number of processes
        signalled. SIGKILL rather than SIGTERM because nothing tracks a member once its
        state files are gone, so a shutdown that hangs would leak. Supervising attendants
        hold no state file; they notice the emptied directory and exit on their own."""
        signalled = 0
        with self._directory:
            for uid in self._directory.uids():
                for kind, path in self._directory.states(uid).items():
                    if kind == "member":
                        continue
                    data = self._directory.read_json(path)
                    for key in ("pid", "attendant_pid"):
                        if data is not None and key in data:
                            if _signal(int(data[key]), signal.SIGKILL):
                                signalled += 1
                    self._directory.remove(path)
                self._kill_recorded_daemon(uid)
                self._directory.remove_member_dir(uid)
        return signalled


# The member this client process has claimed, if any. A later acquisition layer populates it;
# keeping the idempotent release path here makes lifecycle ownership explicit.
_claimed_member: Optional[PoolMember] = None


def release_pooled_local_connect_server() -> None:
    """Retire this process's claimed pooled server; safe to call when there is none. The
    server winds down in the background while this client moves on."""
    global _claimed_member
    member, _claimed_member = _claimed_member, None
    if member is not None:
        ServerPool().release(member)


def purge_local_connect_pool() -> int:
    """See :meth:`ServerPool.purge`."""
    return ServerPool().purge()
