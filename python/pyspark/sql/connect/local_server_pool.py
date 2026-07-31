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

"""
Opt-in pool of single-use local Spark Connect servers
(``spark.local.connect.pool`` / ``SPARK_LOCAL_CONNECT_POOL``).

The reuse mode (``spark.local.connect.reuse``, see ``pyspark.sql.connect.local_server``) makes
local runs fast by sharing one long-lived server, at the price of state backed by the shared
``SparkContext`` (persistent catalog, global temp views, cached data) carrying across runs.
The pool keeps the speed without the sharing: it maintains a small set of booted servers that
have never been assigned to an application run, and
``SparkSession.builder.remote("local[*]").getOrCreate()`` *claims* one exclusively, spawns a
replacement in the background, and tears the claimed server down when the session stops or the
client exits. No server ever serves two application runs, so runs are as isolated from each
other as with the default in-process server -- at the cost of the idle servers' memory while
you iterate. If both opt-ins are set, the pool takes precedence.

The pool lives in a ``pool`` subdirectory of the per-user runtime directory (override with
``SPARK_LOCAL_CONNECT_POOL_DIR``). A member is a set of files named by a random ``<uid>``;
every access happens under the directory's ``.lock`` file lock, so readers always observe
complete states:

    pending-<uid>.json         an in-flight launch (the attendant process booting the server)
    conf-<uid>.json            startup confs for that launch, read once by its attendant
    server-<uid>.json          a ready, unclaimed server (host/port/token/pid/version)
    claimed-<pid>-<uid>.json   a server owned by the live client process <pid>
    retired-<uid>.json         a server being torn down; hard-killed if it hangs
    member-<uid>/              the server's pid file and logs (spark-daemon.sh directories)

Each launch runs an *attendant* (``python -m pyspark.sql.connect.local_server_pool --attend``),
a small detached process that boots the server through ``sbin/start-connect-server.sh``,
publishes its ``server-<uid>.json``, and supervises it. It retires the server once it has sat
unclaimed past the idle timeout, or once the client that claimed it has died without releasing
it. A janitor pass on every acquire is the backstop for members whose attendant itself died.

Servers are only handed to runs they were built for: each member carries a fingerprint of its
master, seeded confs, working directory, and Python executable, and a run only claims members
whose fingerprint matches its own. ``python -m pyspark.sql.connect.local_server_pool --purge``
force-stops every member and empties the pool directory.

This mode is experimental. Everything but the opt-in itself -- the pool directory layout, the
attendant, and the ``--purge`` entry point -- is an internal detail that may change or move,
for example into a unified ``spark connect`` CLI. POSIX only, like the reuse mode.
"""

import argparse
import atexit
import contextlib
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyspark.errors import PySparkRuntimeError

# How long one acquire may wait for a member to become ready. A cold launch takes at most
# 120s; this leaves room for one relaunch of a failed one.
_ACQUIRE_TIMEOUT = 180
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

_DEFAULT_POOL_SIZE = 2
_DEFAULT_IDLE_TIMEOUT = 1800


def _pool_size(opts: Dict[str, Any]) -> int:
    """The number of ready or in-flight members to keep per fingerprint; at least one.
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


def _signal(pid: int, sig: int) -> bool:
    """Best-effort signal; ``False`` when the process is already gone or not ours."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, sig)
        return True
    except OSError:
        return False


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

    def acquire(self, master: str, opts: Dict[str, Any]) -> PoolMember:
        """Claim one ready, fingerprint-matching member and top the pool back up.

        When a member is ready this returns after one janitor-claim-refill pass; on a cold pool
        (first run, conf change, or all members consumed) the refill starts a full complement
        and the loop waits for the first member to become ready, which costs one ordinary
        cold start. The lock is released between polls so attendants can publish; launches
        that die are relaunched by later passes, and only the overall deadline fails.
        """
        from pyspark.sql.connect.local_server import startup_seed_conf

        seed_conf = startup_seed_conf(opts)
        fingerprint = pool_fingerprint(master, seed_conf)
        target = _pool_size(opts)
        deadline = time.monotonic() + _ACQUIRE_TIMEOUT
        while True:
            with self._directory:
                self.janitor()
                member = self.claim(fingerprint)
                self.refill(master, seed_conf, fingerprint, target)
            if member is not None:
                return member
            if time.monotonic() >= deadline:
                raise PySparkRuntimeError(
                    errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
                    messageParameters={
                        "reason": f"no pooled local server became ready within "
                        f"{_ACQUIRE_TIMEOUT}s; see the attendant and server logs under "
                        f"{self._directory.path}"
                    },
                )
            time.sleep(0.25)

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

    def refill(self, master: str, seed_conf: Dict[str, Any], fingerprint: str, target: int) -> None:
        """Launch members until ready or in-flight ones with this fingerprint reach
        ``target``. Running under the directory lock is what makes concurrent cold starters
        share one complement of launches instead of each spawning their own."""
        available = 0
        for kind in ("server", "pending"):
            for _, path in self._directory.paths_of_kind(kind):
                data = self._directory.read_json(path)
                if data is not None and data.get("fingerprint") == fingerprint:
                    available += 1
        for _ in range(target - available):
            MemberAttendant.spawn(self._directory, master, seed_conf, fingerprint)

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
        member = PoolMember(data) if data is not None else None
        idle = _idle_timeout()
        expired = member is not None and idle > 0 and time.time() - member.created > idle
        if member is None or expired or not member.is_usable():
            self._retire(path, member.pid if member is not None else -1)

    def _reap_claimed(self, path: str) -> None:
        """A claimed member whose client died without releasing it (e.g. SIGKILL), or whose
        server died under its client: retire it. Claims of this live process are its own."""
        data = self._directory.read_json(path)
        server_pid = int(data["pid"]) if data else -1
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


# The member this client process has claimed, if any. Module-level so the session's stop
# callback and atexit share one idempotent release path.
_claimed_member: Optional[PoolMember] = None
_release_registered = False


def acquire_pooled_local_connect_server(master: str, opts: Dict[str, Any]) -> str:
    """Claim a local Connect server not previously assigned to an application run.

    Returns the ``sc://host:port`` endpoint and sets ``SPARK_CONNECT_AUTHENTICATE_TOKEN`` so
    the client authenticates against that server. Only reached for a ``local`` master when the
    pool opt-in is set; see ``SparkSession.getOrCreate`` in ``pyspark.sql.session``.
    """
    global _claimed_member, _release_registered
    if os.name != "posix":
        raise PySparkRuntimeError(
            errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
            messageParameters={
                "reason": "spark.local.connect.pool relies on the POSIX scripts under sbin/; "
                "on this platform start a server manually (sbin/start-connect-server.sh) and "
                'connect with .remote("sc://...")'
            },
        )
    # getOrCreate() may be re-entered while this process already holds a live claimed member
    # (the connect layer then returns the existing session); claiming again would strand a
    # second server.
    if _claimed_member is not None and _pid_alive(_claimed_member.pid):
        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = _claimed_member.token
        return _claimed_member.url

    member = ServerPool().acquire(master, opts)
    _claimed_member = member
    if not _release_registered:
        # A client that exits without stopping its session still releases its member; the
        # member's attendant and the janitor cover clients that die uncleanly.
        atexit.register(release_pooled_local_connect_server)
        _release_registered = True
    os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = member.token
    return member.url


def release_pooled_local_connect_server() -> None:
    """Retire this process's claimed pooled server; safe to call when there is none. The
    server winds down in the background while this client moves on."""
    global _claimed_member
    member, _claimed_member = _claimed_member, None
    if member is not None:
        ServerPool().release(member)


def purge_local_connect_pool() -> int:
    """See :meth:`ServerPool.purge`. Also available as
    ``python -m pyspark.sql.connect.local_server_pool --purge``."""
    return ServerPool().purge()


class MemberAttendant:
    """Boots one pool member, publishes it, and supervises it until it is gone.

    Runs detached from the spawning client (``--attend``). Every phase is appended to
    ``member-<uid>/attendant.log`` so a member that misbehaved can be debugged after the
    fact. A failed boot reaps whatever half-started server spark-daemon.sh recorded and
    withdraws the launch's bookkeeping, so refills stop counting it.
    """

    @classmethod
    def spawn(
        cls,
        directory: PoolDirectory,
        master: str,
        seed_conf: Dict[str, Any],
        fingerprint: str,
    ) -> None:
        """Start one detached attendant and publish its in-flight launch.

        Callers hold ``directory``'s lock, so another refill sees the pending marker before it
        can start a duplicate launch for the same pool complement.
        """
        uid = uuid.uuid4().hex[:12]
        directory.write_json(directory.conf_path(uid), seed_conf)
        cmd = [
            sys.executable,
            "-m",
            "pyspark.sql.connect.local_server_pool",
            "--attend",
            "--pool-dir",
            directory.path,
            "--uid",
            uid,
            "--master",
            master,
            "--fingerprint",
            fingerprint,
        ]
        env = dict(os.environ)
        # The attendant must neither see this client's Connect mode nor inherit its auth
        # token: each member gets its own token from LocalConnectServer.
        for var in (
            "SPARK_REMOTE",
            "SPARK_LOCAL_REMOTE",
            "SPARK_CONNECT_MODE_ENABLED",
            "SPARK_CONNECT_AUTHENTICATE_TOKEN",
        ):
            env.pop(var, None)
        try:
            proc = subprocess.Popen(
                cmd,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError:
            directory.remove(directory.conf_path(uid))
            raise
        directory.write_json(
            directory.pending_path(uid),
            {"attendant_pid": proc.pid, "created": time.time(), "fingerprint": fingerprint},
        )

    def __init__(self, directory: PoolDirectory, uid: str, master: str, fingerprint: str):
        self._directory = directory
        self._pool = ServerPool(directory)
        self._uid = uid
        self._master = master
        self._fingerprint = fingerprint
        self._member_dir = directory.member_dir(uid)

    def run(self) -> int:
        os.makedirs(self._member_dir, mode=0o700, exist_ok=True)
        member = self._boot()
        if member is None:
            return 1
        self._log("supervising")
        self._supervise(member.pid)
        self._log("done")
        return 0

    def _log(self, message: str) -> None:
        with open(os.path.join(self._member_dir, "attendant.log"), "a") as f:
            f.write(f"{time.time():.3f} {message}\n")

    def _boot(self) -> Optional[PoolMember]:
        """Start the server on a fresh ephemeral port and publish it as ready-to-claim.
        Publishing consumes the launch's pending marker and conf seed: from that moment the
        member counts as a server, and this process's job shifts to supervising it."""
        from pyspark.sql.connect.local_server import Discovery, LocalConnectServer

        with self._directory:
            seed_conf = self._directory.read_json(self._directory.conf_path(self._uid)) or {}
        discovery = Discovery(os.path.join(self._member_dir, "connect-local.json"))
        self._log(f"booting master={self._master}")
        try:
            with discovery:
                server = LocalConnectServer(discovery)
                server.start(
                    self._master,
                    {},
                    use_ephemeral_port=True,
                    seed_conf=seed_conf,
                )
                data = discovery.load()
            assert data is not None  # launch() saved it
        except Exception as e:
            self._log(f"boot failed: {e!r}")
            with self._directory:
                self._pool.abort_launch(self._uid)
            return None
        data["fingerprint"] = self._fingerprint
        data["created"] = time.time()
        with self._directory:
            self._directory.write_json(self._directory.server_path(self._uid), data)
            self._directory.remove(self._directory.pending_path(self._uid))
            self._directory.remove(self._directory.conf_path(self._uid))
        self._log(f"published pid={data['pid']} port={data['port']}")
        return PoolMember(data)

    def _supervise(self, server_pid: int) -> None:
        """Watch this member until nothing of it remains, applying the pool's reaping rules.

        This is what lets an idle machine drain to zero servers with no further Spark run: the
        idle timeout, dead-client cleanup, and hard-kill escalation all fire from here even
        when no client ever runs again.
        """
        while True:
            with self._directory:
                if self._pool.reap(self._uid):
                    return
                states = set(self._directory.states(self._uid))
                if states <= {"member"} and not _pid_alive(server_pid):
                    # A purge or another process's janitor already tore the member down; only
                    # the young member directory remains.
                    self._directory.remove_member_dir(self._uid)
                    return
            time.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage the opt-in pool of single-use local Spark Connect servers "
        "(spark.local.connect.pool)."
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="force-stop every pool member and empty the pool directory",
    )
    # Internal entry point spawned by ServerPool.
    parser.add_argument("--attend", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--pool-dir", help=argparse.SUPPRESS)
    parser.add_argument("--uid", help=argparse.SUPPRESS)
    parser.add_argument("--master", help=argparse.SUPPRESS)
    parser.add_argument("--fingerprint", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.purge:
        print(f"Signalled {purge_local_connect_pool()} pool process(es).")
    elif args.attend:
        attendant = MemberAttendant(
            PoolDirectory(args.pool_dir), args.uid, args.master, args.fingerprint
        )
        sys.exit(attendant.run())
    else:
        parser.print_help(sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
