#!/usr/bin/env python2
"""Utility program to hold resources for a given session.

A session is defined as the lifetime of some process (externally specified,
or the original forking process of this process)

This program will daemonize and return 0 if the lock succeeds. If the lock
fails, it will return a non-zero exit code.

After the parent program (or other specified pid) exits, it will also exit,
unlocking the file
"""
import argparse
import errno
import fcntl
import os
import sys
import time


_LOCK_DIR = "/tmp/session_locked_resources"


def _parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--timeout-secs", type=int, help="How long to wait for lock acquisition, in seconds"
    )
    parser.add_argument(
        "-p", "--pid", type=int, help="PID to wait for exit (defaults to parent pid)"
    )
    parser.add_argument("resource", help="Resource to lock")
    return parser.parse_args()


def _acquire_lock(filename, timeout_secs, message):
    """Acquire a lock file.

    Returns True iff the file could be locked within the timeout
    """
    f = open(filename, "a+")
    time_attempted = 0
    while True:
        try:
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Lock acquired
            f.truncate(0)
            f.write(message)
            f.flush()
            return f
        except IOError:
            # Locking failed
            time_attempted += 1
            if timeout_secs and time_attempted >= timeout_secs:
                # Timeout exceeded
                raise IOError("Can't get child lock")
        time.sleep(1)


def _daemonize(child_body):
    """Daemonize. Returns whether the child was successful.

    Child body is a function to call in the child. It should take one
    argument, which is a function that is called with a boolean
    that indicates that the child succeeded/failed in its initialization
    """
    CHILD_FAIL = "\2"
    CHILD_SUCCESS = "\0"
    r_fd, w_fd = os.pipe()
    if os.fork() != 0:
        # We are the original script. Read success/fail from the final
        # child and log an error message if needed.
        child_code = os.read(r_fd, 1)  # .decode('utf-8')
        return child_code == CHILD_SUCCESS
    # First child
    os.setsid()
    if os.fork() != 0:
        # Still in first child
        _close_std_streams()
        os._exit(0)
    # Second child (daemon process)

    def _write_to_parent(success):
        parent_message = CHILD_SUCCESS if success else CHILD_FAIL
        os.write(w_fd, parent_message)

    child_body(_write_to_parent)
    os._exit(0)


def _close_std_streams():
    """Close all our stdin/stdout/stderr streams."""
    sys.stdin.close()
    sys.stdout.close()
    sys.stderr.close()
    os.close(0)
    os.close(1)
    os.close(2)


def _wait_for_pid_exit(pid):
    while _is_pid_running(pid):
        time.sleep(1)


def _is_pid_running(pid):
    """Wait for a pid to finish.

    From Stack Overflow: https://stackoverflow.com/questions/7653178
    """
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
    return True


def _lock_and_wait(lock_success_callback, resource, timeout_secs, controlling_pid):
    """Attempt to lock the file then wait.

    lock_success_callback will be called if the locking worked.
    """
    lock_filename = os.path.join(_LOCK_DIR, resource)
    lock_message = (
        "Session lock on " + resource + ", controlling pid " + str(controlling_pid) + "\n"
    )
    try:
        f = _acquire_lock(lock_filename, timeout_secs, lock_message)
    except IOError:
        lock_success_callback(False)
        return
    lock_success_callback(True)
    _wait_for_pid_exit(controlling_pid)


def main():
    """Main program"""
    args = _parse_args()
    if not os.path.exists(_LOCK_DIR):
        os.mkdir(_LOCK_DIR)
    controlling_pid = args.pid or os.getppid()
    child_body_func = lambda success_callback: _lock_and_wait(
        success_callback, args.resource, args.timeout_secs, controlling_pid
    )
    if _daemonize(child_body_func):
        return 0
    else:
        print("Could not acquire lock")
        return 1


if __name__ == "__main__":
    sys.exit(main())
