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
# type: ignore

import time
import socket
import socketserver
import sys
import threading
import traceback
from typing import Optional, Generator
import warnings
from pyspark.context import SparkContext

# Use b'\x00' as separator instead of b'\n', because the bytes are encoded in utf-8
_SEP_CHAR = b"\x00"
_SERVER_POLL_INTERVAL = 0.1
_TRUNCATE_MSG_LEN = 4000


def get_driver_host(sc: SparkContext) -> Optional[str]:
    return sc.getConf().get("spark.driver.host")


_log_print_lock = threading.Lock()  # pylint: disable=invalid-name


def _get_log_print_lock() -> threading.Lock:
    return _log_print_lock


class WriteLogToStdout(socketserver.StreamRequestHandler):
    def _read_bline(self) -> Generator[bytes, None, None]:
        remaining_data = b""
        while self.server.is_active:
            new_data = self.rfile.read1(4096)
            if not new_data:
                time.sleep(_SERVER_POLL_INTERVAL)
                continue
            blines = new_data.split(_SEP_CHAR)
            blines[0] = remaining_data + blines[0]
            for i in range(len(blines) - 1):
                yield blines[i]
            # The last line in blines is a half line.
            remaining_data = blines[-1]  # pylint: disable=attribute-defined-outside-init

    def handle(self) -> None:
        self.request.setblocking(0)  # non-blocking mode
        for bline in self._read_bline():
            with _get_log_print_lock():
                sys.stdout.write(bline.decode("utf-8") + "\n")


# What is run on the local driver
class LogStreamingServer:
    def __init__(self) -> None:
        self.server = None
        self.serve_thread = None
        self.port = None

    @staticmethod
    def _get_free_port() -> int:
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.bind(("", 0))
        _, port = tcp.getsockname()
        tcp.close()
        return port

    def start(self) -> None:
        if self.server:
            raise RuntimeError("Cannot start the server twice.")

        def serve_task(port: int) -> None:
            with socketserver.ThreadingTCPServer(("", port), WriteLogToStdout) as server:
                self.server = server
                server.is_active = True
                server.serve_forever(poll_interval=_SERVER_POLL_INTERVAL)

        self.port = LogStreamingServer._get_free_port()
        self.serve_thread = threading.Thread(target=serve_task, args=(self.port,))
        self.serve_thread.setDaemon(True)
        self.serve_thread.start()

    def shutdown(self) -> None:
        if self.server:
            # Sleep to ensure all log has been received and printed.
            time.sleep(_SERVER_POLL_INTERVAL * 2)
            # Before close we need flush to ensure all stdout buffer were printed.
            sys.stdout.flush()
            self.server.is_active = False
            self.server.shutdown()
            self.serve_thread.join()
            self.server = None
            self.serve_thread = None


class LogStreamingClientBase:
    @staticmethod
    def _maybe_truncate_msg(message: str) -> str:
        if len(message) > _TRUNCATE_MSG_LEN:
            message = message[:_TRUNCATE_MSG_LEN]
            return message + "...(truncated)"
        else:
            return message

    def send(self, message: str) -> None:
        pass

    def close(self) -> None:
        pass


class LogStreamingClient(LogStreamingClientBase):
    """
    A client that streams log messages to :class:`LogStreamingServer`.
    In case of failures, the client will skip messages instead of raising an error.
    """

    _log_callback_client = None
    _server_address = None
    _singleton_lock = threading.Lock()

    @staticmethod
    def _init(address: str, port: int) -> None:
        LogStreamingClient._server_address = (address, port)

    @staticmethod
    def _destroy() -> None:
        LogStreamingClient._server_address = None
        if LogStreamingClient._log_callback_client is not None:
            LogStreamingClient._log_callback_client.close()

    def __init__(self, address: str, port: int, timeout: int = 10):
        """
        Creates a connection to the logging server and authenticates.This client is best effort,
        if authentication or sending a message  fails, the client will be marked as not alive and
        stop trying to send message.

        :param address: Address where the service is running.
        :param port: Port where the service is listening for new connections.
        """
        self.address = address
        self.port = port
        self.timeout = timeout
        self.sock = None
        self.failed = True
        self._lock = threading.RLock()

    def _fail(self, error_msg: str) -> None:
        self.failed = True
        warnings.warn(f"{error_msg}: {traceback.format_exc()}\n")

    def _connect(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect((self.address, self.port))
            self.sock = sock
            self.failed = False
        except (OSError, IOError):  # pylint: disable=broad-except
            self._fail("Error connecting log streaming server")

    def send(self, message: str) -> None:
        """
        Sends a message.
        """
        with self._lock:
            if self.sock is None:
                self._connect()
            if not self.failed:
                try:
                    message = LogStreamingClientBase._maybe_truncate_msg(message)
                    # TODO:
                    #  1) addressing issue: idle TCP connection might get disconnected by
                    #     cloud provider
                    #  2) sendall may block when server is busy handling data.
                    self.sock.sendall(bytes(message, "utf-8") + _SEP_CHAR)
                except Exception:  # pylint: disable=broad-except
                    self._fail("Error sending logs to driver, stopping log streaming")

    def close(self) -> None:
        """
        Closes the connection.
        """
        if self.sock:
            self.sock.close()
            self.sock = None
