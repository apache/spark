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

from contextlib import closing
import time
import socket
import socketserver
from struct import pack, unpack
import sys
import threading
import traceback
from typing import Optional, Generator
import warnings
from pyspark.context import SparkContext

# Use b'\x00' as separator instead of b'\n', because the bytes are encoded in utf-8
_SERVER_POLL_INTERVAL = 0.1
_TRUNCATE_MSG_LEN = 4000


def get_driver_host(sc: SparkContext) -> Optional[str]:
    return sc.getConf().get("spark.driver.host")


_log_print_lock = threading.Lock()  # pylint: disable=invalid-name


def _get_log_print_lock() -> threading.Lock:
    return _log_print_lock


class WriteLogToStdout(socketserver.StreamRequestHandler):
    def _read_bline(self) -> Generator[bytes, None, None]:
        while self.server.is_active:
            packed_number_bytes = self.rfile.read(4)
            if not packed_number_bytes:
                time.sleep(_SERVER_POLL_INTERVAL)
                continue
            number_bytes = unpack(">i", packed_number_bytes)[0]
            message = self.rfile.read(number_bytes)
            yield message

    def handle(self) -> None:
        self.request.setblocking(0)  # non-blocking mode
        for bline in self._read_bline():
            with _get_log_print_lock():
                sys.stderr.write(bline.decode("utf-8") + "\n")
                sys.stderr.flush()


# What is run on the local driver
class LogStreamingServer:
    def __init__(self) -> None:
        self.server = None
        self.serve_thread = None
        self.port = None

    @staticmethod
    def _get_free_port(spark_host_address: str = "") -> int:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as tcp:
            tcp.bind((spark_host_address, 0))
            _, port = tcp.getsockname()
        return port

    def start(self, spark_host_address: str = "") -> None:
        if self.server:
            raise RuntimeError("Cannot start the server twice.")

        def serve_task(port: int) -> None:
            with socketserver.ThreadingTCPServer(("0.0.0.0", port), WriteLogToStdout) as server:
                self.server = server
                server.is_active = True
                server.serve_forever(poll_interval=_SERVER_POLL_INTERVAL)

        self.port = LogStreamingServer._get_free_port(spark_host_address)
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
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
                    binary_message = message.encode("utf-8")
                    packed_number_bytes = pack(">i", len(binary_message))
                    self.sock.sendall(packed_number_bytes + binary_message)
                except Exception:  # pylint: disable=broad-except
                    self._fail("Error sending logs to driver, stopping log streaming")

    def close(self) -> None:
        """
        Closes the connection.
        """
        if self.sock:
            self.sock.close()
            self.sock = None
