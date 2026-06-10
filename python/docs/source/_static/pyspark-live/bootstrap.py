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

"""
In-browser bootstrap for the "Try PySpark in your browser" documentation
feature. This runs *inside* Pyodide (CPython on WebAssembly), after pyspark-live.js
has installed PySpark.

It lives entirely under python/docs and is never imported by the PySpark package.
It exposes two synchronous helpers that pyspark-live.js drives:

    pyspark_live_plan_b64(code)   -> JSON: build a DataFrame from the user's code
                                     and return its serialized Spark Connect plan
                                     (base64). No RPC, no execution.
    pyspark_live_render_b64(b64)  -> JSON: decode Arrow IPC stream bytes (the result
                                     of running the plan) and render them as a table.

Execution happens in JavaScript, which calls the sail-wasm ``execute_plan(planBytes)``
between these two steps. Splitting the work this way keeps every Python step
synchronous (Pyodide cannot block on a JS Promise on the main thread) while the
asynchronous wasm call stays in JS.

PySpark normally imports ``grpc`` at module load, but grpcio is a C-extension and
is unavailable in Pyodide. We register permissive stand-ins for ``grpc`` and
``grpc_status`` in ``sys.modules`` before importing the Connect client. They are
never actually used: building a DataFrame and serializing its plan is entirely
client-side, and execution goes through sail-wasm, not gRPC.
"""

import sys
import types
from importlib.machinery import ModuleSpec


# ---------------------------------------------------------------------------
# 1. Satisfy "import grpc" / "from grpc_status import rpc_status" without grpcio.
#
# The stock Connect client hard-imports grpcio at module load
# (pyspark/sql/connect/client/core.py, retries.py, reattach.py, artifact.py and
# the generated base_pb2_grpc.py), and references grpc symbols such as
# ``grpc.Channel`` inside type annotations evaluated at import time (those
# modules do not use ``from __future__ import annotations``).
#
# Rather than enumerate every symbol, register permissive stand-in modules. Any
# attribute access resolves to an inert placeholder; ``grpc.RpcError`` is a real
# exception class so ``except grpc.RpcError`` stays valid. The placeholders are
# never invoked -- no RPC is ever made. Documentation-only; nothing ships in the
# PySpark package.
# ---------------------------------------------------------------------------
class _Placeholder:
    """Inert stand-in for any unused gRPC symbol (callable and attributable)."""

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return _Placeholder()

    def __getattr__(self, name):
        return _Placeholder()


class _FakeRpcError(Exception):
    pass


def _install_fake_module(name, attrs):
    if name in sys.modules:
        return sys.modules[name]
    module = types.ModuleType(name)
    # A non-None __spec__ keeps importlib.util.find_spec(name) working, which
    # PySpark uses to probe for optional packages.
    module.__spec__ = ModuleSpec(name, loader=None)
    for key, value in attrs.items():
        setattr(module, key, value)
    # PEP 562: resolve any other attribute access to a placeholder so import-time
    # annotations like ``grpc.Channel`` evaluate without error.
    module.__getattr__ = lambda attr: _Placeholder()  # type: ignore[attr-defined]
    sys.modules[name] = module
    return module


def _install_fake_grpc():
    # PySpark's connect dependency check requires grpcio >= 1.48.1, grpcio-status
    # and zstandard. None can run in Pyodide, so present satisfying stand-ins; the
    # actual RPC/compression paths are never exercised when only building plans.
    _install_fake_module("grpc", {"RpcError": _FakeRpcError, "__version__": "1.99.0"})
    rpc_status = _install_fake_module(
        "grpc_status.rpc_status", {"from_call": lambda *a, **k: None}
    )
    _install_fake_module("grpc_status", {"rpc_status": rpc_status, "__version__": "1.99.0"})
    _install_fake_module("zstandard", {"__version__": "0.99.0"})


def _install_google_rpc_shim():
    # The Connect client imports ``google.rpc.error_details_pb2`` (from the
    # googleapis-common-protos package) at module load, only for error-detail
    # decoding. The protobuf runtime provides ``google`` and ``google.protobuf``
    # but not ``google.rpc``. If the real package is present (a normal install),
    # leave it alone; otherwise register an inert stand-in so the import succeeds.
    try:
        import google.rpc.error_details_pb2  # noqa: F401

        return
    except Exception:
        pass
    import google  # provided by the protobuf runtime

    errmod = _install_fake_module("google.rpc.error_details_pb2", {})
    rpcpkg = _install_fake_module("google.rpc", {"error_details_pb2": errmod})
    google.rpc = rpcpkg  # type: ignore[attr-defined]


_install_fake_grpc()
_install_google_rpc_shim()

# PySpark's check_dependencies() treats "no __main__.__file__ and no spec and no
# sys.ps1" as "running doctests" and imports the heavy pyspark.testing module
# (and may sys.exit). Give __main__ a file so that branch is skipped.
import __main__ as _main_module

if not hasattr(_main_module, "__file__"):
    _main_module.__file__ = "<pyspark-live>"

# The generated *_pb2 modules may be built with a newer protobuf "gencode" than
# the protobuf runtime bundled with Pyodide. That major-version guard is advisory
# for the simple Spark Connect message types built here, so relax it.
try:
    from google.protobuf import runtime_version as _pb_runtime_version

    _pb_runtime_version.ValidateProtobufRuntimeVersion = lambda *a, **k: None
except Exception:
    pass


# ---------------------------------------------------------------------------
# 2. An inert channel + a session that never talks to a server.
#
# Building a DataFrame and serializing its plan does not require any RPC, so the
# channel only needs to satisfy the generated stub's constructor (which calls
# channel.unary_unary(...) / unary_stream(...) at client init). The returned
# multicallables raise if ever invoked -- they should not be, because execution
# is handled by sail-wasm in JS, not through this channel.
# ---------------------------------------------------------------------------
from pyspark.sql.connect.client.core import ChannelBuilder


class _InertMultiCallable:
    def __call__(self, *args, **kwargs):
        raise RuntimeError(
            "Spark Connect RPCs are not available in the in-browser docs shell; "
            "execution is performed locally by sail-wasm."
        )


class _InertChannel:
    def unary_unary(self, *args, **kwargs):
        return _InertMultiCallable()

    def unary_stream(self, *args, **kwargs):
        return _InertMultiCallable()

    def stream_unary(self, *args, **kwargs):
        return _InertMultiCallable()

    def close(self):
        pass


class WasmChannelBuilder(ChannelBuilder):
    """A ``ChannelBuilder`` for the in-browser shell. Builds plans only."""

    def __init__(self):
        super().__init__()

    def toChannel(self):
        return _InertChannel()

    @property
    def host(self):
        return "in-browser"


from pyspark.sql.connect.session import SparkSession

spark = SparkSession.builder.channelBuilder(WasmChannelBuilder()).getOrCreate()


# ---------------------------------------------------------------------------
# 3. Helpers driven by pyspark-live.js.
# ---------------------------------------------------------------------------
import ast
import base64
import io
import json
import traceback
from contextlib import redirect_stdout

from pyspark.sql.connect.dataframe import DataFrame as _ConnectDataFrame

# Namespace shared across cells on the page. ``spark`` is always available; cells
# import whatever else they need (the same code they would write locally).
_CELL_GLOBALS = {"__name__": "__pyspark_live__", "spark": spark}


def _run_cell(code):
    """Execute a cell. If its last statement is an expression, return its value."""
    tree = ast.parse(code, filename="<cell>", mode="exec")
    last_expr = None
    if tree.body and isinstance(tree.body[-1], ast.Expr):
        last_expr = tree.body.pop().value
    exec(compile(tree, "<cell>", "exec"), _CELL_GLOBALS)
    if last_expr is not None:
        return eval(compile(ast.Expression(last_expr), "<cell>", "eval"), _CELL_GLOBALS)
    return None


def _format_error(exc):
    return "".join(traceback.format_exception_only(type(exc), exc)).strip()


def pyspark_live_plan_b64(code):
    """Run the user's code and, if it yields a DataFrame, return its plan (base64).

    Returns a JSON string: {"ok", "stdout", "plan"} or {"ok": false, "error"}.
    """
    out = io.StringIO()
    try:
        with redirect_stdout(out):
            result = _run_cell(code)
        plan_b64 = None
        if isinstance(result, _ConnectDataFrame):
            plan_bytes = result._plan.to_proto(spark._client).SerializeToString()
            plan_b64 = base64.b64encode(plan_bytes).decode("ascii")
        return json.dumps({"ok": True, "stdout": out.getvalue(), "plan": plan_b64})
    except Exception as exc:  # noqa: BLE001 - surfaced to the reader as cell output
        return json.dumps(
            {"ok": False, "error": _format_error(exc), "stdout": out.getvalue()}
        )


def pyspark_live_render_b64(arrow_b64):
    """Decode Arrow IPC stream bytes and render them as a text table.

    Returns a JSON string: {"ok", "text"} or {"ok": false, "error"}.
    """
    try:
        import pyarrow as pa

        table = pa.ipc.open_stream(base64.b64decode(arrow_b64)).read_all()
        if table.num_columns == 0:
            text = "(no columns)"
        else:
            pdf = table.to_pandas()
            text = pdf.to_string(index=False)
            text += "\n\n[%d row%s]" % (
                table.num_rows,
                "" if table.num_rows == 1 else "s",
            )
        return json.dumps({"ok": True, "text": text})
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"ok": False, "error": _format_error(exc)})


print("PySpark is ready. A SparkSession is available as `spark`.")
