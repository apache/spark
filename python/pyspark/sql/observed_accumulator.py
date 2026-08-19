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
Observe-backed accumulator for the DataFrame / UDF execution path.

Matches the SparkContext accumulator surface -- ``spark.accumulator(name, zero)``, imperative
``acc.add(x)`` inside a UDF, and ``acc.value`` on the driver -- with no wrapper API. A plain UDF
that references the accumulator is detected (its closure is inspected) and rewritten so the
per-row delta is carried out as a hidden column that a ``CollectMetrics`` (df.observe) node
aggregates. Because the value rides through the query plan rather than a scheduler side channel,
it is exactly-once: task retries, speculation, and stage recomputation do not double count.

Scope: this only covers accumulation inside a UDF whose output flows through a plan node. It
cannot back ``add`` inside arbitrary RDD closures (no plan node for observe to attach to).
"""

import inspect
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    LongType,
    BinaryType,
    BooleanType,
    DataType,
    _parse_datatype_string,
)

# Sentinel for "no partial accumulated yet" in a custom accumulator's task-local buffer.
_UNSET = object()


def _is_integral(term: Any) -> bool:
    """Whether ``term`` represents a whole number (a Python/NumPy int, or a float like ``2.0``),
    used to guard additions to an integer accumulator. Non-numeric or fractional values are not
    integral."""
    try:
        return term == int(term)
    except (TypeError, ValueError, OverflowError):
        return False


class ObservedAccumulator:
    """A driver-side handle whose value is produced by an observe node.

    Created via ``spark.accumulator(name, zero)``. Reference it inside a UDF, call :meth:`add`
    there, run an action, then read :attr:`value` on the driver. No ``udf``/``observe`` wrapper is
    needed: the analyzer detects the accumulator captured in the UDF's closure.
    """

    # Per-invocation accumulation buffer, on the executor's Python worker.
    _tls = threading.local()

    # Must match the prefixes in ObservedAccumulator on the JVM side.
    _MARKER_PREFIX = "__oa_udf::"
    NodePrefix = "__oa_node_"
    MetricPrefix = "__oa_metric_"

    def __init__(
        self,
        name: Optional[str] = None,
        zero: Any = 0,
        session: Optional[Any] = None,
        merge: Optional[Callable[[Any, Any], Any]] = None,
    ) -> None:
        self._zero: Any = zero
        self._name: str = name or ("acc_" + uuid.uuid4().hex[:8])
        # Field on the thread-local for the per-row numeric delta.
        self._key = "__oa_delta_" + self._name
        # Field on the thread-local for a custom accumulator's per-task object partial.
        self._objkey = "__oa_obj_" + self._name
        # None -> numeric (SQL sum); a function merge(acc, term) -> custom, associative combine.
        self._merge: Optional[Callable[[Any, Any], Any]] = merge
        # Driver-only: the creating session, for the value bridge.
        self._session: Any = session
        # Driver-only: Observations from custom-accumulator operator applications, and the running
        # folded value harvested from them.
        self._pending: List[Any] = []
        self._folded: Any = None
        self._register_harvest_listener()

    # The UDF closure captures this handle (it calls acc.add), so it must be picklable. The executor
    # needs the delta key (numeric) or key+objkey+merge+zero (custom); the session is driver-only.
    def __getstate__(self) -> Dict[str, Any]:
        return {
            "_zero": self._zero,
            "_name": self._name,
            "_key": self._key,
            "_objkey": self._objkey,
            "_merge": self._merge,
        }

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._session = None
        self._pending = []
        self._folded = None

    # -- executor side ------------------------------------------------------

    def add(self, term: Any = 1) -> None:
        """Add ``term`` for the current row (numeric accumulator) or fold it into this task's
        object partial (custom accumulator with a ``merge`` function). Call inside a UDF."""
        if self._merge is not None:
            import copy

            tls = ObservedAccumulator._tls
            cur = getattr(tls, self._objkey, _UNSET)
            if cur is _UNSET:
                cur = copy.deepcopy(self._zero)
            setattr(tls, self._objkey, self._merge(cur, term))
        else:
            # An integer accumulator (created with an integer ``zero``) is harvested through the
            # exact-Long path, and its delta column is typed integer end-to-end. A fractional term
            # would otherwise be handled inconsistently across UDF flavors -- silently truncated in
            # a row-at-a-time UDF, but rejected by the ``int64`` Series in a vectorized one -- so
            # reject it up front with a clear error. Whole-valued terms (Python/NumPy ints, or
            # floats like ``2.0``) are accepted.
            if (
                isinstance(self._zero, int)
                and not isinstance(self._zero, bool)
                and not _is_integral(term)
            ):
                from pyspark.errors import PySparkValueError

                raise PySparkValueError(
                    errorClass="OBSERVED_ACCUMULATOR_NON_INTEGER_ADD",
                    messageParameters={"term": str(term), "name": self._name},
                )
            cur = getattr(ObservedAccumulator._tls, self._key, 0)
            setattr(ObservedAccumulator._tls, self._key, cur + term)

    # -- observe-path bridge -----------------------------------------------

    def _register_harvest_listener(self) -> None:
        """Register the JVM listener that harvests observe-path values (classic only, idempotent).

        On Spark Connect the client captures the values from the query response directly, so no
        listener registration is needed there.
        """
        from pyspark.sql.utils import is_remote

        sess = self._session
        if sess is None or is_remote():
            return
        jvm = getattr(sess, "_jvm", None)
        jsess = getattr(sess, "_jsparkSession", None)
        if jvm is not None and jsess is not None:
            try:
                jvm.org.apache.spark.sql.ObservedAccumulatorRegistry.ensureListener(jsess)
            except Exception:
                pass

    def _session_uuid(self) -> str:
        # The classic JVM registry is keyed by (session UUID, name) so same-named accumulators in
        # different sessions do not collide; read the creating session's UUID over py4j.
        return self._session._jsparkSession.sessionUUID()

    def _harvested_value(self) -> float:
        from pyspark.sql.utils import is_remote

        sess = self._session
        if sess is None:
            return 0
        if is_remote():
            # Spark Connect: read the client registry populated from server-injected metrics.
            client = getattr(sess, "_client", None)
            reg = getattr(client, "_observed_accumulator_registry", None) if client else None
            try:
                return float(reg.get(self._name, 0.0)) if reg is not None else 0.0
            except Exception:
                return 0.0
        # Classic: block until queued listener events are processed (so value reflects every
        # completed query), then read the driver-side JVM registry over py4j.
        jvm = getattr(sess, "_jvm", None)
        if jvm is None:
            return 0.0
        try:
            sess.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
        except Exception:
            pass
        try:
            return float(
                jvm.org.apache.spark.sql.ObservedAccumulatorRegistry.registryValue(
                    self._session_uuid(), self._name
                )
            )
        except Exception:
            return 0.0

    def _harvested_long(self) -> int:
        """Exact integer counterpart of ``_harvested_value``: the classic JVM ``LongAdder`` over
        py4j, or the Connect client's integer store."""
        from pyspark.sql.utils import is_remote

        sess = self._session
        if sess is None:
            return 0
        if is_remote():
            client = getattr(sess, "_client", None)
            reg = getattr(client, "_observed_accumulator_long", None) if client else None
            try:
                return int(reg.get(self._name, 0)) if reg is not None else 0
            except Exception:
                return 0
        jvm = getattr(sess, "_jvm", None)
        if jvm is None:
            return 0
        try:
            sess.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
            return int(
                jvm.org.apache.spark.sql.ObservedAccumulatorRegistry.registryValueLong(
                    self._session_uuid(), self._name
                )
            )
        except Exception:
            return 0

    def _harvested_custom_partials(self) -> List[bytes]:
        """Drain the harvested pickled partials for a scalar custom-merge accumulator (gathered by
        the analyzer's ``collect_list`` metric): the classic JVM store over py4j, or the Connect
        client store. Returns a list of ``bytes``; each is folded with ``merge`` by ``value``."""
        from pyspark.sql.utils import is_remote

        sess = self._session
        if sess is None:
            return []
        if is_remote():
            client = getattr(sess, "_client", None)
            store = getattr(client, "_observed_accumulator_custom", None) if client else None
            try:
                return list(store.pop(self._name, [])) if store is not None else []
            except Exception:
                return []
        jvm = getattr(sess, "_jvm", None)
        if jvm is None:
            return []
        try:
            sess.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
            arr = jvm.org.apache.spark.sql.ObservedAccumulatorRegistry.takeCustomPartials(
                self._session_uuid(), self._name
            )
            return [bytes(b) for b in arr] if arr is not None else []
        except Exception:
            return []

    def _register(self, observation: Any) -> None:
        """Track an Observation from a custom-accumulator operator application (driver-only)."""
        self._pending.append(observation)

    def _check_session(self) -> None:
        """Guard against cross-session use: an accumulator's value is harvested by the session that
        created it (its registry / client store), so touching it while a *different* session is
        active would silently read 0. Raise instead. Skips when the active session is unknown (so
        normal single-session use, where the active session is the creating one, never trips)."""
        sess = self._session
        if sess is None:
            return
        try:
            active = type(sess).getActiveSession()
        except Exception:
            return
        if active is not None and active is not sess:
            from pyspark.errors import PySparkRuntimeError

            raise PySparkRuntimeError(
                errorClass="OBSERVED_ACCUMULATOR_DIFFERENT_SESSION",
                messageParameters={"name": self._name},
            )

    @property
    def value(self) -> Any:
        """The accumulated value on the driver, cumulative across queries like a classic
        accumulator. Numeric: filled from the observe node the analyzer injected (int for an int
        ``zero``, float for a float ``zero``). Custom: fold the per-partition partials that were
        serialized, gathered via ``collect_list``, and returned through the Observation."""
        self._check_session()
        if self._merge is not None:
            import copy

            from pyspark import cloudpickle

            if self._folded is None:
                self._folded = copy.deepcopy(self._zero)
            metric = ObservedAccumulator.MetricPrefix + self._name
            for obs in self._pending:  # operator UDFs: obs.get blocks until the action has run
                for b in obs.get.get(metric, []) or []:
                    self._folded = self._merge(self._folded, cloudpickle.loads(bytes(b)))
            self._pending = []
            for b in self._harvested_custom_partials():  # scalar UDFs: harvested collect_list
                self._folded = self._merge(self._folded, cloudpickle.loads(bytes(b)))
            return self._folded
        if isinstance(self._zero, int) and not isinstance(self._zero, bool):
            # Integer accumulator. The scalar-UDF path is exact via the Long registry (matches a
            # classic LongAccumulator); the operator-UDF path sums a Double delta, added here
            # rounded. Both registries are cumulative and disjoint per path, so summing is correct.
            return self._zero + self._harvested_long() + int(round(self._harvested_value()))
        return self._zero + self._harvested_value()


# Bounds for the closure walk below: cap recursion depth and total nodes visited so inspecting a
# UDF that happens to capture a large object graph can never stall (or blow the stack during) UDF
# creation. Generous enough that realistic closures are traversed in full.
_WALK_MAX_DEPTH = 8
_WALK_MAX_NODES = 5000


def _find_accumulators_in_closure(func: Any) -> List["ObservedAccumulator"]:
    """Return every distinct :class:`ObservedAccumulator` reachable from ``func``, in a stable
    order. A UDF may reference several accumulators; each is harvested independently.

    Reachability is a bounded, cycle-safe, best-effort walk of the closure graph, so an accumulator
    is found however it is held: a free variable or module global; nested inside a
    ``list``/``tuple``/``set``/``dict`` (at any depth, e.g. ``[[acc]]`` or ``{"k": [acc]}``); an
    attribute of a captured object (``self.acc``); or referenced by a helper function the UDF calls
    (its closure and globals are followed too). Framework objects (``pyspark.*``), modules, and
    classes are not descended into, and every access is guarded, so detection never raises or walks
    into the JVM. The walk is bounded by :data:`_WALK_MAX_DEPTH` / :data:`_WALK_MAX_NODES`.

    Missing an accumulator here means it is silently not harvested (``value`` stays at ``zero``),
    so the walk errs toward finding too much rather than too little: over-detection only injects an
    observe whose delta is zero when the accumulator is never actually touched.
    """
    found = []
    seen_accs = set()
    visited = set()
    budget = [_WALK_MAX_NODES]

    def add(acc: "ObservedAccumulator") -> None:
        if id(acc) not in seen_accs:
            seen_accs.add(id(acc))
            found.append(acc)

    def walk_function(fn: Any, depth: int) -> None:
        for cell in getattr(fn, "__closure__", None) or ():
            try:
                walk(cell.cell_contents, depth)
            except ValueError:
                continue  # empty cell
        code = getattr(fn, "__code__", None)
        g = getattr(fn, "__globals__", None) or {}
        if code is not None:
            for nm in code.co_names:
                if nm in g:
                    walk(g[nm], depth)

    def walk(val: Any, depth: int) -> None:
        if val is None or depth > _WALK_MAX_DEPTH or budget[0] <= 0:
            return
        budget[0] -= 1
        if isinstance(val, ObservedAccumulator):
            add(val)
            return
        vid = id(val)
        if vid in visited:
            return
        visited.add(vid)
        try:
            if isinstance(val, (list, tuple, set, frozenset)):
                for v in val:
                    walk(v, depth + 1)
            elif isinstance(val, dict):
                for v in val.values():
                    walk(v, depth + 1)
            elif inspect.isfunction(val):
                walk_function(val, depth + 1)
            elif inspect.ismethod(val):
                walk(val.__func__, depth + 1)
                walk(getattr(val, "__self__", None), depth + 1)
            elif not _skip_object(val):
                for v in vars(val).values():
                    walk(v, depth + 1)
        except Exception:
            pass  # closure inspection must never break UDF creation

    walk_function(func, 0)
    return found


# Framework "gateway" types that lead into the JVM / large object graphs; the walk does not descend
# into them. A denylist of specific class names (matching the JVM-side ``skipObject``) rather than
# whole packages, so ordinary user objects -- even ones defined inside a ``pyspark.*`` test/util
# module -- are still walked. An :class:`ObservedAccumulator` is matched before this is consulted.
_SKIP_CLASS_NAMES = frozenset(
    {
        "SparkSession",
        "SparkContext",
        "SQLContext",
        "DataFrame",
        "Column",
        "RDD",
        "GroupedData",
        "DataFrameReader",
        "DataFrameWriter",
        "Catalog",
        "Broadcast",
        "SparkConf",
    }
)


def _skip_object(val: Any) -> bool:
    """Whether to avoid descending into ``val``'s attributes: objects with no ``__dict__`` (most
    builtins), modules, classes, py4j handles, and the framework gateway types in
    :data:`_SKIP_CLASS_NAMES`."""
    import types as _types

    if isinstance(val, (_types.ModuleType, type)):
        return True
    if not hasattr(val, "__dict__"):
        return True
    cls = type(val)
    if (getattr(cls, "__module__", "") or "").startswith("py4j"):
        return True
    return cls.__name__ in _SKIP_CLASS_NAMES


def _find_accumulator_in_closure(func: Any) -> Optional["ObservedAccumulator"]:
    """Return the first :class:`ObservedAccumulator` captured in ``func`` (or None). Kept for the
    operator-UDF path and closure-detection tests; see :func:`_find_accumulators_in_closure`."""
    accs = _find_accumulators_in_closure(func)
    return accs[0] if accs else None


def _maybe_transform_for_accumulator(udf_obj: Any) -> None:
    """If ``udf_obj``'s function captures an accumulator, rewrite it in place to emit the hidden
    ``struct(value, delta)``, tag it with the marker name, and mark it non-deterministic -- so a
    plain ``@udf`` that calls ``acc.add()`` is picked up by the JVM rule. No-op otherwise.
    Idempotent. Called from ``UserDefinedFunction`` before the JVM UDF is built.
    """
    if getattr(udf_obj, "_oa_transformed", False):
        return
    from pyspark.util import PythonEvalType

    et = getattr(udf_obj, "evalType", None)
    # Row-at-a-time scalar UDFs (plain and Arrow-optimized; the Arrow variant is still invoked per
    # row -- Arrow is only serialization): the wrapper returns a (value, delta) tuple per row.
    row_types = (PythonEvalType.SQL_BATCHED_UDF, PythonEvalType.SQL_ARROW_BATCHED_UDF)
    # Vectorized scalar UDFs (pandas / Arrow Series in, Series out): the wrapper returns a
    # struct batch whose delta column carries the batch-total delta on its first row (the observe
    # sum then totals it across batches). The user calls acc.add(count) once per batch.
    vector_types = (
        PythonEvalType.SQL_SCALAR_PANDAS_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
        PythonEvalType.SQL_SCALAR_ARROW_UDF,
        PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF,
    )
    if et not in row_types + vector_types:
        return
    func = getattr(udf_obj, "func", None)
    if func is None:
        return
    accs = _find_accumulators_in_closure(func)
    if not accs:
        return
    base = udf_obj.returnType
    base_dt = _parse_datatype_string(base) if isinstance(base, str) else base
    if not isinstance(base_dt, DataType):
        return
    original = func
    import copy

    from pyspark import cloudpickle

    # One hidden delta field per referenced accumulator, named after it so the JVM rule maps each
    # back. Numeric accumulators carry an exact Long (int zero) or Double delta the rule sums;
    # custom-merge accumulators carry a pickled partial the rule collect_lists and the driver folds
    # (see ObservedAccumulator.value). A UDF may reference several accumulators; all are harvested.
    def _delta_field(a: "ObservedAccumulator") -> StructField:
        dt: DataType
        if a._merge is not None:
            dt = BinaryType()
        elif isinstance(a._zero, int) and not isinstance(a._zero, bool):
            dt = LongType()
        else:
            dt = DoubleType()
        return StructField(a._name, dt, True)

    struct_t = StructType([StructField("v", base_dt, True)] + [_delta_field(a) for a in accs])

    if et in row_types:

        def wrapped(*args: Any) -> Any:
            tls = ObservedAccumulator._tls
            for a in accs:
                if a._merge is not None:
                    setattr(tls, a._objkey, copy.deepcopy(a._zero))
                else:
                    setattr(tls, a._key, 0)
            v = original(*args)
            deltas: List[Any] = []
            for a in accs:
                if a._merge is not None:
                    deltas.append(cloudpickle.dumps(getattr(tls, a._objkey, a._zero)))
                else:
                    d = getattr(tls, a._key, 0)
                    is_int = isinstance(a._zero, int) and not isinstance(a._zero, bool)
                    deltas.append(int(d) if is_int else float(d))
            return (v,) + tuple(deltas)

    else:  # vectorized scalar (pandas / Arrow): struct batch, each delta on the batch's first row

        def wrapped(*args: Any) -> Any:
            import pandas as pd

            tls = ObservedAccumulator._tls
            for a in accs:
                if a._merge is not None:
                    setattr(tls, a._objkey, copy.deepcopy(a._zero))
                else:
                    setattr(tls, a._key, 0)
            result = pd.Series(original(*args)).reset_index(drop=True)
            n = len(result)
            data = {"v": result}
            for a in accs:
                if a._merge is not None:
                    val = cloudpickle.dumps(getattr(tls, a._objkey, a._zero))
                    data[a._name] = pd.Series(([val] + [None] * (n - 1)) if n else [], dtype=object)
                else:
                    d = getattr(tls, a._key, 0)
                    is_int = isinstance(a._zero, int) and not isinstance(a._zero, bool)
                    fill = 0 if is_int else 0.0
                    dtype = "int64" if is_int else "float64"
                    data[a._name] = pd.Series(([d] + [fill] * (n - 1)) if n else [], dtype=dtype)
            return pd.DataFrame(data)

    udf_obj.func = wrapped
    udf_obj._returnType = struct_t  # returnType is a read-only property over _returnType
    udf_obj._returnType_placeholder = None  # drop any cached parsed return type
    udf_obj._name = ObservedAccumulator._MARKER_PREFIX + accs[0]._name
    udf_obj.deterministic = False
    udf_obj._oa_transformed = True


# ---------------------------------------------------------------------------
# Operator UDFs: mapInPandas / mapInArrow / applyInPandas / applyInArrow.
#
# These are separate logical operators, not a UDF inside a projection, so the JVM rule cannot
# rewrite them. Instead we (Python-side) add a hidden ``__oa_delta`` column to the operator's
# output, then attach an observe node named ``__oa_node_*`` whose metric key is
# ``__oa_metric_<name>`` and drop the column again. The classic harvest listener and the Connect
# client registry pick that up by name/key exactly as they do for the scalar path, so no
# extra JVM change is needed.
# ---------------------------------------------------------------------------

_DELTA_COL = "__oa_delta"


def _extend_schema(schema: Any) -> Optional[StructType]:
    """Append a nullable ``__oa_delta`` double field to a StructType or DDL-string schema."""
    dt = _parse_datatype_string(schema) if isinstance(schema, str) else schema
    if not isinstance(dt, StructType):
        return None
    return StructType(list(dt.fields) + [StructField(_DELTA_COL, DoubleType(), True)])


def _observe_delta(df: Any, acc: "ObservedAccumulator") -> Any:
    """Attach observe(sum(__oa_delta)) as a ``__oa_node_*`` node, then drop the column."""
    import uuid as _uuid
    from pyspark.sql import functions as sf

    node = ObservedAccumulator.NodePrefix + _uuid.uuid4().hex
    metric = ObservedAccumulator.MetricPrefix + acc._name
    return df.observe(node, sf.sum(sf.col(_DELTA_COL)).alias(metric)).drop(_DELTA_COL)


def _with_delta_pandas(pdf: Any, delta: Any) -> Any:
    import pandas as pd  # noqa: F401

    pdf = pdf.copy()
    col = [0.0] * len(pdf)
    if len(pdf) > 0:
        col[0] = float(delta)
    pdf[_DELTA_COL] = col
    return pdf


def _with_delta_arrow(batch: Any, delta: Any) -> Any:
    import pyarrow as pa

    n = batch.num_rows
    arr = pa.array([float(delta)] + [0.0] * (n - 1) if n else [], type=pa.float64())
    # Works for both pyarrow.RecordBatch and pyarrow.Table.
    return batch.append_column(_DELTA_COL, arr)


def _wrap_iter(func: Any, acc: "ObservedAccumulator", arrow: bool) -> Any:
    """Wrap an iterator->iterator map/applyIn function so each output batch carries the incremental
    delta (delta accumulated since the previous yielded batch, on its first row). ``*args`` is
    passed through so the applyIn ``(key, iterator)`` form works, and the signature is preserved for
    applyIn's own key/arity detection.

    Known edge case (documented): delta added strictly after the final output batch, or when the
    function yields no rows at all, is not captured.
    """
    import functools

    key = acc._key
    with_delta = _with_delta_arrow if arrow else _with_delta_pandas

    @functools.wraps(func)
    def wrapped(*args: Any) -> Any:
        setattr(ObservedAccumulator._tls, key, 0)
        emitted = 0.0
        last = None
        for batch in func(*args):
            if last is not None:
                cur = float(getattr(ObservedAccumulator._tls, key, 0))
                yield with_delta(last, cur - emitted)
                emitted = cur
            last = batch
        if last is not None:
            cur = float(getattr(ObservedAccumulator._tls, key, 0))
            yield with_delta(last, cur - emitted)

    # applyIn* inspects the arg count via getfullargspec (which ignores functools.wraps'
    # __wrapped__) to decide key/data arity; carry the original signature so it still detects it.
    wrapped.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return wrapped


def _wrap_applyin(func: Any, acc: "ObservedAccumulator", arrow: bool) -> Any:
    """Wrap a grouped map function (whole group in, whole group out) so the group's total delta
    rides on the first row of its output. Only the single-DataFrame form is handled; the signature
    is preserved so applyInPandas' own key/arity detection still works."""
    import functools

    key = acc._key
    with_delta = _with_delta_arrow if arrow else _with_delta_pandas

    @functools.wraps(func)
    def wrapped(*args: Any) -> Any:
        setattr(ObservedAccumulator._tls, key, 0)
        out = func(*args)
        d = getattr(ObservedAccumulator._tls, key, 0)
        return with_delta(out, d)

    wrapped.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return wrapped


# -- custom (arbitrary-merge) accumulators for pandas operators ---------------
#
# The general AccumulatorV2 case: the per-task partial is serialized (pickle) and emitted on a
# tagged marker row; the finalize step gathers the partials with collect_list into an Observation,
# then the driver folds them with the user's merge (see ObservedAccumulator.value). Supported for
# all four operator UDFs -- pandas and Arrow, map and applyIn (see maybe_wrap_operator). Not
# supported inside scalar UDFs: a per-row projection has no partition boundary to carry the
# partial on, so _maybe_transform_for_accumulator rejects a custom-merge accumulator there.

_BIN_COL = "__oa_bin"
_MARKER_COL = "__oa_marker"


def _extend_schema_custom(schema: Any) -> Any:
    dt = _parse_datatype_string(schema) if isinstance(schema, str) else schema
    if not isinstance(dt, StructType):
        return None, None
    ext = StructType(
        list(dt.fields)
        + [StructField(_BIN_COL, BinaryType(), True), StructField(_MARKER_COL, BooleanType(), True)]
    )
    return ext, dt


# -- per-batch tag / marker builders (pandas and Arrow) --


def _tag_data_pandas(pdf: Any) -> Any:
    pdf = pdf.copy()
    pdf[_BIN_COL] = None
    pdf[_MARKER_COL] = False
    return pdf


def _marker_pandas(field_names: Any, partial: Any) -> Any:
    import pandas as pd

    row: Dict[str, List[Any]] = {f: [None] for f in field_names}
    row[_BIN_COL] = [partial]
    row[_MARKER_COL] = [True]
    return pd.DataFrame(row)


def _concat_pandas(a: Any, b: Any) -> Any:
    import pandas as pd

    return pd.concat([a, b], ignore_index=True)


def _tag_data_arrow(batch: Any) -> Any:
    import pyarrow as pa

    n = batch.num_rows
    binarr = pa.nulls(n, type=pa.binary())
    markerarr = pa.array([False] * n, type=pa.bool_())
    if isinstance(batch, pa.Table):
        return batch.append_column(_BIN_COL, binarr).append_column(_MARKER_COL, markerarr)
    return pa.RecordBatch.from_arrays(
        list(batch.columns) + [binarr, markerarr],
        names=list(batch.schema.names) + [_BIN_COL, _MARKER_COL],
    )


def _marker_arrow(arrow_fields: Any, partial: Any) -> Any:
    import pyarrow as pa

    arrays = [pa.nulls(1, type=f.type) for f in arrow_fields]
    arrays += [pa.array([partial], type=pa.binary()), pa.array([True], type=pa.bool_())]
    names = [f.name for f in arrow_fields] + [_BIN_COL, _MARKER_COL]
    return pa.RecordBatch.from_arrays(arrays, names=names)


def _concat_arrow(tagged: Any, marker_rb: Any) -> Any:
    import pyarrow as pa

    left = tagged if isinstance(tagged, pa.Table) else pa.Table.from_batches([tagged])
    return pa.concat_tables([left, pa.Table.from_batches([marker_rb])])


def _wrap_iter_custom(func: Any, acc: "ObservedAccumulator", tag: Any, make_marker: Any) -> Any:
    import functools

    @functools.wraps(func)
    def wrapped(*args: Any) -> Any:
        import copy

        from pyspark import cloudpickle

        setattr(ObservedAccumulator._tls, acc._objkey, copy.deepcopy(acc._zero))
        for batch in func(*args):
            yield tag(batch)
        local = getattr(ObservedAccumulator._tls, acc._objkey, acc._zero)
        yield make_marker(cloudpickle.dumps(local))

    wrapped.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return wrapped


def _wrap_single_custom(
    func: Any, acc: "ObservedAccumulator", tag: Any, make_marker: Any, concat: Any
) -> Any:
    import functools

    @functools.wraps(func)
    def wrapped(*args: Any) -> Any:
        import copy

        from pyspark import cloudpickle

        setattr(ObservedAccumulator._tls, acc._objkey, copy.deepcopy(acc._zero))
        out = func(*args)
        local = getattr(ObservedAccumulator._tls, acc._objkey, acc._zero)
        return concat(tag(out), make_marker(cloudpickle.dumps(local)))

    wrapped.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return wrapped


def _observe_custom(df: Any, acc: "ObservedAccumulator") -> Any:
    import uuid as _uuid

    from pyspark.sql import Observation
    from pyspark.sql import functions as sf

    metric = ObservedAccumulator.MetricPrefix + acc._name
    obs = Observation("__oa_custom_" + _uuid.uuid4().hex)
    observed = df.observe(obs, sf.collect_list(sf.col(_BIN_COL)).alias(metric))
    acc._register(obs)
    keep = sf.coalesce(sf.col(_MARKER_COL), sf.lit(False)) == sf.lit(False)
    return observed.where(keep).drop(_BIN_COL, _MARKER_COL)


def maybe_wrap_operator(df: Any, func: Any, schema: Any, evalType: int) -> Any:
    """If ``func`` captures an accumulator, return (wrapped_func, extended_schema, finalize) for a
    map*/applyIn* operator; else None. ``finalize`` attaches the observe node and drops the hidden
    columns. ``evalType`` is the operator's ``PythonEvalType`` (map vs applyIn, pandas vs Arrow).
    """
    try:
        from pyspark.util import PythonEvalType

        acc = _find_accumulator_in_closure(func)
        if acc is None:
            return None
        arrow = evalType in (
            PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
            PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF,
            PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
        )
        is_iter = evalType in (
            PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
            PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
            PythonEvalType.SQL_GROUPED_MAP_PANDAS_ITER_UDF,
            PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
        )

        if acc._merge is not None:
            # Custom (arbitrary merge) via serialized partials + collect_list + driver fold, for
            # both pandas and Arrow operators.
            ext, orig = _extend_schema_custom(schema)
            if ext is None:
                return None
            tag: Any
            concat: Any
            if arrow:
                from pyspark.sql.pandas.types import to_arrow_schema

                arrow_fields = list(to_arrow_schema(orig))
                tag = _tag_data_arrow

                def make_marker(p: Any) -> Any:
                    return _marker_arrow(arrow_fields, p)

                concat = _concat_arrow
            else:
                names = [f.name for f in orig.fields]
                tag = _tag_data_pandas

                def make_marker(p: Any) -> Any:
                    return _marker_pandas(names, p)

                concat = _concat_pandas
            wrapped = (
                _wrap_iter_custom(func, acc, tag, make_marker)
                if is_iter
                else _wrap_single_custom(func, acc, tag, make_marker, concat)
            )
            return wrapped, ext, (lambda result: _observe_custom(result, acc))

        # Numeric (SQL sum) fast path.
        extended = _extend_schema(schema)
        if extended is None:
            return None
        wrapped = _wrap_iter(func, acc, arrow) if is_iter else _wrap_applyin(func, acc, arrow)
        return wrapped, extended, (lambda result: _observe_delta(result, acc))
    except Exception:
        return None  # never break ordinary map*/applyIn* on an accumulator-detection failure
