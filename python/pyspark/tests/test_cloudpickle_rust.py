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
import base64
import dataclasses
import enum
import functools
import io
import json
import os
import pickle
import shutil
import subprocess
import sys
import tempfile
import types
import typing
import unittest

from pyspark import cloudpickle
from pyspark.serializers import (
    CloudPickleSerializer,
    CPickleSerializer,
    PickleSerializer,
    _cloudpickle,
    _cloudpickle_dumps,
    _cloudpickle_loads,
)
from pyspark.testing.utils import (
    ReusedPySparkTestCase,
    cloudpickle_rust_requirement_message,
    have_cloudpickle_rust,
)

# Whether this process pickles with cloudpickle_rust: it is installed and not disabled.
uses_cloudpickle_rust = _cloudpickle().__name__ == "cloudpickle_rust"
cloudpickle_rust_unused_message = (
    cloudpickle_rust_requirement_message or "PYSPARK_CLOUDPICKLE_RUST=0 disables cloudpickle_rust"
)


def _make_payloads():
    """
    Objects that cloudpickle pickles by value, which use its reconstructors. They only reference
    the standard library, so that loading them does not need this module.
    """
    factor = 3
    offset = [10]

    def closure(x, y=2, *args, z=4, **kwargs):
        return x * factor + offset[0] + y + z + len(args) + len(kwargs)

    def outer(n):
        def middle(m):
            def inner(k):
                return n * 100 + m * 10 + k

            return inner

        return middle

    def fact(n):
        return 1 if n <= 1 else n * fact(n - 1)

    def squares(n):
        for i in range(n):
            yield i * i

    def with_empty_cell():
        if False:
            cell = None

        def get():
            return cell

        return get

    class Point:
        __slots__ = ("x", "y")

        def __init__(self, x, y):
            self.x, self.y = x, y

        def norm2(self):
            return self.x * self.x + self.y * self.y

        @property
        def swapped(self):
            return type(self)(self.y, self.x)

        @classmethod
        def origin(cls):
            return cls(0, 0)

        @staticmethod
        def add(a, b):
            return a + b

    class Color(enum.Enum):
        RED = 1
        GREEN = 2

    class Perm(enum.IntFlag):
        R = 4
        W = 2

    @dataclasses.dataclass(frozen=True)
    class Record:
        a: int
        b: typing.List[int] = dataclasses.field(default_factory=list)

    T = typing.TypeVar("T", bound=int)

    class Box(typing.Generic[T]):
        def __init__(self, value):
            self.value = value

    class Pair(typing.NamedTuple):
        key: str
        value: int = 0

    module = types.ModuleType("pyspark_test_cloudpickle_rust_dynamic")
    exec("X = 41\n\ndef g():\n    return X + 1\n", module.__dict__)

    def compose(funcs):
        return functools.reduce(lambda f, g: (lambda split, it: g(split, f(split, it))), funcs)

    return {
        "lambda": lambda x: x * 2 + 1,
        "closure": closure,
        "nested_functions": outer(1)(2),
        "recursive_function": fact,
        "generator_function": squares,
        "empty_cell": with_empty_cell(),
        "partial": functools.partial(closure, 1, z=5),
        "dynamic_class": Point,
        "dynamic_class_instance": Point(3, 4),
        "dynamic_enum": (Color, Color.GREEN, Perm, Perm.R | Perm.W),
        "dataclass": (Record, Record(1, [2])),
        "typing_generics": (T, Box, Box[int], typing.Dict[str, typing.List[int]]),
        "named_tuple": (Pair, Pair("k", 1)),
        "dict_views": ({"a": 1}.keys(), {"a": 1}.values(), {"a": 1}.items()),
        "dynamic_module": module,
        "rdd_like_pipeline": compose(
            [
                lambda split, it: map(lambda x: x + factor, it),
                lambda split, it: filter(lambda x: x % 2 == 0, it),
                lambda split, it: ((x, split) for x in it),
            ]
        ),
    }


def _exercise(name, obj):
    """
    Return what the payload `name` does, as JSON-compatible values, to compare an object with a
    copy loaded in another process.
    """
    if name == "lambda":
        # The code keeps its original filename, like with the vendored cloudpickle 3.1.
        return [obj(5), os.path.basename(obj.__code__.co_filename)]
    if name == "closure":
        return obj(1, 2, 3, z=4, k=5)
    if name == "nested_functions":
        return obj(3)
    if name == "recursive_function":
        return obj(6)
    if name == "generator_function":
        return list(obj(4))
    if name == "empty_cell":
        try:
            obj()
        except NameError as e:
            return type(e).__name__
        return None
    if name == "partial":
        return obj(7)
    if name == "dynamic_class":
        point = obj(1, 2)
        return [point.norm2(), point.swapped.x, obj.origin().norm2(), obj.add(2, 3), obj.__name__]
    if name == "dynamic_class_instance":
        return [type(obj).__name__, obj.x, obj.y, obj.norm2()]
    if name == "dynamic_enum":
        color, green, perm, read_write = obj
        return [[m.name for m in color], color(2) is green, int(read_write), perm(6) == read_write]
    if name == "dataclass":
        record_type, record = obj
        try:
            record.a = 2
            frozen = False
        except dataclasses.FrozenInstanceError:
            frozen = True
        return [dataclasses.asdict(record), record_type(5).b, frozen]
    if name == "typing_generics":
        type_var, box, box_of_int, dict_alias = obj
        return [
            type_var.__name__,
            type_var.__bound__ is int,
            box(3).value,
            box_of_int.__origin__ is box,
            str(dict_alias),
        ]
    if name == "named_tuple":
        pair_type, pair = obj
        return [pair._asdict(), pair_type("x")._asdict(), isinstance(pair, pair_type)]
    if name == "dict_views":
        return [sorted(view) for view in obj]
    if name == "dynamic_module":
        return [obj.__name__, obj.X, obj.g()]
    if name == "rdd_like_pipeline":
        return list(obj(7, iter(range(6))))
    raise ValueError(name)


class _RecordingUnpickler(pickle.Unpickler):
    def __init__(self, data):
        super().__init__(io.BytesIO(data))
        self.modules = set()

    def find_class(self, module, name):
        self.modules.add(module)
        return super().find_class(module, name)


def _global_modules(data):
    """The modules of the globals that the payload `data` references."""
    unpickler = _RecordingUnpickler(data)
    unpickler.load()
    return unpickler.modules


# Loads the payloads of the file argv[1] where cloudpickle_rust cannot be imported, and prints
# what each payload does, loaded by each PySpark loader.
_LOAD_WITHOUT_CLOUDPICKLE_RUST = """
import json
import os
import pickle
import sys

sys.modules["cloudpickle_rust"] = None
# The loading process stands for a Python worker, the only process that imports worker_util.
os.environ["SPARK_PYTHON_RUNTIME"] = "PYTHON_WORKER"

from pyspark.serializers import CloudPickleSerializer, CPickleSerializer, PickleSerializer
from pyspark.tests.test_cloudpickle_rust import _exercise
from pyspark.worker_util import read_command

with open(sys.argv[1], "rb") as f:
    payloads = pickle.load(f)
results = {}
for name, data in payloads.items():
    try:
        pickle.loads(data)
        missing = None
    except ModuleNotFoundError as e:
        missing = e.name
    results[name] = {
        "pickle.loads": missing,
        "CloudPickleSerializer": _exercise(name, CloudPickleSerializer().loads(data)),
        "PickleSerializer": _exercise(name, PickleSerializer().loads(data)),
        "read_command": _exercise(name, read_command(CPickleSerializer(), data)),
    }
print(json.dumps(results))
"""


class CloudPickleRustTests(unittest.TestCase):
    def run_python(self, code, *args, env_value=None):
        env = dict(os.environ)
        env.pop("PYSPARK_CLOUDPICKLE_RUST", None)
        if env_value is not None:
            env["PYSPARK_CLOUDPICKLE_RUST"] = env_value
        result = subprocess.run(
            [sys.executable, "-c", code, *args], env=env, capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip().splitlines()[-1]

    def test_choice(self):
        code = "from pyspark.serializers import _cloudpickle; print(_cloudpickle().__name__)"
        expected = "cloudpickle_rust" if have_cloudpickle_rust else "pyspark.cloudpickle"
        self.assertEqual(self.run_python(code), expected)
        self.assertEqual(self.run_python(code, env_value="0"), "pyspark.cloudpickle")

    def test_old_cloudpickle_rust(self):
        # Before 0.3.0, cloudpickle_rust has no DYNAMIC_CODE_FILENAME: the vendored one is used.
        with tempfile.TemporaryDirectory() as d:
            os.mkdir(os.path.join(d, "cloudpickle_rust"))
            with open(os.path.join(d, "cloudpickle_rust", "__init__.py"), "w") as f:
                f.write("from . import cloudpickle\n")
            with open(os.path.join(d, "cloudpickle_rust", "cloudpickle.py"), "w") as f:
                f.write("_PICKLE_BY_VALUE_MODULES = set()\n")
            code = (
                "import sys; sys.path.insert(0, sys.argv[1]); "
                "from pyspark.serializers import _cloudpickle; print(_cloudpickle().__name__)"
            )
            self.assertEqual(self.run_python(code, d), "pyspark.cloudpickle")

    def test_disabled(self):
        code = (
            "from pyspark.serializers import CloudPickleSerializer; "
            "from pyspark.tests.test_cloudpickle_rust import _global_modules; "
            "print(sorted(_global_modules(CloudPickleSerializer().dumps(lambda: 1))))"
        )
        self.assertEqual(
            self.run_python(code, env_value="0"), "['pyspark.cloudpickle.cloudpickle']"
        )

    @unittest.skipIf(not uses_cloudpickle_rust, cloudpickle_rust_unused_message)
    def test_pickles_with_cloudpickle_rust(self):
        for data in [CloudPickleSerializer().dumps(lambda x: x + 1), _cloudpickle_dumps(lambda: 1)]:
            modules = _global_modules(data)
            self.assertIn("cloudpickle_rust.cloudpickle", modules)
            self.assertNotIn("pyspark.cloudpickle.cloudpickle", modules)
        self.assertEqual(CloudPickleSerializer().loads(data)(), 1)

    @unittest.skipIf(not uses_cloudpickle_rust, cloudpickle_rust_unused_message)
    def test_load_without_cloudpickle_rust(self):
        payloads = _make_payloads()
        expected = json.loads(json.dumps({k: _exercise(k, v) for k, v in payloads.items()}))
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "payloads.pickle")
            with open(path, "wb") as f:
                pickle.dump({k: CloudPickleSerializer().dumps(v) for k, v in payloads.items()}, f)
            results = json.loads(self.run_python(_LOAD_WITHOUT_CLOUDPICKLE_RUST, path))
        self.assertEqual(sorted(results), sorted(payloads))
        for name, result in results.items():
            with self.subTest(payload=name):
                # pickle alone cannot load the payload there.
                self.assertRegex(str(result.pop("pickle.loads")), r"^cloudpickle_rust(\.|$)")
                for loader, value in result.items():
                    self.assertEqual(value, expected[name], loader)

    def test_code_filename(self):
        def f():
            return 1

        g = CloudPickleSerializer().loads(CloudPickleSerializer().dumps(f))
        self.assertIsNot(g, f)
        self.assertEqual(g.__code__.co_filename, f.__code__.co_filename)

    @unittest.skipIf(not uses_cloudpickle_rust, cloudpickle_rust_unused_message)
    def test_register_pickle_by_value(self):
        import cloudpickle_rust

        module = types.ModuleType("pyspark_test_cloudpickle_rust_by_value")
        exec("def f(x):\n    return x + 1\n", module.__dict__)
        sys.modules[module.__name__] = module
        try:
            # Registered with the vendored cloudpickle, unregistered with cloudpickle_rust.
            cloudpickle.register_pickle_by_value(module)
            try:
                registry = cloudpickle_rust.cloudpickle.list_registry_pickle_by_value()
                self.assertIn(module.__name__, registry)
                data = CloudPickleSerializer().dumps(module.f)
            finally:
                cloudpickle_rust.unregister_pickle_by_value(module)
            registry = cloudpickle.cloudpickle.list_registry_pickle_by_value()
            self.assertNotIn(module.__name__, registry)
        finally:
            del sys.modules[module.__name__]
        # Pickled by value: the module is not needed to load it.
        self.assertEqual(CloudPickleSerializer().loads(data)(1), 2)

    @unittest.skipIf(not uses_cloudpickle_rust, cloudpickle_rust_unused_message)
    def test_udt_serialized_class(self):
        # Schemas are persisted, e.g. in Parquet metadata: the class of a UDT is pickled with the
        # vendored cloudpickle, whose payloads every PySpark version loads.
        from pyspark.sql.types import DoubleType, UserDefinedType

        class LocalUDT(UserDefinedType):
            @classmethod
            def sqlType(cls):
                return DoubleType()

            @classmethod
            def module(cls):
                return "__main__"

        modules = _global_modules(base64.b64decode(LocalUDT().jsonValue()["serializedClass"]))
        self.assertIn("pyspark.cloudpickle.cloudpickle", modules)
        self.assertNotIn("cloudpickle_rust.cloudpickle", modules)

    def test_load_vendored_payloads(self):
        data = cloudpickle.dumps(lambda: 3)
        for loader in [_cloudpickle_loads, CloudPickleSerializer().loads, PickleSerializer().loads]:
            self.assertEqual(loader(data)(), 3)

    def test_other_import_errors(self):
        # A GLOBAL opcode of a missing module other than cloudpickle_rust.
        data = b"cpyspark_test_no_such_module\nf\n."
        loaders = [_cloudpickle_loads, CloudPickleSerializer().loads, CPickleSerializer().loads]
        for loader in loaders:
            with self.assertRaises(ModuleNotFoundError) as ctx:
                loader(data)
            self.assertEqual(ctx.exception.name, "pyspark_test_no_such_module")


@unittest.skipIf(not uses_cloudpickle_rust, cloudpickle_rust_unused_message)
class CloudPickleRustWorkerTests(ReusedPySparkTestCase):
    """The driver pickles with cloudpickle_rust, which the Python workers cannot import."""

    @classmethod
    def setUpClass(cls):
        cls.shadow_dir = tempfile.mkdtemp()
        os.mkdir(os.path.join(cls.shadow_dir, "cloudpickle_rust"))
        with open(os.path.join(cls.shadow_dir, "cloudpickle_rust", "__init__.py"), "w") as f:
            f.write(
                "raise ModuleNotFoundError(\n"
                "    \"No module named 'cloudpickle_rust'\", name='cloudpickle_rust'\n"
                ")\n"
            )
        try:
            super().setUpClass()
        except BaseException:
            shutil.rmtree(cls.shadow_dir, ignore_errors=True)
            raise

    @classmethod
    def tearDownClass(cls):
        try:
            super().tearDownClass()
        finally:
            shutil.rmtree(cls.shadow_dir, ignore_errors=True)

    @classmethod
    def conf(cls):
        # The Python workers find this cloudpickle_rust, which fails to import, before the
        # installed one.
        return super().conf().set("spark.executorEnv.PYTHONPATH", cls.shadow_dir)

    def test_workers_cannot_import_cloudpickle_rust(self):
        def importable(_):
            try:
                import cloudpickle_rust  # noqa: F401
            except ImportError:
                return False
            return True

        self.assertEqual(self.sc.parallelize([0], 1).map(importable).collect(), [False])

    def test_rdd(self):
        payloads = _make_payloads()
        point = payloads["dynamic_class"]
        factor = 3
        rdd = self.sc.parallelize(range(10), 4)
        self.assertEqual(
            rdd.map(lambda x: point(x, factor).norm2()).collect(),
            [x * x + 9 for x in range(10)],
        )
        self.assertEqual(
            sorted(rdd.map(lambda x: (x % 3, x)).reduceByKey(lambda a, b: a + b).collect()),
            [(0, 18), (1, 12), (2, 15)],
        )
        # The partitions are [0, 1], [2, 3, 4], [5, 6] and [7, 8, 9].
        self.assertEqual(
            rdd.mapPartitionsWithIndex(payloads["rdd_like_pipeline"]).collect(),
            [(4, 0), (6, 1), (8, 2), (10, 3), (12, 3)],
        )
        # The workers pickle with the vendored cloudpickle, which the driver loads.
        adders = rdd.map(lambda x: lambda y: x + y).collect()
        self.assertEqual([add(1) for add in adders], list(range(1, 11)))

    def test_broadcast_command(self):
        # A command larger than the broadcast threshold (1 MiB by default) is broadcast.
        big = list(range(500000))
        self.assertGreater(len(CloudPickleSerializer().dumps(lambda: big)), 1 << 20)
        self.assertEqual(
            self.sc.parallelize([1, 2], 2).map(lambda x: x + len(big)).collect(),
            [500001, 500002],
        )

    def test_python_udf(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import udf

        spark = SparkSession(self.sc)
        offset = 10
        plus = udf(lambda x: x + offset, "long")
        self.assertEqual(
            [row.v for row in spark.range(3).select(plus("id").alias("v")).collect()],
            [10, 11, 12],
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
