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
from contextlib import contextmanager
import os
import textwrap
from typing import Any, BinaryIO, Callable, Iterator
import unittest

from parameterized import parameterized

from pyspark import cloudpickle
from pyspark.ml.dl_util import FunctionPickler


class TestFunctionPickler(unittest.TestCase):
    # Function that will be used to test pickling.
    @staticmethod
    def _test_function(x: float, y: float) -> float:
        return x**2 + y**2

    def _check_if_test_function_pickled(
        self,
        file: BinaryIO,
        desired_function: Callable,
        output_value: Any,
        *arguments,
        **key_word_args,
    ):
        fn, args, kwargs = cloudpickle.load(file)
        self.assertEqual(fn, desired_function)
        self.assertEqual(args, arguments)
        self.assertEqual(kwargs, key_word_args)
        fn_output = fn(*args, **kwargs)
        self.assertEqual(fn_output, output_value)

    @parameterized.expand(
        [
            ("See if it pickles correctly with no path specified", "", ""),
            ("See if it pickles correctly with path specified", "silly_bear", ""),
            (
                "See if it pickles correctly with both path and save_dir specified",
                "silly_bear",
                "tmp_dir",
            ),
        ]
    )
    def test_pickle_fn_and_save(self, _: str, file_path_to_save: str, save_dir: str):
        x, y = 1, 3  # args of test_function
        if save_dir != "":
            os.makedirs(save_dir, exist_ok=True)
        pickled_fn_path = FunctionPickler.pickle_fn_and_save(
            TestFunctionPickler._test_function, file_path_to_save, save_dir, x, y
        )
        if file_path_to_save != "":
            self.assertEqual(file_path_to_save, pickled_fn_path)

        with open(pickled_fn_path, "rb") as f:
            self._check_if_test_function_pickled(f, TestFunctionPickler._test_function, 10, x, y)
        os.remove(pickled_fn_path)

        if save_dir != "":
            os.rmdir(save_dir)

    def test_getting_output_from_pickle_file(self):
        a, b = 2, 0  # arguments for _test_function
        pickle_fn_file = FunctionPickler.pickle_fn_and_save(
            TestFunctionPickler._test_function, "", "", a, b
        )
        fn, args, kwargs = FunctionPickler.get_fn_output(pickle_fn_file)
        self.assertEqual(fn, TestFunctionPickler._test_function)
        self.assertEqual(len(args), 2)
        self.assertEqual(len(kwargs), 0)
        self.assertEqual(args[0], a)
        self.assertEqual(args[1], b)
        self.assertEqual(fn(*args, **kwargs), 4)
        os.remove(pickle_fn_file)

    @contextmanager
    def create_reference_file(
        self, body: str, prefix: str = "", suffix: str = "", fname: str = "reference.py"
    ) -> Iterator[None]:
        try:
            with open(fname, "w") as f:
                if prefix != "":
                    f.write(prefix)
                f.write(body)
                if suffix != "":
                    f.write(suffix)
            yield
        finally:
            os.remove(fname)

    def _create_code_snippet_body(self, pickled_fn_path: str, fn_output_save_path: str) -> str:
        code_snippet = textwrap.dedent(
            f"""
                    from pyspark import cloudpickle
                    import os

                    if __name__ == "__main__":
                        with open("{pickled_fn_path}", "rb") as f:
                            fn, args, kwargs = cloudpickle.load(f)
                        output = fn(*args, **kwargs)
                        with open("{fn_output_save_path}", "wb") as f:
                            cloudpickle.dump(output, f)
                    """
        )
        return code_snippet

    def _are_two_files_identical(self, fpath1: str, fpath2: str) -> bool:
        with open(fpath1, "rb") as f:
            contents_one = f.read()
        with open(fpath2, "rb") as f:
            contents_two = f.read()
        self.assertEqual(contents_one, contents_two)
        return contents_one == contents_two

    @parameterized.expand(
        [
            ("Check if it creates the correct file with no prefix nor suffix", "", ""),
            (
                "Check if it creates the correct file with only prefix + body",
                "print('hello before')\n",
                "",
            ),
            (
                "Check if it creates the correct file with only suffix + body",
                "",
                "print('goodbye')",
            ),
            (
                "Check if it creates the correct file prefix, body, and suffix",
                "print('hello')\n",
                "print('goodbye')\n",
            ),
        ]
    )
    def test_create_fn_run_script(self, mesg: str, prefix_test: str, suffix_test: str):
        arg1, arg2 = 3, 4
        pickled_fn_path = FunctionPickler.pickle_fn_and_save(
            TestFunctionPickler._test_function, "", "", arg1, arg2
        )
        fn_out_path = "output.pickled"
        reference_path = "ref_result_file.py"
        test_path = "test_result.py"
        body_for_reference = self._create_code_snippet_body(pickled_fn_path, fn_out_path)

        with self.create_reference_file(
            body_for_reference, prefix=prefix_test, suffix=suffix_test, fname=reference_path
        ) as _:
            executable_file_path = FunctionPickler.create_fn_run_script(
                pickled_fn_path,
                fn_out_path,
                test_path,
                prefix_code=prefix_test,
                suffix_code=suffix_test,
            )
            self.assertTrue(self._are_two_files_identical(reference_path, executable_file_path))
            os.remove(executable_file_path)

        os.remove(pickled_fn_path)


if __name__ == "__main__":
    from pyspark.ml.tests.test_dl_util import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
