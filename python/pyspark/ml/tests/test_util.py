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

from collections.abc import Iterable
from contextlib import contextmanager
import os
from re import A
import textwrap
from typing import Iterator

import unittest

from pyspark import cloudpickle
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.util import MetaAlgorithmReadWrite, FunctionPickler
from pyspark.testing.mlutils import SparkSessionTestCase

class MetaAlgorithmReadWriteTests(SparkSessionTestCase):
     def test_getAllNestedStages(self):
         def _check_uid_set_equal(stages, expected_stages):
             uids = set(map(lambda x: x.uid, stages))
             expected_uids = set(map(lambda x: x.uid, expected_stages))
             self.assertEqual(uids, expected_uids)
 
         df1 = self.spark.createDataFrame(
             [
                 (Vectors.dense([1.0, 2.0]), 1.0),
                 (Vectors.dense([-1.0, -2.0]), 0.0),
             ],
             ["features", "label"],
         )
         df2 = self.spark.createDataFrame(
             [
                 (1.0, 2.0, 1.0),
                 (1.0, 2.0, 0.0),
             ],
             ["a", "b", "label"],
         )
         vs = VectorAssembler(inputCols=["a", "b"], outputCol="features")
         lr = LogisticRegression()
         pipeline = Pipeline(stages=[vs, lr])
         pipelineModel = pipeline.fit(df2)
         ova = OneVsRest(classifier=lr)
         ovaModel = ova.fit(df1)
 
         ova_pipeline = Pipeline(stages=[vs, ova])
         nested_pipeline = Pipeline(stages=[ova_pipeline])
 
         _check_uid_set_equal(
             MetaAlgorithmReadWrite.getAllNestedStages(pipeline), [pipeline, vs, lr]
         )
         _check_uid_set_equal(
             MetaAlgorithmReadWrite.getAllNestedStages(pipelineModel),
             [pipelineModel] + pipelineModel.stages,
         )
         _check_uid_set_equal(MetaAlgorithmReadWrite.getAllNestedStages(ova), [ova, lr])
         _check_uid_set_equal(
             MetaAlgorithmReadWrite.getAllNestedStages(ovaModel), [ovaModel, lr] + ovaModel.models
         )
         _check_uid_set_equal(
             MetaAlgorithmReadWrite.getAllNestedStages(nested_pipeline),
             [nested_pipeline, ova_pipeline, vs, ova, lr],
         )

# Function that will be used to test pickling.
def test_function(x: float, y: float) -> float:
    return x**2 + y**2

class TestFunctionPickler(unittest.TestCase):

    def check_if_test_function_pickled(self, f, og_fn, output_value, *arguments, **key_word_args):
        fn, args, kwargs = cloudpickle.load(f)
        self.assertEqual(fn, og_fn)
        self.assertEqual(args, arguments)
        self.assertEqual(kwargs, key_word_args)
        fn_output = fn(*args, **kwargs)
        self.assertEqual(fn_output, output_value)

    def test_pickle_fn_and_save(self):
        x, y = 1, 3 # args of test_function
        tmp_dir = "silly_goose"
        os.mkdir(tmp_dir)
        file_path_to_save = "silly_bear"
        with self.subTest(msg="See if it pickles correctly if no file_path or save_dir are specified"):
            pickled_fn_path = FunctionPickler.pickle_fn_and_save(test_function, "", "", x, y)
            with open(pickled_fn_path, "rb") as f:
                self.check_if_test_function_pickled(f, test_function, 10, x, y)
            os.remove(pickled_fn_path)
        with self.subTest(msg="See if pickles correctly and uses file path given as argument"):
            pickled_fn_path = FunctionPickler.pickle_fn_and_save(test_function, file_path_to_save, "", x, y)
            self.assertEqual(pickled_fn_path, file_path_to_save)
            with open(pickled_fn_path, "rb") as f:
                self.check_if_test_function_pickled(f, test_function, 10, x, y)
            os.remove(pickled_fn_path)
        with self.subTest(msg="See if pickles correctly and uses file path despite save_dir being specified"):
            pickled_fn_path = FunctionPickler.pickle_fn_and_save(test_function, file_path_to_save, tmp_dir, x, y)
            self.assertEqual(pickled_fn_path, file_path_to_save)
            with open(pickled_fn_path, "rb") as f:
                self.check_if_test_function_pickled(f, test_function, 10, x, y)
            os.remove(pickled_fn_path)

        os.rmdir(tmp_dir)

    def test_getting_output_from_pickle_file(self):
        a, b = 2, 0
        pickle_fn_file = FunctionPickler.pickle_fn_and_save(test_function, "", "", a, b)
        fn, args, kwargs = FunctionPickler.get_func_output(pickle_fn_file)
        self.assertEqual(fn, test_function)
        self.assertEqual(len(args), 2)
        self.assertEqual(len(kwargs), 0)
        self.assertEqual(args[0], a)
        self.assertEqual(args[1], b)
        self.assertEqual(fn(*args, **kwargs), 4)
        os.remove(pickle_fn_file)
    
    @contextmanager
    def create_reference_file(self, body: str, prefix: str = "", suffix: str = "", fname: str = "reference.py") -> Iterator[None]:
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

        
    def create_code_snippet_body(self, pickled_fn_path: str, fn_output_save_path: str) ->  str:
        code_snippet =  textwrap.dedent(
            f"""
                    from pyspark import cloudpickle
                    import os

                    if __name__ == "__main__":
                        with open("{pickled_fn_path}", "rb") as f:
                            train_fn, args, kwargs = cloudpickle.load(f)
                        output = train_fn(*args, **kwargs)
                        with open("{fn_output_save_path}", "wb") as f:
                            cloudpickle.dump(output, f)
                    """
        )
        return code_snippet

    def are_two_files_identical(self, fpath1: str, fpath2: str) -> bool:
        with open(fpath1, "rb") as f:
            contents_one = f.read()
        with open(fpath2, "rb") as f:
            contents_two = f.read()
        return contents_one == contents_two

    def test_create_training_script_from_func(self):
        arg1, arg2 = 3, 4
        pickled_fn_path = FunctionPickler.pickle_fn_and_save(test_function, "", "", arg1, arg2)
        fn_out_path = "output.pickled"
        reference_path = "ref_result_file.py"
        test_path = "test_result.py"
        body_for_reference = self.create_code_snippet_body(pickled_fn_path, fn_out_path)
        with self.subTest(msg="Check if it creates the correct file with no prefix nor suffix"):
            with self.create_reference_file(body_for_reference, prefix="", suffix="", fname=reference_path) as _:
                executable_file_path = FunctionPickler.create_training_script_from_func(fn_out_path, test_path, pickled_fn_path) 
                self.assertTrue(self.are_two_files_identical(reference_path, executable_file_path))
                os.remove(executable_file_path)
        prefix_test = "prefix_string = 'This is a prefix string'\n" 
        suffix_test = "suffix_string = 'this is a suffix string'\n"
        with self.subTest(msg="Check if it creates the correct file with only prefix + body"):
            with self.create_reference_file(body_for_reference, prefix=prefix_test, suffix="", fname=reference_path) as _:
                executable_file_path = FunctionPickler.create_training_script_from_func(fn_out_path, test_path, pickled_fn_path, prefix_code=prefix_test) 
                self.assertTrue(self.are_two_files_identical(reference_path, executable_file_path))
                os.remove(executable_file_path)
        with self.subTest(msg="Check if it creates the correct file with only suffix + body"):
            with self.create_reference_file(body_for_reference, prefix="", suffix=suffix_test, fname=reference_path) as _:
                executable_file_path = FunctionPickler.create_training_script_from_func(fn_out_path, test_path, pickled_fn_path, suffix_code=suffix_test) 
                self.assertTrue(self.are_two_files_identical(reference_path, executable_file_path))
                os.remove(executable_file_path)
        with self.subTest(msg="Check if it creates the correct file with prefix + suffix + body"):
            with self.create_reference_file(body_for_reference, prefix=prefix_test, suffix=suffix_test, fname=reference_path) as _:
                executable_file_path = FunctionPickler.create_training_script_from_func(fn_out_path, test_path, pickled_fn_path, prefix_code=prefix_test, suffix_code=suffix_test) 
                self.assertTrue(self.are_two_files_identical(reference_path, executable_file_path))
                os.remove(executable_file_path)
        os.remove(pickled_fn_path)

if __name__ == "__main__":
    from pyspark.ml.tests.test_util import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
