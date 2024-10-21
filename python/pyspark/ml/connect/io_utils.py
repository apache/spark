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

import json
import shutil
import os
import tempfile
import time
from urllib.parse import urlparse
from typing import Any, Dict, List

from pyspark.ml.base import Params
from pyspark.sql import SparkSession
from pyspark.sql.utils import is_remote
from pyspark import __version__ as pyspark_version


_META_DATA_FILE_NAME = "metadata.json"


def _copy_file_from_local_to_fs(local_path: str, dest_path: str) -> None:
    session = SparkSession.active()
    if is_remote():
        session.copyFromLocalToFs(local_path, dest_path)
    else:
        jvm = session.sparkContext._gateway.jvm  # type: ignore[union-attr]
        jvm.org.apache.spark.ml.python.MLUtil.copyFileFromLocalToFs(local_path, dest_path)


def _copy_dir_from_local_to_fs(local_path: str, dest_path: str) -> None:
    """
    Copy directory from local path to cloud storage path.
    Limitation: Currently only one level directory is supported.
    """
    assert os.path.isdir(local_path)

    file_list = os.listdir(local_path)
    for file_name in file_list:
        file_path = os.path.join(local_path, file_name)
        dest_file_path = os.path.join(dest_path, file_name)
        assert os.path.isfile(file_path)
        _copy_file_from_local_to_fs(file_path, dest_file_path)


def _get_class(clazz: str) -> Any:
    """
    Loads Python class from its name.
    """
    parts = clazz.split(".")
    module = ".".join(parts[:-1])
    m = __import__(module, fromlist=[parts[-1]])
    return getattr(m, parts[-1])


class ParamsReadWrite(Params):
    """
    The base interface Estimator / Transformer / Model / Evaluator needs to inherit
    for supporting saving and loading.
    """

    def _get_extra_metadata(self) -> Any:
        """
        Returns extra metadata of the instance
        """
        return None

    def _get_skip_saving_params(self) -> List[str]:
        """
        Returns params to be skipped when saving metadata.
        """
        return []

    def _get_metadata_to_save(self) -> Dict[str, Any]:
        """
        Extract metadata of Estimator / Transformer / Model / Evaluator instance.
        """
        extra_metadata = self._get_extra_metadata()
        skipped_params = self._get_skip_saving_params()

        uid = self.uid
        cls = self.__module__ + "." + self.__class__.__name__

        # User-supplied param values
        params = self._paramMap
        json_params = {}
        skipped_params = skipped_params or []
        for p in params:
            if p.name not in skipped_params:
                json_params[p.name] = params[p]

        # Default param values
        json_default_params = {}
        for p in self._defaultParamMap:
            json_default_params[p.name] = self._defaultParamMap[p]

        metadata = {
            "class": cls,
            "timestamp": int(round(time.time() * 1000)),
            "sparkVersion": pyspark_version,
            "uid": uid,
            "paramMap": json_params,
            "defaultParamMap": json_default_params,
            "type": "spark_connect",
        }
        if extra_metadata is not None:
            assert isinstance(extra_metadata, dict)
            metadata["extra"] = extra_metadata

        return metadata

    def _load_extra_metadata(self, metadata: Dict[str, Any]) -> None:
        """
        Load extra metadata attribute from metadata json object.
        """
        pass

    def _save_to_local(self, path: str) -> None:
        metadata = self._save_to_node_path(path, [])
        with open(os.path.join(path, _META_DATA_FILE_NAME), "w") as fp:
            json.dump(metadata, fp)

    def saveToLocal(self, path: str, *, overwrite: bool = False) -> None:
        """
        Save Estimator / Transformer / Model / Evaluator to provided local path.

        .. versionadded:: 3.5.0
        """
        if os.path.exists(path):
            if overwrite:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            else:
                raise ValueError(f"The path {path} already exists.")

        os.makedirs(path)
        self._save_to_local(path)

    @classmethod
    def _load_metadata(cls, metadata: Dict[str, Any]) -> "Params":
        if "type" not in metadata or metadata["type"] != "spark_connect":
            raise RuntimeError(
                "The saved data is not saved by ML algorithm implemented in 'pyspark.ml.connect' "
                "module."
            )

        class_name = metadata["class"]
        instance = _get_class(class_name)()
        instance._resetUid(metadata["uid"])

        # Set user-supplied param values
        for paramName in metadata["paramMap"]:
            param = instance.getParam(paramName)
            paramValue = metadata["paramMap"][paramName]
            instance.set(param, paramValue)

        for paramName in metadata["defaultParamMap"]:
            paramValue = metadata["defaultParamMap"][paramName]
            instance._setDefault(**{paramName: paramValue})

        if "extra" in metadata:
            instance._load_extra_metadata(metadata["extra"])
        return instance

    @classmethod
    def _load_instance_from_metadata(cls, metadata: Dict[str, Any], path: str) -> Any:
        instance = cls._load_metadata(metadata)

        if isinstance(instance, CoreModelReadWrite):
            core_model_path = metadata["core_model_path"]
            instance._load_core_model(os.path.join(path, core_model_path))

        if isinstance(instance, MetaAlgorithmReadWrite):
            instance._load_meta_algorithm(path, metadata)

        return instance

    @classmethod
    def _load_from_local(cls, path: str) -> "Params":
        with open(os.path.join(path, _META_DATA_FILE_NAME), "r") as fp:
            metadata = json.load(fp)

        return cls._load_instance_from_metadata(metadata, path)

    @classmethod
    def loadFromLocal(cls, path: str) -> "Params":
        """
        Load Estimator / Transformer / Model / Evaluator from provided local path.

        .. versionadded:: 3.5.0
        """
        return cls._load_from_local(path)

    def _save_to_node_path(self, root_path: str, node_path: List[str]) -> Any:
        """
        Save the instance to provided node path, and return the node metadata.
        """
        if isinstance(self, MetaAlgorithmReadWrite):
            metadata = self._save_meta_algorithm(root_path, node_path)
        else:
            metadata = self._get_metadata_to_save()
            if isinstance(self, CoreModelReadWrite):
                core_model_path = ".".join(node_path + [self._get_core_model_filename()])
                self._save_core_model(os.path.join(root_path, core_model_path))
                metadata["core_model_path"] = core_model_path

        return metadata

    def save(self, path: str, *, overwrite: bool = False) -> None:
        """
        Save Estimator / Transformer / Model / Evaluator to provided cloud storage path.

        .. versionadded:: 3.5.0
        """
        session = SparkSession.active()
        path_exist = True
        try:
            session.read.format("binaryFile").load(path).head()
        except Exception as e:
            if "Path does not exist" in str(e):
                path_exist = False
            else:
                # Unexpected error.
                raise e

        if path_exist and not overwrite:
            raise ValueError(f"The path {path} already exists.")

        tmp_local_dir = tempfile.mkdtemp(prefix="pyspark_ml_model_")
        try:
            self._save_to_local(tmp_local_dir)
            _copy_dir_from_local_to_fs(tmp_local_dir, path)
        finally:
            shutil.rmtree(tmp_local_dir, ignore_errors=True)

    @classmethod
    def load(cls, path: str) -> "Params":
        """
        Load Estimator / Transformer / Model / Evaluator from provided cloud storage path.

        .. versionadded:: 3.5.0
        """
        session = SparkSession.active()

        tmp_local_dir = tempfile.mkdtemp(prefix="pyspark_ml_model_")
        try:
            file_data_df = session.read.format("binaryFile").load(path)

            for row in file_data_df.toLocalIterator():
                file_name = os.path.basename(urlparse(row.path).path)
                file_content = bytes(row.content)
                with open(os.path.join(tmp_local_dir, file_name), "wb") as f:
                    f.write(file_content)

            return cls._load_from_local(tmp_local_dir)
        finally:
            shutil.rmtree(tmp_local_dir, ignore_errors=True)


class CoreModelReadWrite:
    def _get_core_model_filename(self) -> str:
        """
        Returns the name of the file for saving the core model.
        """
        raise NotImplementedError()

    def _save_core_model(self, path: str) -> None:
        """
        Save the core model to provided local path.
        Different pyspark models contain different type of core model,
        e.g. for LogisticRegressionModel, its core model is a pytorch model.
        """
        raise NotImplementedError()

    def _load_core_model(self, path: str) -> None:
        """
        Load the core model from provided local path.
        """
        raise NotImplementedError()


class MetaAlgorithmReadWrite(ParamsReadWrite):
    """
    Meta-algorithm such as pipeline and cross validator must implement this interface.
    """

    def _get_child_stages(self) -> List[Any]:
        raise NotImplementedError()

    def _save_meta_algorithm(self, root_path: str, node_path: List[str]) -> Dict[str, Any]:
        raise NotImplementedError()

    def _load_meta_algorithm(self, root_path: str, node_metadata: Dict[str, Any]) -> None:
        raise NotImplementedError()

    @staticmethod
    def _get_all_nested_stages(instance: Any) -> List[Any]:
        if isinstance(instance, MetaAlgorithmReadWrite):
            child_stages = instance._get_child_stages()
        else:
            child_stages = []

        nested_stages = []
        for stage in child_stages:
            nested_stages.extend(MetaAlgorithmReadWrite._get_all_nested_stages(stage))

        return [instance] + nested_stages

    @staticmethod
    def get_uid_map(instance: Any) -> Dict[str, Any]:
        all_nested_stages = MetaAlgorithmReadWrite._get_all_nested_stages(instance)
        uid_map = {stage.uid: stage for stage in all_nested_stages}
        if len(all_nested_stages) != len(uid_map):
            raise RuntimeError(
                f"{instance.__class__.__module__}.{instance.__class__.__name__}"
                f"is a compound estimator with stages with duplicate "
                f"UIDs. List of UIDs: {list(uid_map.keys())}."
            )
        return uid_map
