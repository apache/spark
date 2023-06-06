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
import os
import time
from typing import Any, Dict, Optional
from pyspark.errors import PySparkNotImplementedError, PySparkRuntimeError
from pyspark.util import VersionUtils

from pyspark import __version__ as pyspark_version


_META_DATA_FILE_NAME = "metadata.json"


def _get_metadata_to_save(
        instance: "Params",
        extra_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Extract metadata of Estimator / Transformer / Model / Evaluator instance.
    """
    uid = instance.uid
    cls = instance.__module__ + "." + instance.__class__.__name__

    # User-supplied param values
    params = instance._paramMap
    json_params = {}
    for p in params:
        json_params[p.name] = params[p]

    # Default param values
    json_default_params = {}
    for p in instance._defaultParamMap:
        json_default_params[p.name] = instance.json_default_params[p]

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
        metadata.update(extra_metadata)

    return metadata


def _get_class(clazz: str) -> Any:
    """
    Loads Python class from its name.
    """
    parts = clazz.split(".")
    module = ".".join(parts[:-1])
    m = __import__(module, fromlist=[parts[-1]])
    return getattr(m, parts[-1])


class ParamsReadWrite:
    """
    The base interface Estimator / Transformer / Model / Evaluator needs to inherit
    for supporting saving and loading.
    """

    def _get_metadata(self) -> Any:
        """
        Returns metadata of the object as a JSON object.
        """
        return _get_metadata_to_save(self)

    # TODO: support saving to cloud storage file system.
    def saveToLocal(self, path):
        """
        Save model to provided local path.
        """
        metadata = self._get_metadata()

        os.makedirs(path)
        with open(os.path.join(path, _META_DATA_FILE_NAME), "w") as fp:
            json.dump(metadata, fp)

    # TODO: support loading from cloud storage file system.
    @classmethod
    def loadFromLocal(cls, path):
        """
        Load model from provided local path.
        """
        with open(os.path.join(path, _META_DATA_FILE_NAME), "r") as fp:
            metadata = json.load(fp)

        if "type" not in metadata or metadata["type"] != "spark_connect":
            raise RuntimeError("The model is not saved by spark ML under spark connect mode.")

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

        return instance


class ModelReadWrite:

    def _get_core_model_filename(self):
        """
        Returns the name of the file for saving the core model.
        """
        raise NotImplementedError()

    def _save_core_model(self, path):
        """
        Save the core model to provided path.
        Different pyspark models contain different type of core model,
        e.g. for LogisticRegressionModel, its core model is a pytorch model.
        """
        raise NotImplementedError()

    def _load_core_model(self, path):
        """
        Load the core model from provided path.
        """
        raise NotImplementedError()

    def saveToLocal(self, path):
        super(ModelReadWrite, self).save(path)
        self._save_core_model(os.path.join(path, self._get_core_model_filename()))

    @classmethod
    def loadFromLocal(cls, path):
        instance = super(ModelReadWrite, cls).loadFromLocal(path)
        instance._load_core_model(os.path.join(path, instance._get_core_model_filename()))
        return instance
