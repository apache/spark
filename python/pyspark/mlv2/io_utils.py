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
        extraMetadata: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Extract metadata of Estimator / Transformer / Model / Evaluator instance.
    """
    uid = instance.uid
    cls = instance.__module__ + "." + instance.__class__.__name__

    # User-supplied param values
    params = instance._paramMap
    jsonParams = {}
    for p in params:
        jsonParams[p.name] = params[p]

    # Default param values
    jsonDefaultParams = {}
    for p in instance._defaultParamMap:
        jsonDefaultParams[p.name] = instance._defaultParamMap[p]

    metadata = {
        "class": cls,
        "timestamp": int(round(time.time() * 1000)),
        "sparkVersion": pyspark_version,
        "uid": uid,
        "paramMap": jsonParams,
        "defaultParamMap": jsonDefaultParams,
        "type": "spark_connect",
    }
    if extraMetadata is not None:
        metadata.update(extraMetadata)

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

    def saveToLocal(self, path):
        metadata = self._get_metadata()

        os.makedirs(path)
        with open(os.path.join(path, _META_DATA_FILE_NAME), "w") as fp:
            json.dump(metadata, fp)

    @classmethod
    def loadFromLocal(cls, path):
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
