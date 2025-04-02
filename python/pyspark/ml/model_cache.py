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
from collections import OrderedDict
from threading import Lock
from typing import Callable, Optional
from uuid import UUID


class ModelCache:
    """Cache for model prediction functions on executors.

    This requires the `spark.python.worker.reuse` configuration to be set to `true`, otherwise a
    new python worker (with an empty cache) will be started for every task.

    If a python worker is idle for more than one minute (per the IDLE_WORKER_TIMEOUT_NS setting in
    PythonWorkerFactory.scala), it will be killed, effectively clearing the cache until a new python
    worker is started.

    Caching large models can lead to out-of-memory conditions, which may require adjusting spark
    memory configurations, e.g. `spark.executor.memoryOverhead`.
    """

    _models: OrderedDict = OrderedDict()
    _capacity: int = 3  # "reasonable" default size for now, make configurable later, if needed
    _lock: Lock = Lock()

    @staticmethod
    def add(uuid: UUID, predict_fn: Callable) -> None:
        with ModelCache._lock:
            ModelCache._models[uuid] = predict_fn
            ModelCache._models.move_to_end(uuid)
            if len(ModelCache._models) > ModelCache._capacity:
                ModelCache._models.popitem(last=False)

    @staticmethod
    def get(uuid: UUID) -> Optional[Callable]:
        with ModelCache._lock:
            predict_fn = ModelCache._models.get(uuid)
            if predict_fn:
                ModelCache._models.move_to_end(uuid)
            return predict_fn
