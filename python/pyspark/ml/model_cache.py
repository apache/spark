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
    """Cache for model prediction functions on executors."""

    _models: OrderedDict[UUID, Callable] = OrderedDict()
    _capacity: int = 8
    _lock: Lock = Lock()

    @staticmethod
    def add(uuid: UUID, predict_fn: Callable) -> None:
        ModelCache._lock.acquire()
        ModelCache._models[uuid] = predict_fn
        ModelCache._models.move_to_end(uuid)
        if len(ModelCache._models) > ModelCache._capacity:
            ModelCache._models.popitem(last=False)
        ModelCache._lock.release()

    @staticmethod
    def get(uuid: UUID) -> Optional[Callable]:
        ModelCache._lock.acquire()
        predict_fn = ModelCache._models.get(uuid)
        if predict_fn:
            ModelCache._models.move_to_end(uuid)
        ModelCache._lock.release()
        return predict_fn
