# -*- encoding: utf-8 -*-
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

import logging
import json


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        log_entry.update(record.__dict__.get("kwargs", {}))
        return json.dumps(log_entry, ensure_ascii=False)


class PySparkLogger(logging.Logger):
    def __init__(self, name="PySparkLogger", level=logging.INFO, stream=None, filename=None):
        super().__init__(name, level)
        if not self.hasHandlers():
            if filename:
                self.handler = logging.FileHandler(filename)
            else:
                self.handler = logging.StreamHandler(stream)
            self.handler.setFormatter(JSONFormatter())
            self.addHandler(self.handler)
            self.setLevel(level)

    @staticmethod
    def get_logger(name="PySparkLogger", level=logging.INFO, stream=None, filename=None):
        return PySparkLogger(name, level, stream, filename)

    def log_info(self, message, **kwargs):
        self.info(message, exc_info=False, extra={"kwargs": kwargs})

    def log_warn(self, message, **kwargs):
        self.warning(message, exc_info=False, extra={"kwargs": kwargs})

    def log_error(self, message, **kwargs):
        self.error(message, exc_info=False, extra={"kwargs": kwargs})
