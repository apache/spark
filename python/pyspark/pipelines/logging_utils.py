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

from datetime import datetime


def log_with_curr_timestamp(message: str) -> None:
    """
    Print a log message with a formatted timestamp corresponding to the current time. Note that
    currently only the UTC timezone is supported, but we plan to eventually support the timezone
    specified by the SESSION_LOCAL_TIMEZONE SQL conf.

    Args:
        message (str): The message to log
    """
    log_with_provided_timestamp(message, datetime.now())


def log_with_provided_timestamp(message: str, timestamp: datetime) -> None:
    """
    Print a log message with a formatted timestamp prefix.

    Args:
     message (str): The message to log
     timestamp(datetime): The timestamp to use for the log message.
    """
    formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{formatted_timestamp}: {message}")
