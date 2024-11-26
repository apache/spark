# syntax = edrevo/dockerfile-plus

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

FROM ubuntu:jammy-20240911.1
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image for Documentation"

ENV FULL_REFRESH_DATE 20241029

INCLUDE+ ./dev/spark-test-image/base/Dockerfile

INCLUDE+ ./dev/spark-test-image/base/Dockerfile.apt-get

INCLUDE+ ./dev/spark-test-image/base/Dockerfile.r

# Should unpin 'sphinxcontrib-*' after upgrading sphinx>5
# See 'ipython_genutils' in SPARK-38517
# See 'docutils<0.18.0' in SPARK-39421
INCLUDE+ ./dev/spark-test-image/base/Dockerfile.python
