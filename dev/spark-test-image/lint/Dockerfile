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
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image for Linter"

ENV FULL_REFRESH_DATE 20241112

INCLUDE+ ./dev/spark-test-image/base/Dockerfile

INCLUDE+ ./dev/spark-test-image/base/Dockerfile.apt-get
RUN rm -rf /var/lib/apt/lists/*

INCLUDE+ ./dev/spark-test-image/base/Dockerfile.r
RUN Rscript -e "devtools::install_version('lintr', version='2.0.1', repos='https://cloud.r-project.org')"

INCLUDE+ ./dev/spark-test-image/base/Dockerfile.python
RUN python3.9 -m pip install \
    'grpcio-status==1.67.0' \
     'grpcio==1.67.0' \
    'matplotlib' \
    'numpy==2.0.2' \
    'pyarrow>=18.0.0' \
    && python3.9 -m pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu \
    && python3.9 -m pip install torcheval \
    && python3.9 -m pip cache purge
