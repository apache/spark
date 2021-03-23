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

ARG base_img

FROM $base_img
WORKDIR /

# Reset to root to run installation tasks
USER 0

RUN mkdir ${SPARK_HOME}/R

# Install R 3.6.3 (http://cloud.r-project.org/bin/linux/debian/)
RUN \
  apt-get update && \
  apt install -y gnupg && \
  echo "deb http://cloud.r-project.org/bin/linux/debian buster-cran35/" >> /etc/apt/sources.list && \
  (apt-key adv --keyserver keys.gnupg.net --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF' || apt-key adv --keyserver keys.openpgp.org --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF') && \
  apt-get update && \
  apt install -y -t buster-cran35 r-base r-base-dev && \
  rm -rf /var/cache/apt/*

COPY R ${SPARK_HOME}/R
ENV R_HOME /usr/lib/R

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
