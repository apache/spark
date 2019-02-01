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

.PHONY: all publish base python r

BASE_IMAGE_NAME = palantirtechnologies/circle-spark-base:0.1.3
PYTHON_IMAGE_NAME = palantirtechnologies/circle-spark-python:0.1.3
R_IMAGE_NAME = palantirtechnologies/circle-spark-r:0.1.3

all: base python r

base: base/Dockerfile
	time docker build base -t ${BASE_IMAGE_NAME}

python: base python/Dockerfile
	time docker build python -t ${PYTHON_IMAGE_NAME}

r: base r/Dockerfile
	time docker build r -t ${R_IMAGE_NAME}

publish: base python r
	docker push ${BASE_IMAGE_NAME}
	docker push ${PYTHON_IMAGE_NAME}
	docker push ${R_IMAGE_NAME}
