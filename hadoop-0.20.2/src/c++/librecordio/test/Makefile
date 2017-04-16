#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

COPTS=-g3 -O0 -Wall

all: test testFromJava

test: ${LIBRECORDIO_TEST_DIR}/test.o ${LIBRECORDIO_TEST_DIR}/test.jr.o
	g++ -g3 -O0 -o ${LIBRECORDIO_TEST_DIR}/test ${LIBRECORDIO_TEST_DIR}/test.o \
	${LIBRECORDIO_TEST_DIR}/test.jr.o -L${LIBRECORDIO_BUILD_DIR} -L${XERCESCROOT}/lib -lrecordio -lxerces-c
	
${LIBRECORDIO_TEST_DIR}/test.o: test.cc
	g++ ${COPTS} -c -I .. -o ${LIBRECORDIO_TEST_DIR}/test.o test.cc

testFromJava: ${LIBRECORDIO_TEST_DIR}/testFromJava.o ${LIBRECORDIO_TEST_DIR}/test.jr.o
	g++ -g3 -O0 -o ${LIBRECORDIO_TEST_DIR}/testFromJava ${LIBRECORDIO_TEST_DIR}/testFromJava.o ${LIBRECORDIO_TEST_DIR}/test.jr.o \
	-L${LIBRECORDIO_BUILD_DIR} -L${XERCESCROOT}/lib -lrecordio -lxerces-c
	
${LIBRECORDIO_TEST_DIR}/testFromJava.o: testFromJava.cc
	g++ ${COPTS} -c -I.. -o ${LIBRECORDIO_TEST_DIR}/testFromJava.o testFromJava.cc

${LIBRECORDIO_TEST_DIR}/test.jr.o: test.jr.cc
	g++ ${COPTS} -c -I..  -o ${LIBRECORDIO_TEST_DIR}/test.jr.o test.jr.cc

%.jr.cc %.jr.hh: %.jr
	${HADOOP_HOME}/bin/rcc --language c++ $<

%: %.o
%: %.cc

test.cc: test.hh
test.hh: test.jr.hh ../recordio.hh ../filestream.hh

clean:
	rm -f ${LIBRECORDIO_TEST_DIR}/*~ ${LIBRECORDIO_TEST_DIR}/*.o ${LIBRECORDIO_TEST_DIR}/test \
	${LIBRECORDIO_TEST_DIR}/testFromJava ${LIBRECORDIO_TEST_DIR}/*.jr.*

