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

all: ${LIBRECORDIO_BUILD_DIR}/librecordio.a

COBJS = $(addprefix ${LIBRECORDIO_BUILD_DIR}/, recordio.o filestream.o binarchive.o csvarchive.o xmlarchive.o \
	exception.o typeIDs.o fieldTypeInfo.o recordTypeInfo.o utils.o)

CCMD = $(addprefix ${LIBRECORDIO_BUILD_DIR}/, librecordio.a recordio.o filestream.o binarchive.o csvarchive.o xmlarchive.o \
        exception.o typeIDs.o fieldTypeInfo.o recordTypeInfo.o utils.o)

${LIBRECORDIO_BUILD_DIR}/librecordio.a: ${COBJS}
	ar cru ${CCMD}

${LIBRECORDIO_BUILD_DIR}/recordio.o: recordio.cc recordio.hh archive.hh
	g++ ${COPTS} -c -I${XERCESCROOT}/include -o ${LIBRECORDIO_BUILD_DIR}/recordio.o recordio.cc

${LIBRECORDIO_BUILD_DIR}/filestream.o: filestream.cc recordio.hh filestream.hh
	g++ ${COPTS} -c -o ${LIBRECORDIO_BUILD_DIR}/filestream.o filestream.cc

${LIBRECORDIO_BUILD_DIR}/binarchive.o: binarchive.cc recordio.hh binarchive.hh archive.hh
	g++ ${COPTS} -c -o ${LIBRECORDIO_BUILD_DIR}/binarchive.o binarchive.cc

${LIBRECORDIO_BUILD_DIR}/csvarchive.o: csvarchive.cc recordio.hh csvarchive.hh archive.hh
	g++ ${COPTS} -c -o ${LIBRECORDIO_BUILD_DIR}/csvarchive.o csvarchive.cc

${LIBRECORDIO_BUILD_DIR}/xmlarchive.o: xmlarchive.cc recordio.hh xmlarchive.hh archive.hh
	g++ ${COPTS} -c -I${XERCESCROOT}/include -o ${LIBRECORDIO_BUILD_DIR}/xmlarchive.o xmlarchive.cc

${LIBRECORDIO_BUILD_DIR}/exception.o: exception.cc exception.hh
	g++ ${COPTS} -c -o ${LIBRECORDIO_BUILD_DIR}/exception.o exception.cc

${LIBRECORDIO_BUILD_DIR}/typeIDs.o: typeIDs.cc typeIDs.hh
	g++ ${COPTS} -c -o ${LIBRECORDIO_BUILD_DIR}/typeIDs.o typeIDs.cc
${LIBRECORDIO_BUILD_DIR}/fieldTypeInfo.o: fieldTypeInfo.cc fieldTypeInfo.hh
	g++ ${COPTS} -c -o ${LIBRECORDIO_BUILD_DIR}/fieldTypeInfo.o fieldTypeInfo.cc
${LIBRECORDIO_BUILD_DIR}/recordTypeInfo.o: recordTypeInfo.cc recordTypeInfo.hh
	g++ ${COPTS} -c -o ${LIBRECORDIO_BUILD_DIR}/recordTypeInfo.o recordTypeInfo.cc
${LIBRECORDIO_BUILD_DIR}/utils.o: utils.cc utils.hh
	g++ ${COPTS} -c -o ${LIBRECORDIO_BUILD_DIR}/utils.o utils.cc
recordio.cc: recordio.hh archive.hh exception.hh
filestream.cc: recordio.hh filestream.hh 
binarchive.cc: recordio.hh binarchive.hh 
csvarchive.cc: recordio.hh csvarchive.hh 
xmlarchive.cc: recordio.hh xmlarchive.hh 
exception.cc: exception.hh 
typeIDs.cc: typeIDs.hh
fieldTypeInfo.cc: fieldTypeInfo.hh
recordTypeInfo.cc: recordTypeInfo.hh
utils.cc: utils.hh

test: librecordio.a
	make -C test all

clean:
	rm -f ${LIBRECORDIO_BUILD_DIR}/*~ ${LIBRECORDIO_BUILD_DIR}/*.o ${LIBRECORDIO_BUILD_DIR}/*.a
	make -C test clean
