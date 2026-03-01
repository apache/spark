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

"""
Arrow <-> PyArrow zero-copy bridge for in-process Python UDF execution.

Both input and output use the Arrow C Data Interface (CDI) for zero-copy transfer.

Input path (JVM -> Python, zero-copy via CDI):
    The JVM calls ``Data.exportVector`` to fill pre-allocated ``ArrowArray`` /
    ``ArrowSchema`` C structs and passes their native addresses to Python.
    Python calls ``pa.Array._import_from_c(array_ptr, schema_ptr)`` to wrap the
    same Arrow buffers as a PyArrow array -- no memcpy. When Python GCs the array,
    the CDI release callback decrements the buffer reference counts.

Output path (Python -> JVM, zero-copy via CDI):
    The JVM pre-allocates ``ArrowArray`` and ``ArrowSchema`` C structs and passes
    their native addresses to Python. Python calls
    ``arr._export_to_c(output_array_ptr, output_schema_ptr)`` to fill those
    JVM-owned structs in-place. The JVM then calls ``Data.importVector`` which
    wraps the data buffers via ``ReferenceCountedArrowArray`` (zero-copy).
    When the imported FieldVector is closed, PyArrow's CDI release callback is
    invoked, decrementing the Python array refcount and allowing GC.
"""
