/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.nativeio;

/**
 * Enum representing POSIX errno values.
 */
public enum Errno {
  EPERM,
  ENOENT,
  ESRCH,
  EINTR,
  EIO,
  ENXIO,
  E2BIG,
  ENOEXEC,
  EBADF,
  ECHILD,
  EAGAIN,
  ENOMEM,
  EACCES,
  EFAULT,
  ENOTBLK,
  EBUSY,
  EEXIST,
  EXDEV,
  ENODEV,
  ENOTDIR,
  EISDIR,
  EINVAL,
  ENFILE,
  EMFILE,
  ENOTTY,
  ETXTBSY,
  EFBIG,
  ENOSPC,
  ESPIPE,
  EROFS,
  EMLINK,
  EPIPE,
  EDOM,
  ERANGE,

  UNKNOWN;
}
