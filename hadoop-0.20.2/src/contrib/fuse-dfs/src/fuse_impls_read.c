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

#include "fuse_dfs.h"
#include "fuse_impls.h"
#include "fuse_file_handle.h"

static size_t min(const size_t x, const size_t y) {
  return x < y ? x : y;
}

/**
 * dfs_read
 *
 * Reads from dfs or the open file's buffer.  Note that fuse requires that
 * either the entire read be satisfied or the EOF is hit or direct_io is enabled
 *
 */
int dfs_read(const char *path, char *buf, size_t size, off_t offset,
                   struct fuse_file_info *fi)
{
  TRACE1("read",path)
  
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(dfs);
  assert(path);
  assert(buf);
  assert(offset >= 0);
  assert(size >= 0);
  assert(fi);

  dfs_fh *fh = (dfs_fh*)fi->fh;

  assert(fh != NULL);
  assert(fh->fs != NULL);
  assert(fh->hdfsFH != NULL);

  // special case this as simplifies the rest of the logic to know the caller wanted > 0 bytes
  if (size == 0)
    return 0;

  // If size is bigger than the read buffer, then just read right into the user supplied buffer
  if ( size >= dfs->rdbuffer_size) {
    int num_read;
    size_t total_read = 0;
    while (size - total_read > 0 && (num_read = hdfsPread(fh->fs, fh->hdfsFH, offset + total_read, buf + total_read, size - total_read)) > 0) {
      total_read += num_read;
    }
    // if there was an error before satisfying the current read, this logic declares it an error
    // and does not try to return any of the bytes read. Don't think it matters, so the code
    // is just being conservative.
    if (total_read < size && num_read < 0) {
      total_read = -EIO;
    }
    return total_read;
  }

  //
  // Critical section - protect from multiple reads in different threads accessing the read buffer
  // (no returns until end)
  //

  pthread_mutex_lock(&fh->mutex);

  // used only to check the postcondition of this function - namely that we satisfy
  // the entire read or EOF is hit.
  int isEOF = 0;
  int ret = 0;

  // check if the buffer is empty or
  // the read starts before the buffer starts or
  // the read ends after the buffer ends

  if (fh->bufferSize == 0  || 
      offset < fh->buffersStartOffset || 
      offset + size > fh->buffersStartOffset + fh->bufferSize) 
    {
      // Read into the buffer from DFS
      int num_read = 0;
      size_t total_read = 0;

      while (dfs->rdbuffer_size  - total_read > 0 &&
             (num_read = hdfsPread(fh->fs, fh->hdfsFH, offset + total_read, fh->buf + total_read, dfs->rdbuffer_size - total_read)) > 0) {
        total_read += num_read;
      }

      // if there was an error before satisfying the current read, this logic declares it an error
      // and does not try to return any of the bytes read. Don't think it matters, so the code
      // is just being conservative.
      if (total_read < size && num_read < 0) {
        // invalidate the buffer 
        fh->bufferSize = 0; 
        ERROR("pread failed for %s with return code %d", path, (int)num_read);
        ret = -EIO;
      } else {
        // Either EOF, all read or read beyond size, but then there was an error
        fh->bufferSize = total_read;
        fh->buffersStartOffset = offset;

        if (dfs->rdbuffer_size - total_read > 0) {
          // assert(num_read == 0); this should be true since if num_read < 0 handled above.
          isEOF = 1;
        }
      }
    }

  //
  // NOTE on EOF, fh->bufferSize == 0 and ret = 0 ,so the logic for copying data into the caller's buffer is bypassed, and
  //  the code returns 0 as required
  //
  if (ret == 0 && fh->bufferSize > 0) {

    assert(offset >= fh->buffersStartOffset);
    assert(fh->buf);

    const size_t bufferReadIndex = offset - fh->buffersStartOffset;
    assert(bufferReadIndex >= 0 && bufferReadIndex < fh->bufferSize);

    const size_t amount = min(fh->buffersStartOffset + fh->bufferSize - offset, size);
    assert(amount >= 0 && amount <= fh->bufferSize);

    const char *offsetPtr = fh->buf + bufferReadIndex;
    assert(offsetPtr >= fh->buf);
    assert(offsetPtr + amount <= fh->buf + fh->bufferSize);
    
    memcpy(buf, offsetPtr, amount);

    ret = amount;
  }

  //
  // Critical section end 
  //
  pthread_mutex_unlock(&fh->mutex);
 
  // fuse requires the below and the code should guarantee this assertion
  // 3 cases on return:
  //   1. entire read satisfied
  //   2. partial read and isEOF - including 0 size read
  //   3. error 
  assert(ret == size || isEOF || ret < 0);

 return ret;
}
