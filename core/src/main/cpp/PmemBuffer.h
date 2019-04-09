#include <string>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <mutex>
#include <cstring>
using namespace std;

#define DEFAULT_BUFSIZE 4096*1024+512

class PmemBuffer {
public:
  PmemBuffer() {
    buf_data_capacity = 0;
    remaining = 0;
    remaining_dirty = 0;
    pos = 0;
    pos_dirty = 0;
  }

  ~PmemBuffer() {
    if (buf_data != nullptr)
      free(buf_data);
  }

  int load(char* pmem_data_addr, int pmem_data_len) {
    std::lock_guard<std::mutex> lock(buffer_mtx);
    if (buf_data_capacity == 0) {
      buf_data = (char*)malloc(sizeof(char*) * pmem_data_len);
    }

    buf_data_capacity = remaining + pmem_data_len;
    if (remaining > 0) {
      char* tmp_buf_data = buf_data;
      buf_data = (char*)malloc(sizeof(char*) * buf_data_capacity);
      memcpy(buf_data, tmp_buf_data + pos, remaining);
      free(tmp_buf_data);
    }

    pos = remaining;
    memcpy(buf_data + pos, pmem_data_addr, pmem_data_len);
    remaining += pmem_data_len;
    pos = 0;
    pos_dirty = pos + remaining;
    remaining_dirty = 0;
    return pmem_data_len;
  }

  int getRemaining() {
    std::lock_guard<std::mutex> lock(buffer_mtx);
    return remaining;
  }

  void clean() {
    std::lock_guard<std::mutex> lock(buffer_mtx);
    buf_data_capacity = 0;
    remaining = 0;
    pos = 0;
    remaining_dirty = 0;
    pos_dirty = 0;
    free(buf_data);
    buf_data = nullptr;
  }

  char* getDataForFlush(int size) {
    std::lock_guard<std::mutex> lock(buffer_mtx);
    if (remaining_dirty < size || remaining_dirty == 0) {
      return nullptr;
    }
    int orig_pos = pos_dirty;
    pos_dirty += size;
    remaining_dirty -= size;
    return (buf_data + orig_pos);
  }

  int read(char* ret_data, int len) {
    std::lock_guard<std::mutex> lock(buffer_mtx);
    int read_len = min(len, remaining);
    memcpy(ret_data, buf_data + pos, read_len);
    if ((pos + read_len) > pos_dirty) {
      remaining_dirty -= pos + read_len - pos_dirty;
      pos_dirty = pos + read_len;
    }
    pos += read_len;
    remaining -= read_len;
    return read_len; 
  }

  int write(char* data, int len) {
    std::lock_guard<std::mutex> lock(buffer_mtx);
    if (buf_data_capacity == 0) {
      buf_data_capacity = DEFAULT_BUFSIZE;
      buf_data = (char*)malloc(sizeof(char*) * buf_data_capacity);
    }
    if ((pos + remaining + len) > buf_data_capacity) {
      if ((remaining + len) > buf_data_capacity) {
        buf_data_capacity += DEFAULT_BUFSIZE;
      }
      char* original_buf_data = buf_data;
      buf_data = (char*)malloc(sizeof(char*) * buf_data_capacity);
      memcpy(buf_data, original_buf_data + pos, remaining);
      free(original_buf_data);

      pos = 0;
      pos_dirty = 0;
    }
    memcpy(buf_data + pos + remaining, data, len);
    remaining += len;
    remaining_dirty += len;
    return 0; 
  }

  char* getDataAddr() {
    return buf_data;
  }

private:
  mutex buffer_mtx;
  char* buf_data;
  int buf_data_capacity;
  int pos;
  int remaining;
  int pos_dirty;
  int remaining_dirty;

  int min(int x, int y) {
    return x > y ? y : x;
  }
};
