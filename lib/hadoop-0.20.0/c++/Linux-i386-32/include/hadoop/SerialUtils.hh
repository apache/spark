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
#ifndef HADOOP_SERIAL_UTILS_HH
#define HADOOP_SERIAL_UTILS_HH

#include <string>

namespace HadoopUtils {

  /**
   * A simple exception class that records a message for the user.
   */
  class Error {
  private:
    std::string error;
  public:

    /**
     * Create an error object with the given message.
     */
    Error(const std::string& msg);

    /**
     * Construct an error object with the given message that was created on
     * the given file, line, and functino.
     */
    Error(const std::string& msg, 
          const std::string& file, int line, const std::string& function);

    /**
     * Get the error message.
     */
    const std::string& getMessage() const;
  };

  /**
   * Check to make sure that the condition is true, and throw an exception
   * if it is not. The exception will contain the message and a description
   * of the source location.
   */
  #define HADOOP_ASSERT(CONDITION, MESSAGE) \
    { \
      if (!(CONDITION)) { \
        throw HadoopUtils::Error((MESSAGE), __FILE__, __LINE__, \
                                    __PRETTY_FUNCTION__); \
      } \
    }

  /**
   * An interface for an input stream.
   */
  class InStream {
  public:
    /**
     * Reads len bytes from the stream into the buffer.
     * @param buf the buffer to read into
     * @param buflen the length of the buffer
     * @throws Error if there are problems reading
     */
    virtual void read(void *buf, size_t len) = 0;
    virtual ~InStream() {}
  };

  /**
   * An interface for an output stream.
   */
  class OutStream {
  public:
    /**
     * Write the given buffer to the stream.
     * @param buf the data to write
     * @param len the number of bytes to write
     * @throws Error if there are problems writing
     */
    virtual void write(const void *buf, size_t len) = 0;
    /**
     * Flush the data to the underlying store.
     */
    virtual void flush() = 0;
    virtual ~OutStream() {}
  };

  /**
   * A class to read a file as a stream.
   */
  class FileInStream : public InStream {
  public:
    FileInStream();
    bool open(const std::string& name);
    bool open(FILE* file);
    void read(void *buf, size_t buflen);
    bool skip(size_t nbytes);
    bool close();
    virtual ~FileInStream();
  private:
    /**
     * The file to write to.
     */
    FILE *mFile;
    /**
     * Does is this class responsible for closing the FILE*?
     */
    bool isOwned;
  };

  /**
   * A class to write a stream to a file.
   */
  class FileOutStream: public OutStream {
  public:

    /**
     * Create a stream that isn't bound to anything.
     */
    FileOutStream();

    /**
     * Create the given file, potentially overwriting an existing file.
     */
    bool open(const std::string& name, bool overwrite);
    bool open(FILE* file);
    void write(const void* buf, size_t len);
    bool advance(size_t nbytes);
    void flush();
    bool close();
    virtual ~FileOutStream();
  private:
    FILE *mFile;
    bool isOwned;
  };

  /**
   * A stream that reads from a string.
   */
  class StringInStream: public InStream {
  public:
    StringInStream(const std::string& str);
    virtual void read(void *buf, size_t buflen);
  private:
    const std::string& buffer;
    std::string::const_iterator itr;
  };

  void serializeInt(int32_t t, OutStream& stream);
  int32_t deserializeInt(InStream& stream);
  void serializeLong(int64_t t, OutStream& stream);
  int64_t deserializeLong(InStream& stream);
  void serializeFloat(float t, OutStream& stream);
  float deserializeFloat(InStream& stream);
  void serializeString(const std::string& t, OutStream& stream);
  void deserializeString(std::string& t, InStream& stream);
}

#endif
