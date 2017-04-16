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

#include "hadoop/Pipes.hh"
#include "hadoop/SerialUtils.hh"
#include "hadoop/StringUtils.hh"

#include <map>
#include <vector>

#include <errno.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <pthread.h>
#include <iostream>
#include <fstream>

#include <openssl/hmac.h>
#include <openssl/buffer.h>

using std::map;
using std::string;
using std::vector;

using namespace HadoopUtils;

namespace HadoopPipes {

  class JobConfImpl: public JobConf {
  private:
    map<string, string> values;
  public:
    void set(const string& key, const string& value) {
      values[key] = value;
    }

    virtual bool hasKey(const string& key) const {
      return values.find(key) != values.end();
    }

    virtual const string& get(const string& key) const {
      map<string,string>::const_iterator itr = values.find(key);
      if (itr == values.end()) {
        throw Error("Key " + key + " not found in JobConf");
      }
      return itr->second;
    }

    virtual int getInt(const string& key) const {
      const string& val = get(key);
      return toInt(val);
    }

    virtual float getFloat(const string& key) const {
      const string& val = get(key);
      return toFloat(val);
    }

    virtual bool getBoolean(const string&key) const {
      const string& val = get(key);
      return toBool(val);
    }
  };

  class DownwardProtocol {
  public:
    virtual void start(int protocol) = 0;
    virtual void setJobConf(vector<string> values) = 0;
    virtual void setInputTypes(string keyType, string valueType) = 0;
    virtual void runMap(string inputSplit, int numReduces, bool pipedInput)= 0;
    virtual void mapItem(const string& key, const string& value) = 0;
    virtual void runReduce(int reduce, bool pipedOutput) = 0;
    virtual void reduceKey(const string& key) = 0;
    virtual void reduceValue(const string& value) = 0;
    virtual void close() = 0;
    virtual void abort() = 0;
    virtual ~DownwardProtocol() {}
  };

  class UpwardProtocol {
  public:
    virtual void output(const string& key, const string& value) = 0;
    virtual void partitionedOutput(int reduce, const string& key,
                                   const string& value) = 0;
    virtual void status(const string& message) = 0;
    virtual void progress(float progress) = 0;
    virtual void done() = 0;
    virtual void registerCounter(int id, const string& group, 
                                 const string& name) = 0;
    virtual void 
      incrementCounter(const TaskContext::Counter* counter, uint64_t amount) = 0;
    virtual ~UpwardProtocol() {}
  };

  class Protocol {
  public:
    virtual void nextEvent() = 0;
    virtual UpwardProtocol* getUplink() = 0;
    virtual ~Protocol() {}
  };

  class TextUpwardProtocol: public UpwardProtocol {
  private:
    FILE* stream;
    static const char fieldSeparator = '\t';
    static const char lineSeparator = '\n';

    void writeBuffer(const string& buffer) {
      fprintf(stream, quoteString(buffer, "\t\n").c_str());
    }

  public:
    TextUpwardProtocol(FILE* _stream): stream(_stream) {}
    
    virtual void output(const string& key, const string& value) {
      fprintf(stream, "output%c", fieldSeparator);
      writeBuffer(key);
      fprintf(stream, "%c", fieldSeparator);
      writeBuffer(value);
      fprintf(stream, "%c", lineSeparator);
    }

    virtual void partitionedOutput(int reduce, const string& key,
                                   const string& value) {
      fprintf(stream, "parititionedOutput%c%d%c", fieldSeparator, reduce, 
              fieldSeparator);
      writeBuffer(key);
      fprintf(stream, "%c", fieldSeparator);
      writeBuffer(value);
      fprintf(stream, "%c", lineSeparator);
    }

    virtual void status(const string& message) {
      fprintf(stream, "status%c%s%c", fieldSeparator, message.c_str(), 
              lineSeparator);
    }

    virtual void progress(float progress) {
      fprintf(stream, "progress%c%f%c", fieldSeparator, progress, 
              lineSeparator);
    }

    virtual void registerCounter(int id, const string& group, 
                                 const string& name) {
      fprintf(stream, "registerCounter%c%d%c%s%c%s%c", fieldSeparator, id,
              fieldSeparator, group.c_str(), fieldSeparator, name.c_str(), 
              lineSeparator);
    }

    virtual void incrementCounter(const TaskContext::Counter* counter, 
                                  uint64_t amount) {
      fprintf(stream, "incrCounter%c%d%c%ld%c", fieldSeparator, counter->getId(), 
              fieldSeparator, (long)amount, lineSeparator);
    }
    
    virtual void done() {
      fprintf(stream, "done%c", lineSeparator);
    }
  };

  class TextProtocol: public Protocol {
  private:
    FILE* downStream;
    DownwardProtocol* handler;
    UpwardProtocol* uplink;
    string key;
    string value;

    int readUpto(string& buffer, const char* limit) {
      int ch;
      buffer.clear();
      while ((ch = getc(downStream)) != -1) {
        if (strchr(limit, ch) != NULL) {
          return ch;
        }
        buffer += ch;
      }
      return -1;
    }

    static const char* delim;
  public:

    TextProtocol(FILE* down, DownwardProtocol* _handler, FILE* up) {
      downStream = down;
      uplink = new TextUpwardProtocol(up);
      handler = _handler;
    }

    UpwardProtocol* getUplink() {
      return uplink;
    }

    virtual void nextEvent() {
      string command;
      string arg;
      int sep;
      sep = readUpto(command, delim);
      if (command == "mapItem") {
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(key, delim);
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(value, delim);
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->mapItem(key, value);
      } else if (command == "reduceValue") {
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(value, delim);
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->reduceValue(value);
      } else if (command == "reduceKey") {
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(key, delim);
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->reduceKey(key);
      } else if (command == "start") {
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(arg, delim);
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->start(toInt(arg));
      } else if (command == "setJobConf") {
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(arg, delim);
        int len = toInt(arg);
        vector<string> values(len);
        for(int i=0; i < len; ++i) {
          HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
          sep = readUpto(arg, delim);
          values.push_back(arg);
        }
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->setJobConf(values);
      } else if (command == "setInputTypes") {
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(key, delim);
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(value, delim);
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->setInputTypes(key, value);
      } else if (command == "runMap") {
        string split;
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(split, delim);
        string reduces;
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(reduces, delim);
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(arg, delim);
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->runMap(split, toInt(reduces), toBool(arg));
      } else if (command == "runReduce") {
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        sep = readUpto(arg, delim);
        HADOOP_ASSERT(sep == '\t', "Short text protocol command " + command);
        string piped;
        sep = readUpto(piped, delim);
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->runReduce(toInt(arg), toBool(piped));
      } else if (command == "abort") { 
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->abort();
      } else if (command == "close") {
        HADOOP_ASSERT(sep == '\n', "Long text protocol command " + command);
        handler->close();
      } else {
        throw Error("Illegal text protocol command " + command);
      }
    }

    ~TextProtocol() {
      delete uplink;
    }
  };
  const char* TextProtocol::delim = "\t\n";

  enum MESSAGE_TYPE {START_MESSAGE, SET_JOB_CONF, SET_INPUT_TYPES, RUN_MAP, 
                     MAP_ITEM, RUN_REDUCE, REDUCE_KEY, REDUCE_VALUE, 
                     CLOSE, ABORT, AUTHENTICATION_REQ,
                     OUTPUT=50, PARTITIONED_OUTPUT, STATUS, PROGRESS, DONE,
                     REGISTER_COUNTER, INCREMENT_COUNTER, AUTHENTICATION_RESP};

  class BinaryUpwardProtocol: public UpwardProtocol {
  private:
    FileOutStream* stream;
  public:
    BinaryUpwardProtocol(FILE* _stream) {
      stream = new FileOutStream();
      HADOOP_ASSERT(stream->open(_stream), "problem opening stream");
    }

    virtual void authenticate(const string &responseDigest) {
      serializeInt(AUTHENTICATION_RESP, *stream);
      serializeString(responseDigest, *stream);
      stream->flush();
    }

    virtual void output(const string& key, const string& value) {
      serializeInt(OUTPUT, *stream);
      serializeString(key, *stream);
      serializeString(value, *stream);
    }

    virtual void partitionedOutput(int reduce, const string& key,
                                   const string& value) {
      serializeInt(PARTITIONED_OUTPUT, *stream);
      serializeInt(reduce, *stream);
      serializeString(key, *stream);
      serializeString(value, *stream);
    }

    virtual void status(const string& message) {
      serializeInt(STATUS, *stream);
      serializeString(message, *stream);
    }

    virtual void progress(float progress) {
      serializeInt(PROGRESS, *stream);
      serializeFloat(progress, *stream);
      stream->flush();
    }

    virtual void done() {
      serializeInt(DONE, *stream);
    }

    virtual void registerCounter(int id, const string& group, 
                                 const string& name) {
      serializeInt(REGISTER_COUNTER, *stream);
      serializeInt(id, *stream);
      serializeString(group, *stream);
      serializeString(name, *stream);
    }

    virtual void incrementCounter(const TaskContext::Counter* counter, 
                                  uint64_t amount) {
      serializeInt(INCREMENT_COUNTER, *stream);
      serializeInt(counter->getId(), *stream);
      serializeLong(amount, *stream);
    }
    
    ~BinaryUpwardProtocol() {
      delete stream;
    }
  };

  class BinaryProtocol: public Protocol {
  private:
    FileInStream* downStream;
    DownwardProtocol* handler;
    BinaryUpwardProtocol * uplink;
    string key;
    string value;
    string password;
    bool authDone;
    void getPassword(string &password) {
      const char *passwordFile = getenv("hadoop.pipes.shared.secret.location");
      if (passwordFile == NULL) {
        return;
      }
      std::ifstream fstr(passwordFile, std::fstream::binary);
      if (fstr.fail()) {
        std::cerr << "Could not open the password file" << std::endl;
        return;
      } 
      unsigned char * passBuff = new unsigned char [512];
      fstr.read((char *)passBuff, 512);
      int passwordLength = fstr.gcount();
      fstr.close();
      passBuff[passwordLength] = 0;
      password.replace(0, passwordLength, (const char *) passBuff, passwordLength);
      delete [] passBuff;
      return; 
    }

    void verifyDigestAndRespond(string& digest, string& challenge) {
      if (password.empty()) {
        //password can be empty if process is running in debug mode from
        //command file.
        authDone = true;
        return;
      }

      if (!verifyDigest(password, digest, challenge)) {
        std::cerr << "Server failed to authenticate. Exiting" << std::endl;
        exit(-1);
      }
      authDone = true;
      string responseDigest = createDigest(password, digest);
      uplink->authenticate(responseDigest);
    }

    bool verifyDigest(string &password, string& digest, string& challenge) {
      string expectedDigest = createDigest(password, challenge);
      if (digest == expectedDigest) {
        return true;
      } else {
        return false;
      }
    }

    string createDigest(string &password, string& msg) {
      HMAC_CTX ctx;
      unsigned char digest[EVP_MAX_MD_SIZE];
      HMAC_Init(&ctx, (const unsigned char *)password.c_str(), 
          password.length(), EVP_sha1());
      HMAC_Update(&ctx, (const unsigned char *)msg.c_str(), msg.length());
      unsigned int digestLen;
      HMAC_Final(&ctx, digest, &digestLen);
      HMAC_cleanup(&ctx);

      //now apply base64 encoding
      BIO *bmem, *b64;
      BUF_MEM *bptr;

      b64 = BIO_new(BIO_f_base64());
      bmem = BIO_new(BIO_s_mem());
      b64 = BIO_push(b64, bmem);
      BIO_write(b64, digest, digestLen);
      BIO_flush(b64);
      BIO_get_mem_ptr(b64, &bptr);

      char digestBuffer[bptr->length];
      memcpy(digestBuffer, bptr->data, bptr->length-1);
      digestBuffer[bptr->length-1] = 0;
      BIO_free_all(b64);

      return string(digestBuffer);
    }

  public:
    BinaryProtocol(FILE* down, DownwardProtocol* _handler, FILE* up) {
      downStream = new FileInStream();
      downStream->open(down);
      uplink = new BinaryUpwardProtocol(up);
      handler = _handler;
      authDone = false;
      getPassword(password);
    }

    UpwardProtocol* getUplink() {
      return uplink;
    }

    virtual void nextEvent() {
      int32_t cmd;
      cmd = deserializeInt(*downStream);
      if (!authDone && cmd != AUTHENTICATION_REQ) {
        //Authentication request must be the first message if
        //authentication is not complete
        std::cerr << "Command:" << cmd << "received before authentication. " 
            << "Exiting.." << std::endl;
        exit(-1);
      }
      switch (cmd) {
      case AUTHENTICATION_REQ: {
        string digest;
        string challenge;
        deserializeString(digest, *downStream);
        deserializeString(challenge, *downStream);
        verifyDigestAndRespond(digest, challenge);
        break;
      }
      case START_MESSAGE: {
        int32_t prot;
        prot = deserializeInt(*downStream);
        handler->start(prot);
        break;
      }
      case SET_JOB_CONF: {
        int32_t entries;
        entries = deserializeInt(*downStream);
        vector<string> result(entries);
        for(int i=0; i < entries; ++i) {
          string item;
          deserializeString(item, *downStream);
          result.push_back(item);
        }
        handler->setJobConf(result);
        break;
      }
      case SET_INPUT_TYPES: {
        string keyType;
        string valueType;
        deserializeString(keyType, *downStream);
        deserializeString(valueType, *downStream);
        handler->setInputTypes(keyType, valueType);
        break;
      }
      case RUN_MAP: {
        string split;
        int32_t numReduces;
        int32_t piped;
        deserializeString(split, *downStream);
        numReduces = deserializeInt(*downStream);
        piped = deserializeInt(*downStream);
        handler->runMap(split, numReduces, piped);
        break;
      }
      case MAP_ITEM: {
        deserializeString(key, *downStream);
        deserializeString(value, *downStream);
        handler->mapItem(key, value);
        break;
      }
      case RUN_REDUCE: {
        int32_t reduce;
        int32_t piped;
        reduce = deserializeInt(*downStream);
        piped = deserializeInt(*downStream);
        handler->runReduce(reduce, piped);
        break;
      }
      case REDUCE_KEY: {
        deserializeString(key, *downStream);
        handler->reduceKey(key);
        break;
      }
      case REDUCE_VALUE: {
        deserializeString(value, *downStream);
        handler->reduceValue(value);
        break;
      }
      case CLOSE:
        handler->close();
        break;
      case ABORT:
        handler->abort();
        break;
      default:
        HADOOP_ASSERT(false, "Unknown binary command " + toString(cmd));
      }
    }

    virtual ~BinaryProtocol() {
      delete downStream;
      delete uplink;
    }
  };

  /**
   * Define a context object to give to combiners that will let them
   * go through the values and emit their results correctly.
   */
  class CombineContext: public ReduceContext {
  private:
    ReduceContext* baseContext;
    Partitioner* partitioner;
    int numReduces;
    UpwardProtocol* uplink;
    bool firstKey;
    bool firstValue;
    map<string, vector<string> >::iterator keyItr;
    map<string, vector<string> >::iterator endKeyItr;
    vector<string>::iterator valueItr;
    vector<string>::iterator endValueItr;

  public:
    CombineContext(ReduceContext* _baseContext,
                   Partitioner* _partitioner,
                   int _numReduces,
                   UpwardProtocol* _uplink,
                   map<string, vector<string> >& data) {
      baseContext = _baseContext;
      partitioner = _partitioner;
      numReduces = _numReduces;
      uplink = _uplink;
      keyItr = data.begin();
      endKeyItr = data.end();
      firstKey = true;
      firstValue = true;
    }

    virtual const JobConf* getJobConf() {
      return baseContext->getJobConf();
    }

    virtual const std::string& getInputKey() {
      return keyItr->first;
    }

    virtual const std::string& getInputValue() {
      return *valueItr;
    }

    virtual void emit(const std::string& key, const std::string& value) {
      if (partitioner != NULL) {
        uplink->partitionedOutput(partitioner->partition(key, numReduces),
                                  key, value);
      } else {
        uplink->output(key, value);
      }
    }

    virtual void progress() {
      baseContext->progress();
    }

    virtual void setStatus(const std::string& status) {
      baseContext->setStatus(status);
    }

    bool nextKey() {
      if (firstKey) {
        firstKey = false;
      } else {
        ++keyItr;
      }
      if (keyItr != endKeyItr) {
        valueItr = keyItr->second.begin();
        endValueItr = keyItr->second.end();
        firstValue = true;
        return true;
      }
      return false;
    }

    virtual bool nextValue() {
      if (firstValue) {
        firstValue = false;
      } else {
        ++valueItr;
      }
      return valueItr != endValueItr;
    }
    
    virtual Counter* getCounter(const std::string& group, 
                               const std::string& name) {
      return baseContext->getCounter(group, name);
    }

    virtual void incrementCounter(const Counter* counter, uint64_t amount) {
      baseContext->incrementCounter(counter, amount);
    }
  };

  /**
   * A RecordWriter that will take the map outputs, buffer them up and then
   * combine then when the buffer is full.
   */
  class CombineRunner: public RecordWriter {
  private:
    map<string, vector<string> > data;
    int64_t spillSize;
    int64_t numBytes;
    ReduceContext* baseContext;
    Partitioner* partitioner;
    int numReduces;
    UpwardProtocol* uplink;
    Reducer* combiner;
  public:
    CombineRunner(int64_t _spillSize, ReduceContext* _baseContext, 
                  Reducer* _combiner, UpwardProtocol* _uplink, 
                  Partitioner* _partitioner, int _numReduces) {
      numBytes = 0;
      spillSize = _spillSize;
      baseContext = _baseContext;
      partitioner = _partitioner;
      numReduces = _numReduces;
      uplink = _uplink;
      combiner = _combiner;
    }

    virtual void emit(const std::string& key,
                      const std::string& value) {
      numBytes += key.length() + value.length();
      data[key].push_back(value);
      if (numBytes >= spillSize) {
        spillAll();
      }
    }

    virtual void close() {
      spillAll();
    }

  private:
    void spillAll() {
      CombineContext context(baseContext, partitioner, numReduces, 
                             uplink, data);
      while (context.nextKey()) {
        combiner->reduce(context);
      }
      data.clear();
      numBytes = 0;
    }
  };

  class TaskContextImpl: public MapContext, public ReduceContext, 
                         public DownwardProtocol {
  private:
    bool done;
    JobConf* jobConf;
    string key;
    const string* newKey;
    const string* value;
    bool hasTask;
    bool isNewKey;
    bool isNewValue;
    string* inputKeyClass;
    string* inputValueClass;
    string status;
    float progressFloat;
    uint64_t lastProgress;
    bool statusSet;
    Protocol* protocol;
    UpwardProtocol *uplink;
    string* inputSplit;
    RecordReader* reader;
    Mapper* mapper;
    Reducer* reducer;
    RecordWriter* writer;
    Partitioner* partitioner;
    int numReduces;
    const Factory* factory;
    pthread_mutex_t mutexDone;
    std::vector<int> registeredCounterIds;

  public:

    TaskContextImpl(const Factory& _factory) {
      statusSet = false;
      done = false;
      newKey = NULL;
      factory = &_factory;
      jobConf = NULL;
      inputKeyClass = NULL;
      inputValueClass = NULL;
      inputSplit = NULL;
      mapper = NULL;
      reducer = NULL;
      reader = NULL;
      writer = NULL;
      partitioner = NULL;
      protocol = NULL;
      isNewKey = false;
      isNewValue = false;
      lastProgress = 0;
      progressFloat = 0.0f;
      hasTask = false;
      pthread_mutex_init(&mutexDone, NULL);
    }

    void setProtocol(Protocol* _protocol, UpwardProtocol* _uplink) {

      protocol = _protocol;
      uplink = _uplink;
    }

    virtual void start(int protocol) {
      if (protocol != 0) {
        throw Error("Protocol version " + toString(protocol) + 
                    " not supported");
      }
    }

    virtual void setJobConf(vector<string> values) {
      int len = values.size();
      JobConfImpl* result = new JobConfImpl();
      HADOOP_ASSERT(len % 2 == 0, "Odd length of job conf values");
      for(int i=0; i < len; i += 2) {
        result->set(values[i], values[i+1]);
      }
      jobConf = result;
    }

    virtual void setInputTypes(string keyType, string valueType) {
      inputKeyClass = new string(keyType);
      inputValueClass = new string(valueType);
    }

    virtual void runMap(string _inputSplit, int _numReduces, bool pipedInput) {
      inputSplit = new string(_inputSplit);
      reader = factory->createRecordReader(*this);
      HADOOP_ASSERT((reader == NULL) == pipedInput,
                    pipedInput ? "RecordReader defined when not needed.":
                    "RecordReader not defined");
      if (reader != NULL) {
        value = new string();
      }
      mapper = factory->createMapper(*this);
      numReduces = _numReduces;
      if (numReduces != 0) { 
        reducer = factory->createCombiner(*this);
        partitioner = factory->createPartitioner(*this);
      }
      if (reducer != NULL) {
        int64_t spillSize = 100;
        if (jobConf->hasKey("io.sort.mb")) {
          spillSize = jobConf->getInt("io.sort.mb");
        }
        writer = new CombineRunner(spillSize * 1024 * 1024, this, reducer, 
                                   uplink, partitioner, numReduces);
      }
      hasTask = true;
    }

    virtual void mapItem(const string& _key, const string& _value) {
      newKey = &_key;
      value = &_value;
      isNewKey = true;
    }

    virtual void runReduce(int reduce, bool pipedOutput) {
      reducer = factory->createReducer(*this);
      writer = factory->createRecordWriter(*this);
      HADOOP_ASSERT((writer == NULL) == pipedOutput,
                    pipedOutput ? "RecordWriter defined when not needed.":
                    "RecordWriter not defined");
      hasTask = true;
    }

    virtual void reduceKey(const string& _key) {
      isNewKey = true;
      newKey = &_key;
    }

    virtual void reduceValue(const string& _value) {
      isNewValue = true;
      value = &_value;
    }
    
    virtual bool isDone() {
      pthread_mutex_lock(&mutexDone);
      bool doneCopy = done;
      pthread_mutex_unlock(&mutexDone);
      return doneCopy;
    }

    virtual void close() {
      pthread_mutex_lock(&mutexDone);
      done = true;
      pthread_mutex_unlock(&mutexDone);
    }

    virtual void abort() {
      throw Error("Aborted by driver");
    }

    void waitForTask() {
      while (!done && !hasTask) {
        protocol->nextEvent();
      }
    }

    bool nextKey() {
      if (reader == NULL) {
        while (!isNewKey) {
          nextValue();
          if (done) {
            return false;
          }
        }
        key = *newKey;
      } else {
        if (!reader->next(key, const_cast<string&>(*value))) {
          pthread_mutex_lock(&mutexDone);
          done = true;
          pthread_mutex_unlock(&mutexDone);
          return false;
        }
        progressFloat = reader->getProgress();
      }
      isNewKey = false;
      if (mapper != NULL) {
        mapper->map(*this);
      } else {
        reducer->reduce(*this);
      }
      return true;
    }

    /**
     * Advance to the next value.
     */
    virtual bool nextValue() {
      if (isNewKey || done) {
        return false;
      }
      isNewValue = false;
      progress();
      protocol->nextEvent();
      return isNewValue;
    }

    /**
     * Get the JobConf for the current task.
     */
    virtual JobConf* getJobConf() {
      return jobConf;
    }

    /**
     * Get the current key. 
     * @return the current key or NULL if called before the first map or reduce
     */
    virtual const string& getInputKey() {
      return key;
    }

    /**
     * Get the current value. 
     * @return the current value or NULL if called before the first map or 
     *    reduce
     */
    virtual const string& getInputValue() {
      return *value;
    }

    /**
     * Mark your task as having made progress without changing the status 
     * message.
     */
    virtual void progress() {
      if (uplink != 0) {
        uint64_t now = getCurrentMillis();
        if (now - lastProgress > 1000) {
          lastProgress = now;
          if (statusSet) {
            uplink->status(status);
            statusSet = false;
          }
          uplink->progress(progressFloat);
        }
      }
    }

    /**
     * Set the status message and call progress.
     */
    virtual void setStatus(const string& status) {
      this->status = status;
      statusSet = true;
      progress();
    }

    /**
     * Get the name of the key class of the input to this task.
     */
    virtual const string& getInputKeyClass() {
      return *inputKeyClass;
    }

    /**
     * Get the name of the value class of the input to this task.
     */
    virtual const string& getInputValueClass() {
      return *inputValueClass;
    }

    /**
     * Access the InputSplit of the mapper.
     */
    virtual const std::string& getInputSplit() {
      return *inputSplit;
    }

    virtual void emit(const string& key, const string& value) {
      progress();
      if (writer != NULL) {
        writer->emit(key, value);
      } else if (partitioner != NULL) {
        int part = partitioner->partition(key, numReduces);
        uplink->partitionedOutput(part, key, value);
      } else {
        uplink->output(key, value);
      }
    }

    /**
     * Register a counter with the given group and name.
     */
    virtual Counter* getCounter(const std::string& group, 
                               const std::string& name) {
      int id = registeredCounterIds.size();
      registeredCounterIds.push_back(id);
      uplink->registerCounter(id, group, name);
      return new Counter(id);
    }

    /**
     * Increment the value of the counter with the given amount.
     */
    virtual void incrementCounter(const Counter* counter, uint64_t amount) {
      uplink->incrementCounter(counter, amount); 
    }

    void closeAll() {
      if (reader) {
        reader->close();
      }
      if (mapper) {
        mapper->close();
      }
      if (reducer) {
        reducer->close();
      }
      if (writer) {
        writer->close();
      }
    }

    virtual ~TaskContextImpl() {
      delete jobConf;
      delete inputKeyClass;
      delete inputValueClass;
      delete inputSplit;
      if (reader) {
        delete value;
      }
      delete reader;
      delete mapper;
      delete reducer;
      delete writer;
      delete partitioner;
      pthread_mutex_destroy(&mutexDone);
    }
  };

  /**
   * Ping the parent every 5 seconds to know if it is alive 
   */
  void* ping(void* ptr) {
    TaskContextImpl* context = (TaskContextImpl*) ptr;
    char* portStr = getenv("hadoop.pipes.command.port");
    int MAX_RETRIES = 3;
    int remaining_retries = MAX_RETRIES;
    while (!context->isDone()) {
      try{
        sleep(5);
        int sock = -1;
        if (portStr) {
          sock = socket(PF_INET, SOCK_STREAM, 0);
          HADOOP_ASSERT(sock != - 1,
                        string("problem creating socket: ") + strerror(errno));
          sockaddr_in addr;
          addr.sin_family = AF_INET;
          addr.sin_port = htons(toInt(portStr));
          addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
          HADOOP_ASSERT(connect(sock, (sockaddr*) &addr, sizeof(addr)) == 0,
                        string("problem connecting command socket: ") +
                        strerror(errno));

        }
        if (sock != -1) {
          int result = shutdown(sock, SHUT_RDWR);
          HADOOP_ASSERT(result == 0, "problem shutting socket");
          result = close(sock);
          HADOOP_ASSERT(result == 0, "problem closing socket");
        }
        remaining_retries = MAX_RETRIES;
      } catch (Error& err) {
        if (!context->isDone()) {
          fprintf(stderr, "Hadoop Pipes Exception: in ping %s\n", 
                err.getMessage().c_str());
          remaining_retries -= 1;
          if (remaining_retries == 0) {
            exit(1);
          }
        } else {
          return NULL;
        }
      }
    }
    return NULL;
  }

  /**
   * Run the assigned task in the framework.
   * The user's main function should set the various functions using the 
   * set* functions above and then call this.
   * @return true, if the task succeeded.
   */
  bool runTask(const Factory& factory) {
    try {
      TaskContextImpl* context = new TaskContextImpl(factory);
      Protocol* connection;
      char* portStr = getenv("hadoop.pipes.command.port");
      int sock = -1;
      FILE* stream = NULL;
      FILE* outStream = NULL;
      char *bufin = NULL;
      char *bufout = NULL;
      if (portStr) {
        sock = socket(PF_INET, SOCK_STREAM, 0);
        HADOOP_ASSERT(sock != - 1,
                      string("problem creating socket: ") + strerror(errno));
        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(toInt(portStr));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        HADOOP_ASSERT(connect(sock, (sockaddr*) &addr, sizeof(addr)) == 0,
                      string("problem connecting command socket: ") +
                      strerror(errno));

        stream = fdopen(sock, "r");
        outStream = fdopen(sock, "w");

        // increase buffer size
        int bufsize = 128*1024;
        int setbuf;
        bufin = new char[bufsize];
        bufout = new char[bufsize];
        setbuf = setvbuf(stream, bufin, _IOFBF, bufsize);
        HADOOP_ASSERT(setbuf == 0, string("problem with setvbuf for inStream: ")
                                     + strerror(errno));
        setbuf = setvbuf(outStream, bufout, _IOFBF, bufsize);
        HADOOP_ASSERT(setbuf == 0, string("problem with setvbuf for outStream: ")
                                     + strerror(errno));
        connection = new BinaryProtocol(stream, context, outStream);
      } else if (getenv("hadoop.pipes.command.file")) {
        char* filename = getenv("hadoop.pipes.command.file");
        string outFilename = filename;
        outFilename += ".out";
        stream = fopen(filename, "r");
        outStream = fopen(outFilename.c_str(), "w");
        connection = new BinaryProtocol(stream, context, outStream);
      } else {
        connection = new TextProtocol(stdin, context, stdout);
      }
      context->setProtocol(connection, connection->getUplink());
      pthread_t pingThread;
      pthread_create(&pingThread, NULL, ping, (void*)(context));
      context->waitForTask();
      while (!context->isDone()) {
        context->nextKey();
      }
      context->closeAll();
      connection->getUplink()->done();
      pthread_join(pingThread,NULL);
      delete context;
      delete connection;
      if (stream != NULL) {
        fflush(stream);
      }
      if (outStream != NULL) {
        fflush(outStream);
      }
      fflush(stdout);
      if (sock != -1) {
        int result = shutdown(sock, SHUT_RDWR);
        HADOOP_ASSERT(result == 0, "problem shutting socket");
        result = close(sock);
        HADOOP_ASSERT(result == 0, "problem closing socket");
      }
      if (stream != NULL) {
        //fclose(stream);
      }
      if (outStream != NULL) {
        //fclose(outStream);
      } 
      delete bufin;
      delete bufout;
      return true;
    } catch (Error& err) {
      fprintf(stderr, "Hadoop Pipes Exception: %s\n", 
              err.getMessage().c_str());
      return false;
    }
  }
}

