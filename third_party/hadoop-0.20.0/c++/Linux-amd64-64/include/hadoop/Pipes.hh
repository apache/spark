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
#ifndef HADOOP_PIPES_HH
#define HADOOP_PIPES_HH

#ifdef SWIG
%module (directors="1") HadoopPipes
%include "std_string.i"
%feature("director") Mapper;
%feature("director") Reducer;
%feature("director") Partitioner;
%feature("director") RecordReader;
%feature("director") RecordWriter;
%feature("director") Factory;
#else
#include <string>
#endif

namespace HadoopPipes {

/**
 * This interface defines the interface between application code and the 
 * foreign code interface to Hadoop Map/Reduce.
 */

/**
 * A JobConf defines the properties for a job.
 */
class JobConf {
public:
  virtual bool hasKey(const std::string& key) const = 0;
  virtual const std::string& get(const std::string& key) const = 0;
  virtual int getInt(const std::string& key) const = 0;
  virtual float getFloat(const std::string& key) const = 0;
  virtual bool getBoolean(const std::string&key) const = 0;
  virtual ~JobConf() {}
};

/**
 * Task context provides the information about the task and job.
 */
class TaskContext {
public:
  /**
   * Counter to keep track of a property and its value.
   */
  class Counter {
  private:
    int id;
  public:
    Counter(int counterId) : id(counterId) {}
    Counter(const Counter& counter) : id(counter.id) {}

    int getId() const { return id; }
  };
  
  /**
   * Get the JobConf for the current task.
   */
  virtual const JobConf* getJobConf() = 0;

  /**
   * Get the current key. 
   * @return the current key
   */
  virtual const std::string& getInputKey() = 0;

  /**
   * Get the current value. 
   * @return the current value
   */
  virtual const std::string& getInputValue() = 0;

  /**
   * Generate an output record
   */
  virtual void emit(const std::string& key, const std::string& value) = 0;

  /**
   * Mark your task as having made progress without changing the status 
   * message.
   */
  virtual void progress() = 0;

  /**
   * Set the status message and call progress.
   */
  virtual void setStatus(const std::string& status) = 0;

  /**
   * Register a counter with the given group and name.
   */
  virtual Counter* 
    getCounter(const std::string& group, const std::string& name) = 0;

  /**
   * Increment the value of the counter with the given amount.
   */
  virtual void incrementCounter(const Counter* counter, uint64_t amount) = 0;
  
  virtual ~TaskContext() {}
};

class MapContext: public TaskContext {
public:

  /**
   * Access the InputSplit of the mapper.
   */
  virtual const std::string& getInputSplit() = 0;

  /**
   * Get the name of the key class of the input to this task.
   */
  virtual const std::string& getInputKeyClass() = 0;

  /**
   * Get the name of the value class of the input to this task.
   */
  virtual const std::string& getInputValueClass() = 0;

};

class ReduceContext: public TaskContext {
public:
  /**
   * Advance to the next value.
   */
  virtual bool nextValue() = 0;
};

class Closable {
public:
  virtual void close() {}
  virtual ~Closable() {}
};

/**
 * The application's mapper class to do map.
 */
class Mapper: public Closable {
public:
  virtual void map(MapContext& context) = 0;
};

/**
 * The application's reducer class to do reduce.
 */
class Reducer: public Closable {
public:
  virtual void reduce(ReduceContext& context) = 0;
};

/**
 * User code to decide where each key should be sent.
 */
class Partitioner {
public:
  virtual int partition(const std::string& key, int numOfReduces) = 0;
  virtual ~Partitioner() {}
};

/**
 * For applications that want to read the input directly for the map function
 * they can define RecordReaders in C++.
 */
class RecordReader: public Closable {
public:
  virtual bool next(std::string& key, std::string& value) = 0;

  /**
   * The progress of the record reader through the split as a value between
   * 0.0 and 1.0.
   */
  virtual float getProgress() = 0;
};

/**
 * An object to write key/value pairs as they are emited from the reduce.
 */
class RecordWriter: public Closable {
public:
  virtual void emit(const std::string& key,
                    const std::string& value) = 0;
};

/**
 * A factory to create the necessary application objects.
 */
class Factory {
public:
  virtual Mapper* createMapper(MapContext& context) const = 0;
  virtual Reducer* createReducer(ReduceContext& context) const = 0;

  /**
   * Create a combiner, if this application has one.
   * @return the new combiner or NULL, if one is not needed
   */
  virtual Reducer* createCombiner(MapContext& context) const {
    return NULL; 
  }

  /**
   * Create an application partitioner object.
   * @return the new partitioner or NULL, if the default partitioner should be 
   *     used.
   */
  virtual Partitioner* createPartitioner(MapContext& context) const {
    return NULL;
  }

  /**
   * Create an application record reader.
   * @return the new RecordReader or NULL, if the Java RecordReader should be
   *    used.
   */
  virtual RecordReader* createRecordReader(MapContext& context) const {
    return NULL; 
  }

  /**
   * Create an application record writer.
   * @return the new RecordWriter or NULL, if the Java RecordWriter should be
   *    used.
   */
  virtual RecordWriter* createRecordWriter(ReduceContext& context) const {
    return NULL;
  }

  virtual ~Factory() {}
};

/**
 * Run the assigned task in the framework.
 * The user's main function should set the various functions using the 
 * set* functions above and then call this.
 * @return true, if the task succeeded.
 */
bool runTask(const Factory& factory);

}

#endif
