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
#ifndef HADOOP_PIPES_TEMPLATE_FACTORY_HH
#define HADOOP_PIPES_TEMPLATE_FACTORY_HH

namespace HadoopPipes {

  template <class mapper, class reducer>
  class TemplateFactory2: public Factory {
  public:
    Mapper* createMapper(MapContext& context) const {
      return new mapper(context);
    }
    Reducer* createReducer(ReduceContext& context) const {
      return new reducer(context);
    }
  };

  template <class mapper, class reducer, class partitioner>
  class TemplateFactory3: public TemplateFactory2<mapper,reducer> {
  public:
    Partitioner* createPartitioner(MapContext& context) const {
      return new partitioner(context);
    }
  };

  template <class mapper, class reducer>
  class TemplateFactory3<mapper, reducer, void>
      : public TemplateFactory2<mapper,reducer> {
  };

  template <class mapper, class reducer, class partitioner, class combiner>
  class TemplateFactory4
   : public TemplateFactory3<mapper,reducer,partitioner>{
  public:
    Reducer* createCombiner(MapContext& context) const {
      return new combiner(context);
    }
  };

  template <class mapper, class reducer, class partitioner>
  class TemplateFactory4<mapper,reducer,partitioner,void>
   : public TemplateFactory3<mapper,reducer,partitioner>{
  };

  template <class mapper, class reducer, class partitioner, 
            class combiner, class recordReader>
  class TemplateFactory5
   : public TemplateFactory4<mapper,reducer,partitioner,combiner>{
  public:
    RecordReader* createRecordReader(MapContext& context) const {
      return new recordReader(context);
    }
  };

  template <class mapper, class reducer, class partitioner,class combiner>
  class TemplateFactory5<mapper,reducer,partitioner,combiner,void>
   : public TemplateFactory4<mapper,reducer,partitioner,combiner>{
  };

  template <class mapper, class reducer, class partitioner=void, 
            class combiner=void, class recordReader=void, 
            class recordWriter=void> 
  class TemplateFactory
   : public TemplateFactory5<mapper,reducer,partitioner,combiner,recordReader>{
  public:
    RecordWriter* createRecordWriter(ReduceContext& context) const {
      return new recordWriter(context);
    }
  };

  template <class mapper, class reducer, class partitioner, 
            class combiner, class recordReader>
  class TemplateFactory<mapper, reducer, partitioner, combiner, recordReader, 
                        void>
   : public TemplateFactory5<mapper,reducer,partitioner,combiner,recordReader>{
  };

}

#endif
