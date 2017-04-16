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

package org.apache.hadoop.record.compiler;


/**
 * Abstract base class for all the "compound" types such as ustring,
 * buffer, vector, map, and record.
 */
abstract class JCompType extends JType {
  
  abstract class JavaCompType extends JavaType {
    
    JavaCompType(String type, String suffix, String wrapper, 
        String typeIDByteString) { 
      super(type, suffix, wrapper, typeIDByteString);
    }
    
    void genCompareTo(CodeBuffer cb, String fname, String other) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".compareTo("+other+");\n");
    }
    
    void genEquals(CodeBuffer cb, String fname, String peer) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".equals("+peer+");\n");
    }
    
    void genHashCode(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".hashCode();\n");
    }
    
    void genClone(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = ("+getType()+") this."+
          fname+".clone();\n");
    }
  }
  
  abstract class CppCompType extends CppType {
    
    CppCompType(String type) {
      super(type);
    }
    
    void genGetSet(CodeBuffer cb, String fname) {
      cb.append("virtual const "+getType()+"& get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual "+getType()+"& get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
    }
  }
  
  class CCompType extends CType {
    
  }
}
