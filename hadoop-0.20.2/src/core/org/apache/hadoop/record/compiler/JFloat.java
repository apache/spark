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
 */
public class JFloat extends JType {
  
  class JavaFloat extends JavaType {
    
    JavaFloat() {
      super("float", "Float", "Float", "TypeID.RIOType.FLOAT");
    }
    
    String getTypeIDObjectString() {
      return "org.apache.hadoop.record.meta.TypeID.FloatTypeID";
    }

    void genHashCode(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = Float.floatToIntBits("+fname+");\n");
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      cb.append("if ("+l+"<4) {\n");
      cb.append("throw new java.io.IOException(\"Float is exactly 4 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append(s+"+=4; "+l+"-=4;\n");
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      cb.append("if (l1<4 || l2<4) {\n");
      cb.append("throw new java.io.IOException(\"Float is exactly 4 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append("float f1 = org.apache.hadoop.record.Utils.readFloat(b1, s1);\n");
      cb.append("float f2 = org.apache.hadoop.record.Utils.readFloat(b2, s2);\n");
      cb.append("if (f1 != f2) {\n");
      cb.append("return ((f1-f2) < 0) ? -1 : 0;\n");
      cb.append("}\n");
      cb.append("s1+=4; s2+=4; l1-=4; l2-=4;\n");
      cb.append("}\n");
    }
  }

  class CppFloat extends CppType {
    
    CppFloat() {
      super("float");
    }
    
    String getTypeIDObjectString() {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_FLOAT)";
    }
  }

  /** Creates a new instance of JFloat */
  public JFloat() {
    setJavaType(new JavaFloat());
    setCppType(new CppFloat());
    setCType(new CType());
  }
  
  String getSignature() {
    return "f";
  }
}
