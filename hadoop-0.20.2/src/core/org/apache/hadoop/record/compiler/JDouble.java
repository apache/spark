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
public class JDouble extends JType {
  
  class JavaDouble extends JavaType {
    
    JavaDouble() {
      super("double", "Double", "Double", "TypeID.RIOType.DOUBLE");
    }
    
    String getTypeIDObjectString() {
      return "org.apache.hadoop.record.meta.TypeID.DoubleTypeID";
    }

    void genHashCode(CodeBuffer cb, String fname) {
      String tmp = "Double.doubleToLongBits("+fname+")";
      cb.append(Consts.RIO_PREFIX + "ret = (int)("+tmp+"^("+tmp+">>>32));\n");
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      cb.append("if ("+l+"<8) {\n");
      cb.append("throw new java.io.IOException(\"Double is exactly 8 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append(s+"+=8; "+l+"-=8;\n");
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      cb.append("if (l1<8 || l2<8) {\n");
      cb.append("throw new java.io.IOException(\"Double is exactly 8 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append("double d1 = org.apache.hadoop.record.Utils.readDouble(b1, s1);\n");
      cb.append("double d2 = org.apache.hadoop.record.Utils.readDouble(b2, s2);\n");
      cb.append("if (d1 != d2) {\n");
      cb.append("return ((d1-d2) < 0) ? -1 : 0;\n");
      cb.append("}\n");
      cb.append("s1+=8; s2+=8; l1-=8; l2-=8;\n");
      cb.append("}\n");
    }
  }

  class CppDouble extends CppType {
    
    CppDouble() {
      super("double");
    }
    
    String getTypeIDObjectString() {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_DOUBLE)";
    }
  }

  
  /** Creates a new instance of JDouble */
  public JDouble() {
    setJavaType(new JavaDouble());
    setCppType(new CppDouble());
    setCType(new CType());
  }
  
  String getSignature() {
    return "d";
  }
}
