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

package org.apache.hadoop.test.system;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Class to represent a control action which can be performed on Daemon.<br/>
 * 
 */

public abstract class ControlAction<T extends Writable> implements Writable {

  private T target;

  /**
   * Default constructor of the Control Action, sets the Action type to zero. <br/>
   */
  public ControlAction() {
  }

  /**
   * Constructor which sets the type of the Control action to a specific type. <br/>
   * 
   * @param type
   *          of the control action.
   */
  public ControlAction(T target) {
    this.target = target;
  }

  /**
   * Gets the id of the control action <br/>
   * 
   * @return target of action
   */
  public T getTarget() {
    return target;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    target.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    target.write(out);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ControlAction) {
      ControlAction<T> other = (ControlAction<T>) obj;
      return (this.target.equals(other.getTarget()));
    } else {
      return false;
    }
  }
  
  
  @Override
  public String toString() {
    return "Action Target : " + this.target;
  }
}
