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

package org.apache.hadoop.eclipse;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.widgets.Display;

/**
 * Error dialog helper
 */
public class ErrorMessageDialog {

  public static void display(final String title, final String message) {
    Display.getDefault().syncExec(new Runnable() {

      public void run() {
        MessageDialog.openError(Display.getDefault().getActiveShell(),
            title, message);
      }

    });
  }

  public static void display(Exception e) {
    display("An exception has occured!", "Exception description:\n"
        + e.getLocalizedMessage());
  }

}
