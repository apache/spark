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

package org.apache.hadoop.eclipse.servers;

import org.apache.hadoop.eclipse.server.HadoopServer;
import org.eclipse.jface.viewers.IContentProvider;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.graphics.Image;

/**
 * Provider that enables selection of a predefined Hadoop server.
 */

public class HadoopServerSelectionListContentProvider implements
    IContentProvider, ITableLabelProvider, IStructuredContentProvider {
  public void dispose() {

  }

  public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {

  }

  public Image getColumnImage(Object element, int columnIndex) {
    return null;
  }

  public String getColumnText(Object element, int columnIndex) {
    if (element instanceof HadoopServer) {
      HadoopServer location = (HadoopServer) element;
      if (columnIndex == 0) {
        return location.getLocationName();

      } else if (columnIndex == 1) {
        return location.getMasterHostName();
      }
    }

    return element.toString();
  }

  public void addListener(ILabelProviderListener listener) {

  }

  public boolean isLabelProperty(Object element, String property) {
    return false;
  }

  public void removeListener(ILabelProviderListener listener) {

  }

  public Object[] getElements(Object inputElement) {
    return ServerRegistry.getInstance().getServers().toArray();
  }
}
