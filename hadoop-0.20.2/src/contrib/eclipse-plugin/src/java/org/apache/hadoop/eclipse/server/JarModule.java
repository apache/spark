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

package org.apache.hadoop.eclipse.server;

import java.io.File;
import java.util.logging.Logger;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.ErrorMessageDialog;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.Path;
import org.eclipse.jdt.core.ICompilationUnit;
import org.eclipse.jdt.core.IJavaElement;
import org.eclipse.jdt.core.IType;
import org.eclipse.jdt.ui.jarpackager.IJarExportRunnable;
import org.eclipse.jdt.ui.jarpackager.JarPackageData;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.PlatformUI;

/**
 * Methods for interacting with the jar file containing the
 * Mapper/Reducer/Driver classes for a MapReduce job.
 */

public class JarModule implements IRunnableWithProgress {

  static Logger log = Logger.getLogger(JarModule.class.getName());

  private IResource resource;

  private File jarFile;

  public JarModule(IResource resource) {
    this.resource = resource;
  }

  public String getName() {
    return resource.getProject().getName() + "/" + resource.getName();
  }

  /**
   * Creates a JAR file containing the given resource (Java class with
   * main()) and all associated resources
   * 
   * @param resource the resource
   * @return a file designing the created package
   */
  public void run(IProgressMonitor monitor) {

    log.fine("Build jar");
    JarPackageData jarrer = new JarPackageData();

    jarrer.setExportJavaFiles(true);
    jarrer.setExportClassFiles(true);
    jarrer.setExportOutputFolders(true);
    jarrer.setOverwrite(true);

    try {
      // IJavaProject project =
      // (IJavaProject) resource.getProject().getNature(JavaCore.NATURE_ID);

      // check this is the case before letting this method get called
      Object element = resource.getAdapter(IJavaElement.class);
      IType type = ((ICompilationUnit) element).findPrimaryType();
      jarrer.setManifestMainClass(type);

      // Create a temporary JAR file name
      File baseDir = Activator.getDefault().getStateLocation().toFile();

      String prefix =
          String.format("%s_%s-", resource.getProject().getName(), resource
              .getName());
      File jarFile = File.createTempFile(prefix, ".jar", baseDir);
      jarrer.setJarLocation(new Path(jarFile.getAbsolutePath()));

      jarrer.setElements(resource.getProject().members(IResource.FILE));
      IJarExportRunnable runnable =
          jarrer.createJarExportRunnable(Display.getDefault()
              .getActiveShell());
      runnable.run(monitor);

      this.jarFile = jarFile;

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Allow the retrieval of the resulting JAR file
   * 
   * @return the generated JAR file
   */
  public File getJarFile() {
    return this.jarFile;
  }

  /**
   * Static way to create a JAR package for the given resource and showing a
   * progress bar
   * 
   * @param resource
   * @return
   */
  public static File createJarPackage(IResource resource) {

    JarModule jarModule = new JarModule(resource);
    try {
      PlatformUI.getWorkbench().getProgressService().run(false, true,
          jarModule);

    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    File jarFile = jarModule.getJarFile();
    if (jarFile == null) {
      ErrorMessageDialog.display("Run on Hadoop",
          "Unable to create or locate the JAR file for the Job");
      return null;
    }

    return jarFile;
  }

}
