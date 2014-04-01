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

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IProjectNature;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.NullProgressMonitor;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.QualifiedName;
import org.eclipse.jdt.core.IClasspathEntry;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jdt.core.JavaCore;

/**
 * Class to configure and deconfigure an Eclipse project with the MapReduce
 * project nature.
 */

public class MapReduceNature implements IProjectNature {

  public static final String ID = "org.apache.hadoop.eclipse.Nature";

  private IProject project;

  static Logger log = Logger.getLogger(MapReduceNature.class.getName());

  /**
   * Configures an Eclipse project as a Map/Reduce project by adding the
   * Hadoop libraries to a project's classpath.
   */
  public void configure() throws CoreException {
    String path =
        project.getPersistentProperty(new QualifiedName(Activator.PLUGIN_ID,
            "hadoop.runtime.path"));

    File dir = new File(path);
    final ArrayList<File> coreJars = new ArrayList<File>();
    dir.listFiles(new FileFilter() {
      public boolean accept(File pathname) {
        String fileName = pathname.getName();

        // get the hadoop core jar without touching test or examples
        // older version of hadoop don't use the word "core" -- eyhung
        if ((fileName.indexOf("hadoop") != -1) && (fileName.endsWith("jar"))
            && (fileName.indexOf("test") == -1)
            && (fileName.indexOf("examples") == -1)) {
          coreJars.add(pathname);
        }

        return false; // we don't care what this returns
      }
    });
    File dir2 = new File(path + File.separatorChar + "lib");
    if (dir2.exists() && dir2.isDirectory()) {
      dir2.listFiles(new FileFilter() {
        public boolean accept(File pathname) {
          if ((!pathname.isDirectory())
              && (pathname.getName().endsWith("jar"))) {
            coreJars.add(pathname);
          }

          return false; // we don't care what this returns
        }
      });
    }

    // Add Hadoop libraries onto classpath
    IJavaProject javaProject = JavaCore.create(getProject());
    // Bundle bundle = Activator.getDefault().getBundle();
    try {
      IClasspathEntry[] currentCp = javaProject.getRawClasspath();
      IClasspathEntry[] newCp =
          new IClasspathEntry[currentCp.length + coreJars.size()];
      System.arraycopy(currentCp, 0, newCp, 0, currentCp.length);

      final Iterator<File> i = coreJars.iterator();
      int count = 0;
      while (i.hasNext()) {
        // for (int i = 0; i < s_coreJarNames.length; i++) {

        final File f = (File) i.next();
        // URL url = FileLocator.toFileURL(FileLocator.find(bundle, new
        // Path("lib/" + s_coreJarNames[i]), null));
        URL url = f.toURI().toURL();
        log.finer("hadoop library url.getPath() = " + url.getPath());

        newCp[newCp.length - 1 - count] =
            JavaCore.newLibraryEntry(new Path(url.getPath()), null, null);
        count++;
      }

      javaProject.setRawClasspath(newCp, new NullProgressMonitor());
    } catch (Exception e) {
      log.log(Level.SEVERE, "IOException generated in "
          + this.getClass().getCanonicalName(), e);
    }
  }

  /**
   * Deconfigure a project from MapReduce status. Currently unimplemented.
   */
  public void deconfigure() throws CoreException {
    // TODO Auto-generated method stub
  }

  /**
   * Returns the project to which this project nature applies.
   */
  public IProject getProject() {
    return this.project;
  }

  /**
   * Sets the project to which this nature applies. Used when instantiating
   * this project nature runtime.
   */
  public void setProject(IProject project) {
    this.project = project;
  }

}
