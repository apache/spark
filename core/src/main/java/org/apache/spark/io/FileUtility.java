/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.io;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;

public class FileUtility {

  /**
   * Extract an input tar file into an output files and directories.
   * inputTarFileLoc:  the input file location for the tar file
   * destDirLoc: destination for the extracted files
   *
   * throws IllegalStateException
   */
  public static final String ENCODING = "utf-8";

  public static void extractTarFile(String inputTarFileLoc, String destDirLoc)
    throws IllegalStateException {
    File inputFile = new File(inputTarFileLoc);
    if (!inputTarFileLoc.endsWith(".tar")) {
      throw new IllegalStateException(String.format(
        "Input File[%s] should end with tar extension.", inputTarFileLoc));
    }
    File destDir = new File(destDirLoc);
    if (destDir.exists() && !destDir.delete()) {
      throw new IllegalStateException(String.format(
        "Couldn't delete the existing destination directory[%s] ", destDirLoc));
    } else if (!destDir.mkdir()) {
      throw new IllegalStateException(String.format(
        "Couldn't create directory  %s ", destDirLoc));
    }

    try (InputStream is = new FileInputStream(inputFile);
         TarArchiveInputStream debInputStream = new TarArchiveInputStream(is, ENCODING)) {
      TarArchiveEntry entry;
      while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
        final File outputFile = new File(destDirLoc, entry.getName());
        if (entry.isDirectory()) {
          if (!outputFile.exists() && !outputFile.mkdirs()) {
            throw new IllegalStateException(String.format(
              "Couldn't create directory %s.", outputFile.getAbsolutePath()));
          }
        } else {
          try (OutputStream outputFileStream = new FileOutputStream(outputFile)) {
            IOUtils.copy(debInputStream, outputFileStream);
          }
        }
      }
    } catch (IOException e){
      throw new IllegalStateException(String.format(
        "extractTarFile failed with exception %s.", e.getMessage()));
    }
  }

  /**
   * create a tar file for input source directory location .
   * source: the source directory location
   * destFileLoc: destination of the created tarball
   *
   * throws IllegalStateException
   */

  public static void createTarFile(String source, String destFileLoc)
    throws IllegalStateException {
    File f = new File(destFileLoc);
    if (f.exists() && !f.delete()) {
      throw new IllegalStateException(String.format(
        "Couldn't delete the destination file location[%s]", destFileLoc));
    }
    File folder = new File(source);
    if (!folder.exists()) {
      throw new IllegalStateException(String.format(
        "Source folder[%s] does not exist", source));
    }

    try (FileOutputStream fos = new FileOutputStream(destFileLoc);
         TarArchiveOutputStream tarOs = new TarArchiveOutputStream(fos, ENCODING)) {
      File[] fileNames = folder.listFiles();
      for (File file : fileNames) {
        TarArchiveEntry tar_file = new TarArchiveEntry(file.getName());
        tar_file.setSize(file.length());
        tarOs.putArchiveEntry(tar_file);
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
          IOUtils.copy(bis, tarOs);
          tarOs.closeArchiveEntry();
        }
      }
      tarOs.finish();
    } catch (IOException e) {
      throw new IllegalStateException(String.format(
        "createTarFile failed with exception %s.", e.getMessage()));
    }
  }

}
