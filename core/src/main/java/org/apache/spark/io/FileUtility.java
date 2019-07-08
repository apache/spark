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
   * throws IllegalStateException on error
   */
  public static final String ENCODING = "utf-8";

  public static void extractTarFile(String inputTarFileLoc, String destDirLoc)
          throws IOException, IllegalStateException {
    File inputFile = new File(inputTarFileLoc);
    if (!inputTarFileLoc.endsWith("tar")) {
      throw new IllegalStateException(String.format(
              "Input File %s should end with tar extension.", inputTarFileLoc));
    }
    File destDir = new File(destDirLoc);
    if (destDir.exists() && !destDir.delete()) {
      throw new IllegalStateException(String.format(
              "Couldn't delete the existing destination directory  %s ", destDirLoc));
    } else if (!destDir.mkdir()) {
      throw new IllegalStateException(String.format(
              "Couldn't create directory  %s ", destDirLoc));
    }

    final InputStream is = new FileInputStream(inputFile);
    final TarArchiveInputStream debInputStream = new TarArchiveInputStream(is, ENCODING);
    OutputStream outputFileStream = null;
    try {
      TarArchiveEntry entry;
      while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
        final File outputFile = new File(destDirLoc, entry.getName());
        if (entry.isDirectory()) {
          if (!outputFile.exists() && !outputFile.mkdirs()) {
            throw new IllegalStateException(String.format(
                    "Couldn't create directory %s.", outputFile.getAbsolutePath()));
          }
        } else {
          outputFileStream = new FileOutputStream(outputFile);
          IOUtils.copy(debInputStream, outputFileStream);
          outputFileStream.close();
          outputFileStream = null;
        }
      }
    } catch (IOException e){
      throw new IllegalStateException(String.format(
              "extractTarFile failed with exception %s.", e.getMessage()));
    } finally {
      debInputStream.close();
      if (outputFileStream != null) {
        outputFileStream.close();
      }
    }
  }

  /**
   * create a tar file for input source directory location .
   * throws IOException on error
   */
  public static void createTarFile(String source, String destFileLoc)
          throws IllegalStateException, IOException {
    TarArchiveOutputStream tarOs = null;
    File f = new File(destFileLoc);
    if (f.exists() && !f.delete()) {
      throw new IllegalStateException(String.format(
              "Couldn't delete the destination file location %s", destFileLoc));
    }
    BufferedInputStream bis = null;
    try {
      FileOutputStream fos = new FileOutputStream(destFileLoc);
      tarOs = new TarArchiveOutputStream(fos, ENCODING);
      File folder = new File(source);
      File[] fileNames = folder.listFiles();
      for(File file : fileNames){
        TarArchiveEntry tar_file = new TarArchiveEntry(file.getName());
        tar_file.setSize(file.length());
        tarOs.putArchiveEntry(tar_file);
        bis = new BufferedInputStream(new FileInputStream(file));
        IOUtils.copy(bis, tarOs);
        bis.close();
        bis = null;
        tarOs.closeArchiveEntry();
      }
    } catch (IOException e) {
      throw new IllegalStateException(String.format(
              "createTarFile failed with exception %s.", e.getMessage()));
    } finally {
      tarOs.finish();
      tarOs.close();
      if (bis != null) {
        bis.close();
      }
    }
  }

}
