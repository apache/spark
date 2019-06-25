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

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;

public class FileUtility {

    /**
     * Untar an input file into an output file.
     *
     * The output file is created in the output folder, having the same name as
     * the input file, minus the '.tar' extension.
     *
     * @param inputFile the input .tar file
     * @throws IOException
     *
     * @throws ArchiveException
     */
    public static void unTar(final File inputFile)
            throws IOException, ArchiveException {

        String outputDir = inputFile.getAbsolutePath().split(".tar")[0];
        File outputTarDir = new File(outputDir);
        outputTarDir.mkdir();
        final InputStream is = new FileInputStream(inputFile);
        final TarArchiveInputStream debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream(
                "tar", is);
        TarArchiveEntry entry = null;
        while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
            final File outputFile = new File(outputDir, entry.getName());
            if (entry.isDirectory()) {
                if (!outputFile.exists()) {
                    if (!outputFile.mkdirs()) {
                        throw new IllegalStateException(String.format(
                                "Couldn't create directory %s.", outputFile.getAbsolutePath()));
                    }
                }
            } else {
                final OutputStream outputFileStream = new FileOutputStream(outputFile);
                IOUtils.copy(debInputStream, outputFileStream);
                outputFileStream.close();
            }
        }
        debInputStream.close();
    }

    public static void createTarFile(String source, String destFileName) throws Exception {
        TarArchiveOutputStream tarOs = null;
        File f = new File(destFileName);
        if (f.exists()) {
            f.delete();
        }
        try {
            FileOutputStream fos = new FileOutputStream(destFileName);
            tarOs = (TarArchiveOutputStream) new ArchiveStreamFactory().createArchiveOutputStream("tar", fos);
            tarOs = new TarArchiveOutputStream(fos);
            File folder = new File(source);
            File[] fileNames = folder.listFiles();
            for(File file : fileNames){
                TarArchiveEntry tar_file = new TarArchiveEntry(file.getName());
                tar_file.setSize(file.length());
                tarOs.putArchiveEntry(tar_file);
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                IOUtils.copy(bis, tarOs);
                bis.close();
                tarOs.closeArchiveEntry();
            }
        } catch (IOException e) {
            throw new IllegalStateException(String.format(
                    "createTarFile failed with exception %s.", e.getMessage()));
        } finally {
            try {
                tarOs.finish();
                tarOs.close();
            } catch (IOException e) {
                throw new IllegalStateException(String.format(
                        "createTarFile failed with exception %s.", e.getMessage()));
            }
        }
    }


}
