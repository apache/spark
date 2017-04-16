/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * @author: Sriram Rao (Kosmix Corp.)
 * 
 * Unit tests for testing the KosmosFileSystem API implementation.
 */

package org.apache.hadoop.fs.kfs;

import java.io.*;
import java.net.*;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.kfs.KosmosFileSystem;

public class TestKosmosFileSystem extends TestCase {

    KosmosFileSystem kosmosFileSystem;
    KFSEmulationImpl kfsEmul;
    Path baseDir;
    
    @Override
    protected void setUp() throws IOException {
        Configuration conf = new Configuration();
    
        kfsEmul = new KFSEmulationImpl(conf);
        kosmosFileSystem = new KosmosFileSystem(kfsEmul);
        // a dummy URI; we are not connecting to any setup here
        kosmosFileSystem.initialize(URI.create("kfs:///"), conf);
        baseDir = new Path(System.getProperty("test.build.data", "/tmp" ) +
                                              "/kfs-test");
    }

    @Override
    protected void tearDown() throws Exception {

    }

    // @Test
    // Check all the directory API's in KFS
    public void testDirs() throws Exception {
        Path subDir1 = new Path("dir.1");

        // make the dir
        kosmosFileSystem.mkdirs(baseDir);
        assertTrue(kosmosFileSystem.isDirectory(baseDir));
        kosmosFileSystem.setWorkingDirectory(baseDir);

        kosmosFileSystem.mkdirs(subDir1);
        assertTrue(kosmosFileSystem.isDirectory(subDir1));

        assertFalse(kosmosFileSystem.exists(new Path("test1")));
        assertFalse(kosmosFileSystem.isDirectory(new Path("test/dir.2")));

        FileStatus[] p = kosmosFileSystem.listStatus(baseDir);
        assertEquals(p.length, 1);

        kosmosFileSystem.delete(baseDir, true);
        assertFalse(kosmosFileSystem.exists(baseDir));
    }

    // @Test
    // Check the file API's
    public void testFiles() throws Exception {
        Path subDir1 = new Path("dir.1");
        Path file1 = new Path("dir.1/foo.1");
        Path file2 = new Path("dir.1/foo.2");

        kosmosFileSystem.mkdirs(baseDir);
        assertTrue(kosmosFileSystem.isDirectory(baseDir));
        kosmosFileSystem.setWorkingDirectory(baseDir);

        kosmosFileSystem.mkdirs(subDir1);

        FSDataOutputStream s1 = kosmosFileSystem.create(file1, true, 4096, (short) 1, (long) 4096, null);
        FSDataOutputStream s2 = kosmosFileSystem.create(file2, true, 4096, (short) 1, (long) 4096, null);

        s1.close();
        s2.close();

        FileStatus[] p = kosmosFileSystem.listStatus(subDir1);
        assertEquals(p.length, 2);

        kosmosFileSystem.delete(file1, true);
        p = kosmosFileSystem.listStatus(subDir1);
        assertEquals(p.length, 1);

        kosmosFileSystem.delete(file2, true);
        p = kosmosFileSystem.listStatus(subDir1);
        assertEquals(p.length, 0);

        kosmosFileSystem.delete(baseDir, true);
        assertFalse(kosmosFileSystem.exists(baseDir));
    }

    // @Test
    // Check file/read write
    public void testFileIO() throws Exception {
        Path subDir1 = new Path("dir.1");
        Path file1 = new Path("dir.1/foo.1");

        kosmosFileSystem.mkdirs(baseDir);
        assertTrue(kosmosFileSystem.isDirectory(baseDir));
        kosmosFileSystem.setWorkingDirectory(baseDir);

        kosmosFileSystem.mkdirs(subDir1);

        FSDataOutputStream s1 = kosmosFileSystem.create(file1, true, 4096, (short) 1, (long) 4096, null);

        int bufsz = 4096;
        byte[] data = new byte[bufsz];

        for (int i = 0; i < data.length; i++)
            data[i] = (byte) (i % 16);

        // write 4 bytes and read them back; read API should return a byte per call
        s1.write(32);
        s1.write(32);
        s1.write(32);
        s1.write(32);
        // write some data
        s1.write(data, 0, data.length);
        // flush out the changes
        s1.close();

        // Read the stuff back and verify it is correct
        FSDataInputStream s2 = kosmosFileSystem.open(file1, 4096);
        int v;

        v = s2.read();
        assertEquals(v, 32);
        v = s2.read();
        assertEquals(v, 32);
        v = s2.read();
        assertEquals(v, 32);
        v = s2.read();
        assertEquals(v, 32);

        assertEquals(s2.available(), data.length);

        byte[] buf = new byte[bufsz];
        s2.read(buf, 0, buf.length);
        for (int i = 0; i < data.length; i++)
            assertEquals(data[i], buf[i]);

        assertEquals(s2.available(), 0);

        s2.close();

        kosmosFileSystem.delete(file1, true);
        assertFalse(kosmosFileSystem.exists(file1));        
        kosmosFileSystem.delete(subDir1, true);
        assertFalse(kosmosFileSystem.exists(subDir1));        
        kosmosFileSystem.delete(baseDir, true);
        assertFalse(kosmosFileSystem.exists(baseDir));        
    }
    
}
