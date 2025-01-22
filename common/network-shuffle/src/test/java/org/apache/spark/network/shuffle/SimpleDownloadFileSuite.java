package org.apache.spark.network.shuffle;

import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Assertions;

public class SimpleDownloadFileSuite {
    @Test
    public void testChanenlIsClosedAfterCloseAndRead() throws IOException {
        File tempFile = File.createTempFile("testChannelIsClosed", ".tmp");
        tempFile.deleteOnExit();
        TransportConf conf = new TransportConf("test", MapConfigProvider.EMPTY);

        DownloadFile downloadFile = null;
        try {
            downloadFile = new SimpleDownloadFile(tempFile, conf);
            DownloadFileWritableChannel channel = downloadFile.openForWriting();
            channel.closeAndRead();
            Assertions.assertFalse(channel.isOpen(), "Channel should be closed after closeAndRead.");
        } finally {
            if (downloadFile != null) {
                downloadFile.delete();
            }
        }
    }
}
