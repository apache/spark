package org.apache.spark.sql.catalyst;

import org.apache.spark.sql.Encoders;
import org.junit.Test;

/**
 * Tests if it is possible to create an encoder for enum
 */
public class EnumEncoderSuite {
    public enum A {
        B("www.google.com");

        private String url;

        A(String url) {
            this.url = url;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }

    @Test
    public void testEnum() throws Exception {
        Encoders.bean(A.class);
    }
}
