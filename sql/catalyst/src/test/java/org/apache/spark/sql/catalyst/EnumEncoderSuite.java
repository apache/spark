package org.apache.spark.sql.catalyst;

import org.apache.spark.sql.Encoders;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by mike on 30-Jun-17.
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
    public void testEnum() {
        Encoders.bean(A.class);
    }
}
