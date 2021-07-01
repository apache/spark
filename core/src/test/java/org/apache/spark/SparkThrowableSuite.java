package org.apache.spark;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.SortedMap;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SparkThrowable.SparkThrowableHelper;

public class SparkThrowableSuite implements Serializable {

    @Test
    public void testNoDuplicateErrorClasses() {
        // Enabling this feature incurs performance overhead (20-30%)
        ObjectMapper mapper = JsonMapper.builder()
                .enable(Feature.STRICT_DUPLICATE_DETECTION)
                .build();
        try {
            mapper.readValue(SparkThrowableHelper.errorClassesUrl,
                    new TypeReference<SortedMap<String, ErrorInfo>>(){});
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Test
    public void testErrorClassesCorrectlyFormatted() {
        ObjectMapper mapper = JsonMapper.builder()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .build()
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        try {
            InputStream stream = SparkThrowableHelper.errorClassesUrl.openStream();
            String originalFileContents = IOUtils.toString(stream, Charset.defaultCharset());
            String rewrittenFileContents = mapper.writeValueAsString(
                    SparkThrowableHelper.errorClassToInfoMap);
            assert originalFileContents.equals(rewrittenFileContents) : "Indentation error";
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Test
    public void testRoundTrip() {
        ObjectMapper mapper = JsonMapper.builder()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .build()
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

        try {
            File tempFile = File.createTempFile("rewritten", ".json");
            mapper.writeValue(tempFile, SparkThrowableHelper.errorClassToInfoMap);
            SortedMap<String, ErrorInfo> rereadErrorClassToInfoMap = mapper.readValue(
                    SparkThrowableHelper.errorClassesUrl,
                    new TypeReference<SortedMap<String, ErrorInfo>>(){});
            assert rereadErrorClassToInfoMap.size() ==
                    SparkThrowableHelper.errorClassToInfoMap.size();
            for (Map.Entry<String,ErrorInfo> entry :
                    SparkThrowableHelper.errorClassToInfoMap.entrySet()) {
                String errorClass = entry.getKey();
                ErrorInfo errorInfo = entry.getValue();
                assert rereadErrorClassToInfoMap.containsKey(errorClass);
                assert rereadErrorClassToInfoMap.get(errorClass).messageFormat.equals(
                        errorInfo.messageFormat);
                assert rereadErrorClassToInfoMap.get(errorClass).sqlState.equals(errorInfo.sqlState);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Test
    public void testSqlStateInvariants() {
        for (ErrorInfo errorInfo: SparkThrowableHelper.errorClassToInfoMap.values()) {
            String sqlState = errorInfo.sqlState;
            assert sqlState == null || sqlState.length() == 5 : "SQL state does not have length 5";
        }
    }

    @Test
    public void testMessageFormatInvariants() {
        HashSet<String> messageFormatSet = new HashSet<>();
        for (ErrorInfo errorInfo: SparkThrowableHelper.errorClassToInfoMap.values()) {
            String messageFormat = errorInfo.messageFormat;
            assert messageFormat != null : "Message format cannot be null";
            assert !messageFormatSet.contains(messageFormat) : "Duplicate message format";
            messageFormatSet.add(messageFormat);
        }
    }

    @Test
    public void testMissingErrorClass() {
        IllegalArgumentException ex1 = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> SparkThrowableHelper.getMessage("", new String[0]));

        assert ex1.getMessage().equals("Cannot find error class ''");

        IllegalArgumentException ex2 = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> SparkThrowableHelper.getMessage("LOREM_IPSUM", new String[0]));

        assert ex2.getMessage().equals("Cannot find error class 'LOREM_IPSUM'");
    }

    @Test
    public void testFormatErrorMessage() {
        String[] messageParameters = new String[2];
        messageParameters[0] = "foo";
        messageParameters[1] = "bar";
        String message = SparkThrowableHelper.getMessage("MISSING_COLUMN", messageParameters);
        assert message.equals("cannot resolve 'foo' given input columns: [bar]");
    }

    @Test
    public void testMessageParametersMatchMessageFormat() {
        Assert.assertThrows(
                IllegalFormatException.class,
                () -> SparkThrowableHelper.getMessage("MISSING_COLUMN", new String[0]));

        String[] messageParameters = new String[2];
        messageParameters[0] = "foo";
        messageParameters[1] = "bar";
        String message = SparkThrowableHelper.getMessage("DIVIDE_BY_ZERO", messageParameters);
        assert message.equals("divide by zero");
    }

    @Test
    public void testCatchSparkThrowable() {
        try {
            throw new SparkException("WRITING_JOB_ABORTED", new String[0], null);
        } catch (Throwable t) {
            assert t instanceof SparkThrowable;
            SparkThrowable st = (SparkThrowable) t;
            assert st.getErrorClass().equals("WRITING_JOB_ABORTED");
            assert st.getSqlState().equals("40000");
        }
    }
}
