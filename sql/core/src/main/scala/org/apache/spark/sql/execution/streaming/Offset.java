package org.apache.spark.sql.execution.streaming;

/**
 * This is an internal, deprecated interface. New source implementations should use the
 * org.apache.spark.sql.sources.v2.reader.Offset class, which is the one that will be supported
 * in the long term.
 *
 * This class will be removed in a future release.
 */
public abstract class Offset {
    /**
     * A JSON-serialized representation of an Offset that is
     * used for saving offsets to the offset log.
     * Note: We assume that equivalent/equal offsets serialize to
     * identical JSON strings.
     *
     * @return JSON string encoding
     */
    public abstract String json();

    /**
     * Equality based on JSON string representation. We leverage the
     * JSON representation for normalization between the Offset's
     * in memory and on disk representations.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Offset) {
            return this.json().equals(((Offset) obj).json());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.json().hashCode();
    }

    @Override
    public String toString() {
        return this.json();
    }
}
