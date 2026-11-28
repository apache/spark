/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

import javax.annotation.Nullable;

import org.apache.spark.annotation.Unstable;

/**
 * The external value of the {@link FileType FILE} data type: a reference to a file, with metadata
 * describing it. This is what a FILE value is in a {@code Row}, in {@code collect()} results and
 * in the arguments and results of JVM UDFs.
 *
 * <p>Every field is nullable, as in the Parquet FILE logical type. A reference names the bytes it
 * refers to through {@link #uri}, and {@link #offset} and {@link #size} select the byte range
 * {@code [offset, offset + size)} of it. The Parquet FILE type also allows the bytes to be stored
 * inline in the value. A FileRef carries those inline bytes, so that a value converts to a FileRef
 * and back without loss, but its API has no constructor or getter for them. The canonical JSON
 * form of a value, as shown by {@code Dataset.show()}, includes them.
 *
 * <p>A FileRef is a value: two FileRefs are equal when all their fields are equal. This is
 * unrelated to the FILE type in SQL, which defines no equality.
 *
 * @since 4.5.0
 */
@Unstable
public final class FileRef implements Serializable {
  private static final long serialVersionUID = 1L;

  @Nullable private final String uri;
  @Nullable private final Long offset;
  @Nullable private final Long size;
  @Nullable private final String contentType;
  @Nullable private final String checksum;
  @Nullable private final byte[] inline;

  /**
   * Creates a FileRef.
   *
   * @param uri the opaque URI of the referenced file, which may be absolute or relative
   * @param offset the start of the referenced byte range
   * @param size the length of the referenced byte range
   * @param contentType the MIME/content type of the referenced bytes
   * @param checksum an algorithm-tagged integrity token for the referenced bytes, such as
   *     {@code "MD5:<digest>"}
   */
  public FileRef(
      @Nullable String uri,
      @Nullable Long offset,
      @Nullable Long size,
      @Nullable String contentType,
      @Nullable String checksum) {
    this(uri, offset, size, contentType, checksum, null);
  }

  /** Creates a FileRef that also carries the referenced bytes inline. Internal to Spark. */
  FileRef(
      @Nullable String uri,
      @Nullable Long offset,
      @Nullable Long size,
      @Nullable String contentType,
      @Nullable String checksum,
      @Nullable byte[] inline) {
    this.uri = uri;
    this.offset = offset;
    this.size = size;
    this.contentType = contentType;
    this.checksum = checksum;
    this.inline = inline;
  }

  /** Returns the URI of the referenced file, or null if it is not set. */
  @Nullable
  public String uri() {
    return uri;
  }

  /** Returns the start of the referenced byte range, or null if it is not set. */
  @Nullable
  public Long offset() {
    return offset;
  }

  /** Returns the length of the referenced byte range, or null if it is not set. */
  @Nullable
  public Long size() {
    return size;
  }

  /** Returns the MIME/content type of the referenced bytes, or null if it is not set. */
  @Nullable
  public String contentType() {
    return contentType;
  }

  /**
   * Returns the algorithm-tagged integrity token of the referenced bytes, such as
   * {@code "MD5:<digest>"}, or null if it is not set.
   */
  @Nullable
  public String checksum() {
    return checksum;
  }

  /** Returns the referenced bytes stored inline, or null. Internal to Spark. */
  @Nullable
  byte[] inline() {
    return inline;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FileRef that)) return false;
    return Objects.equals(uri, that.uri)
        && Objects.equals(offset, that.offset)
        && Objects.equals(size, that.size)
        && Objects.equals(contentType, that.contentType)
        && Objects.equals(checksum, that.checksum)
        && Arrays.equals(inline, that.inline);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, offset, size, contentType, checksum, Arrays.hashCode(inline));
  }

  /** Returns the fields that are set, without the inline bytes. */
  @Override
  public String toString() {
    StringJoiner fields = new StringJoiner(", ", "FileRef{", "}");
    if (uri != null) fields.add("uri='" + uri + "'");
    if (offset != null) fields.add("offset=" + offset);
    if (size != null) fields.add("size=" + size);
    if (contentType != null) fields.add("contentType='" + contentType + "'");
    if (checksum != null) fields.add("checksum='" + checksum + "'");
    if (inline != null) fields.add("inline=<" + inline.length + " bytes>");
    return fields.toString();
  }
}
