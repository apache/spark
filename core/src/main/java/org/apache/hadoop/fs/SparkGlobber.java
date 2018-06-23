/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * This is based on hadoop-common-2.7.2
 * {@link org.apache.hadoop.fs.Globber}.
 * This class exposes globWithThreshold which can be used glob path in parallel.
 */
public class SparkGlobber {
  public static final Log LOG = LogFactory.getLog(SparkGlobber.class.getName());

  private final FileSystem fs;
  private final FileContext fc;
  private final Path pathPattern;

  public SparkGlobber(FileSystem fs, Path pathPattern) {
    this.fs = fs;
    this.fc = null;
    this.pathPattern = pathPattern;
  }

  public SparkGlobber(FileContext fc, Path pathPattern) {
    this.fs = null;
    this.fc = fc;
    this.pathPattern = pathPattern;
  }

  private FileStatus getFileStatus(Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.getFileStatus(path);
      } else {
        return fc.getFileStatus(path);
      }
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  private FileStatus[] listStatus(Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.listStatus(path);
      } else {
        return fc.util().listStatus(path);
      }
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }

  private Path fixRelativePart(Path path) {
    if (fs != null) {
      return fs.fixRelativePart(path);
    } else {
      return fc.fixRelativePart(path);
    }
  }

  /**
   * Convert a path component that contains backslash ecape sequences to a
   * literal string.  This is necessary when you want to explicitly refer to a
   * path that contains globber metacharacters.
   */
  private static String unescapePathComponent(String name) {
    return name.replaceAll("\\\\(.)", "$1");
  }

  /**
   * Translate an absolute path into a list of path components.
   * We merge double slashes into a single slash here.
   * POSIX root path, i.e. '/', does not get an entry in the list.
   */
  private static List<String> getPathComponents(String path)
      throws IOException {
    ArrayList<String> ret = new ArrayList<String>();
    for (String component : path.split(Path.SEPARATOR)) {
      if (!component.isEmpty()) {
        ret.add(component);
      }
    }
    return ret;
  }

  private String schemeFromPath(Path path) throws IOException {
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      if (fs != null) {
        scheme = fs.getUri().getScheme();
      } else {
        scheme = fc.getFSofPath(fc.fixRelativePart(path)).
            getUri().getScheme();
      }
    }
    return scheme;
  }

  private String authorityFromPath(Path path) throws IOException {
    String authority = path.toUri().getAuthority();
    if (authority == null) {
      if (fs != null) {
        authority = fs.getUri().getAuthority();
      } else {
        authority = fc.getFSofPath(fc.fixRelativePart(path)).
            getUri().getAuthority();
      }
    }
    return authority ;
  }

  public FileStatus[] globWithThreshold(int threshold) throws IOException {
    // First we get the scheme and authority of the pattern that was passed
    // in.
    String scheme = schemeFromPath(pathPattern);
    String authority = authorityFromPath(pathPattern);

    // Next we strip off everything except the pathname itself, and expand all
    // globs.  Expansion is a process which turns "grouping" clauses,
    // expressed as brackets, into separate path patterns.
    String pathPatternString = pathPattern.toUri().getPath();
    List<String> flattenedPatterns = GlobExpander.expand(pathPatternString);

    // Now loop over all flattened patterns.  In every case, we'll be trying to
    // match them to entries in the filesystem.
    ArrayList<FileStatus> results =
        new ArrayList<FileStatus>(flattenedPatterns.size());
    boolean sawWildcard = false;
    for (String flatPattern : flattenedPatterns) {
      // Get the absolute path for this flattened pattern.  We couldn't do
      // this prior to flattening because of patterns like {/,a}, where which
      // path you go down influences how the path must be made absolute.
      Path absPattern = fixRelativePart(new Path(
          flatPattern.isEmpty() ? Path.CUR_DIR : flatPattern));
      // Now we break the flattened, absolute pattern into path components.
      // For example, /a/*/c would be broken into the list [a, *, c]
      List<String> components =
          getPathComponents(absPattern.toUri().getPath());
      // Starting out at the root of the filesystem, we try to match
      // filesystem entries against pattern components.
      ArrayList<FileStatus> candidates = new ArrayList<FileStatus>(1);
      // To get the "real" FileStatus of root, we'd have to do an expensive
      // RPC to the NameNode.  So we create a placeholder FileStatus which has
      // the correct path, but defaults for the rest of the information.
      // Later, if it turns out we actually want the FileStatus of root, we'll
      // replace the placeholder with a real FileStatus obtained from the
      // NameNode.
      FileStatus rootPlaceholder;
      if (Path.WINDOWS && !components.isEmpty()
          && Path.isWindowsAbsolutePath(absPattern.toUri().getPath(), true)) {
        // On Windows the path could begin with a drive letter, e.g. /E:/foo.
        // We will skip matching the drive letter and start from listing the
        // root of the filesystem on that drive.
        String driveLetter = components.remove(0);
        rootPlaceholder = new FileStatus(0, true, 0, 0, 0, new Path(scheme,
            authority, Path.SEPARATOR + driveLetter + Path.SEPARATOR));
      } else {
        rootPlaceholder = new FileStatus(0, true, 0, 0, 0,
            new Path(scheme, authority, Path.SEPARATOR));
      }
      candidates.add(rootPlaceholder);

      for (int componentIdx = 0; componentIdx < components.size();
         componentIdx++) {
        ArrayList<FileStatus> newCandidates =
            new ArrayList<FileStatus>(candidates.size());
        GlobFilter globFilter = new GlobFilter(components.get(componentIdx));
        String component = unescapePathComponent(components.get(componentIdx));
        if (globFilter.hasPattern()) {
          sawWildcard = true;
        }
        if (candidates.isEmpty() && sawWildcard) {
          // Optimization: if there are no more candidates left, stop examining
          // the path components.  We can only do this if we've already seen
          // a wildcard component-- otherwise, we still need to visit all path
          // components in case one of them is a wildcard.
          break;
        }
        if ((componentIdx < components.size() - 1) &&
            (!globFilter.hasPattern())) {
          // Optimization: if this is not the terminal path component, and we
          // are not matching against a glob, assume that it exists.  If it
          // doesn't exist, we'll find out later when resolving a later glob
          // or the terminal path component.
          for (FileStatus candidate : candidates) {
            candidate.setPath(new Path(candidate.getPath(), component));
          }
          continue;
        }
        for (FileStatus candidate : candidates) {
          if (globFilter.hasPattern()) {
            FileStatus[] children = listStatus(candidate.getPath());
            if (children.length == 1) {
              // If we get back only one result, this could be either a listing
              // of a directory with one entry, or it could reflect the fact
              // that what we listed resolved to a file.
              //
              // Unfortunately, we can't just compare the returned paths to
              // figure this out.  Consider the case where you have /a/b, where
              // b is a symlink to "..".  In that case, listing /a/b will give
              // back "/a/b" again.  If we just went by returned pathname, we'd
              // incorrectly conclude that /a/b was a file and should not match
              // /a/*/*.  So we use getFileStatus of the path we just listed to
              // disambiguate.
              if (!getFileStatus(candidate.getPath()).isDirectory()) {
                continue;
              }
            }
            for (FileStatus child : children) {
              if (componentIdx < components.size() - 1) {
                // Don't try to recurse into non-directories.  See HADOOP-10957.
                if (!child.isDirectory()) continue;
              }
              // Set the child path based on the parent path.
              child.setPath(new Path(candidate.getPath(),
                  child.getPath().getName()));
              if (globFilter.accept(child.getPath())) {
                newCandidates.add(child);
              }
            }
          } else {
            // When dealing with non-glob components, use getFileStatus
            // instead of listStatus.  This is an optimization, but it also
            // is necessary for correctness in HDFS, since there are some
            // special HDFS directories like .reserved and .snapshot that are
            // not visible to listStatus, but which do exist.  (See HADOOP-9877)
            FileStatus childStatus = getFileStatus(
                new Path(candidate.getPath(), component));
            if (childStatus != null) {
              newCandidates.add(childStatus);
            }
          }
        }
        candidates = newCandidates;
        // When size of candidates has reached the threshold, stop expanding other
        // components and add them to candidates.
        if (candidates.size() >= threshold && componentIdx + 1 != components.size()) {
          String restPath = StringUtils.join(
              components.subList(componentIdx + 1, components.size()), "/");
          for (FileStatus candidate : candidates) {
            candidate.setPath(new Path(candidate.getPath(), restPath));
          }
          break;
        }
      }
      for (FileStatus status : candidates) {
        // Use object equality to see if this status is the root placeholder.
        // See the explanation for rootPlaceholder above for more information.
        if (status == rootPlaceholder) {
          status = getFileStatus(rootPlaceholder.getPath());
          if (status == null) continue;
        }
        results.add(status);
      }
    }
    /*
     * When the input pattern "looks" like just a simple filename, and we
     * can't find it, we return null rather than an empty array.
     * This is a special case which the shell relies on.
     *
     * To be more precise: if there were no results, AND there were no
     * groupings (aka brackets), and no wildcards in the input (aka stars),
     * we return null.
     */
    if ((!sawWildcard) && results.isEmpty() &&
        (flattenedPatterns.size() <= 1)) {
      return null;
    }
    return results.toArray(new FileStatus[0]);
  }
}
