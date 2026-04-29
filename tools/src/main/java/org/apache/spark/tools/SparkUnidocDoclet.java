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
package org.apache.spark.tools;

import com.sun.source.util.DocTreePath;

import jdk.javadoc.doclet.Reporter;
import jdk.javadoc.doclet.StandardDoclet;

import javax.lang.model.element.Element;
import javax.tools.Diagnostic;

import java.io.PrintWriter;
import java.util.Locale;

/**
 * Wraps the standard javadoc doclet so every Reporter diagnostic is mirrored
 * to stdout with a recognizable prefix before being delegated. Without this,
 * doclint diagnostics emitted during the doclet's tree-build phase can be
 * lost when javadoc bails on the first error -- the per-file file:line:
 * message becomes invisible in the CI log even though the build fails.
 *
 * Activate via:
 *   -doclet org.apache.spark.tools.SparkUnidocDoclet
 *   -docletpath path/to/tools-classes
 */
public final class SparkUnidocDoclet extends StandardDoclet {

  @Override
  public void init(Locale locale, Reporter reporter) {
    super.init(locale, new MirrorReporter(reporter));
  }

  @Override
  public String getName() {
    return "SparkUnidocDoclet";
  }

  private static final class MirrorReporter implements Reporter {
    private static final String PREFIX = "[unidoc-doclet]";
    private final Reporter delegate;

    MirrorReporter(Reporter delegate) {
      this.delegate = delegate;
    }

    private static void mirror(Diagnostic.Kind kind, String location, String message) {
      String loc = (location == null || location.isEmpty()) ? "" : (location + ": ");
      System.out.println(PREFIX + " " + loc + kind + ": " + message);
      System.out.flush();
    }

    @Override
    public void print(Diagnostic.Kind kind, String message) {
      mirror(kind, null, message);
      delegate.print(kind, message);
    }

    @Override
    public void print(Diagnostic.Kind kind, DocTreePath path, String message) {
      mirror(kind, locationOf(path), message);
      delegate.print(kind, path, message);
    }

    @Override
    public void print(Diagnostic.Kind kind, Element element, String message) {
      mirror(kind, element == null ? null : element.toString(), message);
      delegate.print(kind, element, message);
    }

    // JDK 17 Reporter has `getStandardWriter()` and `getDiagnosticWriter()` as
    // default methods returning null. The standard doclet calls these on
    // whatever Reporter we install; if we don't delegate to the wrapped
    // reporter (which has the real PrintWriter), the doclet NPEs trying to
    // use the null writer.
    @Override
    public PrintWriter getStandardWriter() {
      return delegate.getStandardWriter();
    }

    @Override
    public PrintWriter getDiagnosticWriter() {
      return delegate.getDiagnosticWriter();
    }

    private static String locationOf(DocTreePath path) {
      if (path == null) {
        return null;
      }
      try {
        return path.getTreePath().getCompilationUnit().getSourceFile().getName();
      } catch (Throwable t) {
        return null;
      }
    }
  }
}
