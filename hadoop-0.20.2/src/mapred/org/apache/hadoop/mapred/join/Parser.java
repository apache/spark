/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.join;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Very simple shift-reduce parser for join expressions.
 *
 * This should be sufficient for the user extension permitted now, but ought to
 * be replaced with a parser generator if more complex grammars are supported.
 * In particular, this &quot;shift-reduce&quot; parser has no states. Each set
 * of formals requires a different internal node type, which is responsible for
 * interpreting the list of tokens it receives. This is sufficient for the
 * current grammar, but it has several annoying properties that might inhibit
 * extension. In particular, parenthesis are always function calls; an
 * algebraic or filter grammar would not only require a node type, but must
 * also work around the internals of this parser.
 *
 * For most other cases, adding classes to the hierarchy- particularly by
 * extending JoinRecordReader and MultiFilterRecordReader- is fairly
 * straightforward. One need only override the relevant method(s) (usually only
 * {@link CompositeRecordReader#combine}) and include a property to map its
 * value to an identifier in the parser.
 */
public class Parser {
  public enum TType { CIF, IDENT, COMMA, LPAREN, RPAREN, QUOT, NUM, }

  /**
   * Tagged-union type for tokens from the join expression.
   * @see Parser.TType
   */
  public static class Token {

    private TType type;

    Token(TType type) {
      this.type = type;
    }

    public TType getType() { return type; }
    public Node getNode() throws IOException {
      throw new IOException("Expected nodetype");
    }
    public double getNum() throws IOException {
      throw new IOException("Expected numtype");
    }
    public String getStr() throws IOException {
      throw new IOException("Expected strtype");
    }
  }

  public static class NumToken extends Token {
    private double num;
    public NumToken(double num) {
      super(TType.NUM);
      this.num = num;
    }
    public double getNum() { return num; }
  }

  public static class NodeToken extends Token {
    private Node node;
    NodeToken(Node node) {
      super(TType.CIF);
      this.node = node;
    }
    public Node getNode() {
      return node;
    }
  }

  public static class StrToken extends Token {
    private String str;
    public StrToken(TType type, String str) {
      super(type);
      this.str = str;
    }
    public String getStr() {
      return str;
    }
  }

  /**
   * Simple lexer wrapping a StreamTokenizer.
   * This encapsulates the creation of tagged-union Tokens and initializes the
   * SteamTokenizer.
   */
  private static class Lexer {

    private StreamTokenizer tok;

    Lexer(String s) {
      tok = new StreamTokenizer(new CharArrayReader(s.toCharArray()));
      tok.quoteChar('"');
      tok.parseNumbers();
      tok.ordinaryChar(',');
      tok.ordinaryChar('(');
      tok.ordinaryChar(')');
      tok.wordChars('$','$');
      tok.wordChars('_','_');
    }

    Token next() throws IOException {
      int type = tok.nextToken();
      switch (type) {
        case StreamTokenizer.TT_EOF:
        case StreamTokenizer.TT_EOL:
          return null;
        case StreamTokenizer.TT_NUMBER:
          return new NumToken(tok.nval);
        case StreamTokenizer.TT_WORD:
          return new StrToken(TType.IDENT, tok.sval);
        case '"':
          return new StrToken(TType.QUOT, tok.sval);
        default:
          switch (type) {
            case ',':
              return new Token(TType.COMMA);
            case '(':
              return new Token(TType.LPAREN);
            case ')':
              return new Token(TType.RPAREN);
            default:
              throw new IOException("Unexpected: " + type);
          }
      }
    }
  }

  public abstract static class Node implements ComposableInputFormat {
    /**
     * Return the node type registered for the particular identifier.
     * By default, this is a CNode for any composite node and a WNode
     * for &quot;wrapped&quot; nodes. User nodes will likely be composite
     * nodes.
     * @see #addIdentifier(java.lang.String, java.lang.Class[], java.lang.Class, java.lang.Class)
     * @see CompositeInputFormat#setFormat(org.apache.hadoop.mapred.JobConf)
     */
    static Node forIdent(String ident) throws IOException {
      try {
        if (!nodeCstrMap.containsKey(ident)) {
          throw new IOException("No nodetype for " + ident);
        }
        return nodeCstrMap.get(ident).newInstance(ident);
      } catch (IllegalAccessException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InvocationTargetException e) {
        throw (IOException)new IOException().initCause(e);
      }
    }

    private static final Class<?>[] ncstrSig = { String.class };
    private static final
        Map<String,Constructor<? extends Node>> nodeCstrMap =
        new HashMap<String,Constructor<? extends Node>>();
    protected static final
        Map<String,Constructor<? extends ComposableRecordReader>> rrCstrMap =
        new HashMap<String,Constructor<? extends ComposableRecordReader>>();

    /**
     * For a given identifier, add a mapping to the nodetype for the parse
     * tree and to the ComposableRecordReader to be created, including the
     * formals required to invoke the constructor.
     * The nodetype and constructor signature should be filled in from the
     * child node.
     */
    protected static void addIdentifier(String ident, Class<?>[] mcstrSig,
                              Class<? extends Node> nodetype,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Constructor<? extends Node> ncstr =
        nodetype.getDeclaredConstructor(ncstrSig);
      ncstr.setAccessible(true);
      nodeCstrMap.put(ident, ncstr);
      Constructor<? extends ComposableRecordReader> mcstr =
        cl.getDeclaredConstructor(mcstrSig);
      mcstr.setAccessible(true);
      rrCstrMap.put(ident, mcstr);
    }

    // inst
    protected int id = -1;
    protected String ident;
    protected Class<? extends WritableComparator> cmpcl;

    protected Node(String ident) {
      this.ident = ident;
    }

    protected void setID(int id) {
      this.id = id;
    }

    protected void setKeyComparator(Class<? extends WritableComparator> cmpcl) {
      this.cmpcl = cmpcl;
    }
    abstract void parse(List<Token> args, JobConf job) throws IOException;
  }

  /**
   * Nodetype in the parse tree for &quot;wrapped&quot; InputFormats.
   */
  static class WNode extends Node {
    private static final Class<?>[] cstrSig =
      { Integer.TYPE, RecordReader.class, Class.class };

    static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, WNode.class, cl);
    }

    private String indir;
    private InputFormat inf;

    public WNode(String ident) {
      super(ident);
    }

    /**
     * Let the first actual define the InputFormat and the second define
     * the <tt>mapred.input.dir</tt> property.
     */
    public void parse(List<Token> ll, JobConf job) throws IOException {
      StringBuilder sb = new StringBuilder();
      Iterator<Token> i = ll.iterator();
      while (i.hasNext()) {
        Token t = i.next();
        if (TType.COMMA.equals(t.getType())) {
          try {
          	inf = (InputFormat)ReflectionUtils.newInstance(
          			job.getClassByName(sb.toString()),
                job);
          } catch (ClassNotFoundException e) {
            throw (IOException)new IOException().initCause(e);
          } catch (IllegalArgumentException e) {
            throw (IOException)new IOException().initCause(e);
          }
          break;
        }
        sb.append(t.getStr());
      }
      if (!i.hasNext()) {
        throw new IOException("Parse error");
      }
      Token t = i.next();
      if (!TType.QUOT.equals(t.getType())) {
        throw new IOException("Expected quoted string");
      }
      indir = t.getStr();
      // no check for ll.isEmpty() to permit extension
    }

    private JobConf getConf(JobConf job) {
      JobConf conf = new JobConf(job);
      FileInputFormat.setInputPaths(conf, indir);
      return conf;
    }

    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      return inf.getSplits(getConf(job), numSplits);
    }

    public ComposableRecordReader getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        return rrCstrMap.get(ident).newInstance(id,
            inf.getRecordReader(split, getConf(job), reporter), cmpcl);
      } catch (IllegalAccessException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InvocationTargetException e) {
        throw (IOException)new IOException().initCause(e);
      }
    }

    public String toString() {
      return ident + "(" + inf.getClass().getName() + ",\"" + indir + "\")";
    }
  }

  /**
   * Internal nodetype for &quot;composite&quot; InputFormats.
   */
  static class CNode extends Node {

    private static final Class<?>[] cstrSig =
      { Integer.TYPE, JobConf.class, Integer.TYPE, Class.class };

    static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, CNode.class, cl);
    }

    // inst
    private ArrayList<Node> kids = new ArrayList<Node>();

    public CNode(String ident) {
      super(ident);
    }

    public void setKeyComparator(Class<? extends WritableComparator> cmpcl) {
      super.setKeyComparator(cmpcl);
      for (Node n : kids) {
        n.setKeyComparator(cmpcl);
      }
    }

    /**
     * Combine InputSplits from child InputFormats into a
     * {@link CompositeInputSplit}.
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      InputSplit[][] splits = new InputSplit[kids.size()][];
      for (int i = 0; i < kids.size(); ++i) {
        final InputSplit[] tmp = kids.get(i).getSplits(job, numSplits);
        if (null == tmp) {
          throw new IOException("Error gathering splits from child RReader");
        }
        if (i > 0 && splits[i-1].length != tmp.length) {
          throw new IOException("Inconsistent split cardinality from child " +
              i + " (" + splits[i-1].length + "/" + tmp.length + ")");
        }
        splits[i] = tmp;
      }
      final int size = splits[0].length;
      CompositeInputSplit[] ret = new CompositeInputSplit[size];
      for (int i = 0; i < size; ++i) {
        ret[i] = new CompositeInputSplit(splits.length);
        for (int j = 0; j < splits.length; ++j) {
          ret[i].add(splits[j][i]);
        }
      }
      return ret;
    }

    @SuppressWarnings("unchecked") // child types unknowable
    public ComposableRecordReader getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      if (!(split instanceof CompositeInputSplit)) {
        throw new IOException("Invalid split type:" +
                              split.getClass().getName());
      }
      final CompositeInputSplit spl = (CompositeInputSplit)split;
      final int capacity = kids.size();
      CompositeRecordReader ret = null;
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        ret = (CompositeRecordReader)
          rrCstrMap.get(ident).newInstance(id, job, capacity, cmpcl);
      } catch (IllegalAccessException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InvocationTargetException e) {
        throw (IOException)new IOException().initCause(e);
      }
      for (int i = 0; i < capacity; ++i) {
        ret.add(kids.get(i).getRecordReader(spl.get(i), job, reporter));
      }
      return (ComposableRecordReader)ret;
    }

    /**
     * Parse a list of comma-separated nodes.
     */
    public void parse(List<Token> args, JobConf job) throws IOException {
      ListIterator<Token> i = args.listIterator();
      while (i.hasNext()) {
        Token t = i.next();
        t.getNode().setID(i.previousIndex() >> 1);
        kids.add(t.getNode());
        if (i.hasNext() && !TType.COMMA.equals(i.next().getType())) {
          throw new IOException("Expected ','");
        }
      }
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(ident + "(");
      for (Node n : kids) {
        sb.append(n.toString() + ",");
      }
      sb.setCharAt(sb.length() - 1, ')');
      return sb.toString();
    }
  }

  private static Token reduce(Stack<Token> st, JobConf job) throws IOException {
    LinkedList<Token> args = new LinkedList<Token>();
    while (!st.isEmpty() && !TType.LPAREN.equals(st.peek().getType())) {
      args.addFirst(st.pop());
    }
    if (st.isEmpty()) {
      throw new IOException("Unmatched ')'");
    }
    st.pop();
    if (st.isEmpty() || !TType.IDENT.equals(st.peek().getType())) {
      throw new IOException("Identifier expected");
    }
    Node n = Node.forIdent(st.pop().getStr());
    n.parse(args, job);
    return new NodeToken(n);
  }

  /**
   * Given an expression and an optional comparator, build a tree of
   * InputFormats using the comparator to sort keys.
   */
  static Node parse(String expr, JobConf job) throws IOException {
    if (null == expr) {
      throw new IOException("Expression is null");
    }
    Class<? extends WritableComparator> cmpcl =
      job.getClass("mapred.join.keycomparator", null, WritableComparator.class);
    Lexer lex = new Lexer(expr);
    Stack<Token> st = new Stack<Token>();
    Token tok;
    while ((tok = lex.next()) != null) {
      if (TType.RPAREN.equals(tok.getType())) {
        st.push(reduce(st, job));
      } else {
        st.push(tok);
      }
    }
    if (st.size() == 1 && TType.CIF.equals(st.peek().getType())) {
      Node ret = st.pop().getNode();
      if (cmpcl != null) {
        ret.setKeyComparator(cmpcl);
      }
      return ret;
    }
    throw new IOException("Missing ')'");
  }

}
