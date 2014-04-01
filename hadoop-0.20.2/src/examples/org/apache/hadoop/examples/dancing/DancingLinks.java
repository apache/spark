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
package org.apache.hadoop.examples.dancing;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A generic solver for tile laying problems using Knuth's dancing link
 * algorithm. It provides a very fast backtracking data structure for problems
 * that can expressed as a sparse boolean matrix where the goal is to select a
 * subset of the rows such that each column has exactly 1 true in it.
 * 
 * The application gives each column a name and each row is named after the
 * set of columns that it has as true. Solutions are passed back by giving the 
 * selected rows' names.
 * 
 * The type parameter ColumnName is the class of application's column names.
 */
public class DancingLinks<ColumnName> {
  private static final Log LOG = 
    LogFactory.getLog(DancingLinks.class.getName());
  
  /**
   * A cell in the table with up/down and left/right links that form doubly
   * linked lists in both directions. It also includes a link to the column
   * head.
   */
  private static class Node<ColumnName> {
    Node<ColumnName> left;
    Node<ColumnName> right;
    Node<ColumnName> up;
    Node<ColumnName> down;
    ColumnHeader<ColumnName> head;
    
    Node(Node<ColumnName> l, Node<ColumnName> r, Node<ColumnName> u, 
         Node<ColumnName> d, ColumnHeader<ColumnName> h) {
      left = l;
      right = r;
      up = u;
      down = d;
      head = h;
    }
    
    Node() {
      this(null, null, null, null, null);
    }
  }
  
  /**
   * Column headers record the name of the column and the number of rows that 
   * satisfy this column. The names are provided by the application and can 
   * be anything. The size is used for the heuristic for picking the next 
   * column to explore.
   */
  private static class ColumnHeader<ColumnName> extends Node<ColumnName> {
    ColumnName name;
    int size;
    
    ColumnHeader(ColumnName n, int s) {
      name = n;
      size = s;
      head = this;
    }
    
    ColumnHeader() {
      this(null, 0);
    }
  }
  
  /**
   * The head of the table. Left/Right from the head are the unsatisfied 
   * ColumnHeader objects.
   */
  private ColumnHeader<ColumnName> head;
  
  /**
   * The complete list of columns.
   */
  private List<ColumnHeader<ColumnName>> columns;
  
  public DancingLinks() {
    head = new ColumnHeader<ColumnName>(null, 0);
    head.left = head;
    head.right = head;
    head.up = head;
    head.down = head;
    columns = new ArrayList<ColumnHeader<ColumnName>>(200);
  }
  
  /**
   * Add a column to the table
   * @param name The name of the column, which will be returned as part of 
   *             solutions
   * @param primary Is the column required for a solution?
   */
  public void addColumn(ColumnName name, boolean primary) {
    ColumnHeader<ColumnName> top = new ColumnHeader<ColumnName>(name, 0);
    top.up = top;
    top.down = top;
    if (primary) {
      Node<ColumnName> tail = head.left;
      tail.right = top;
      top.left = tail;
      top.right = head;
      head.left = top;
    } else {
      top.left = top;
      top.right = top;
    }
    columns.add(top);
  }
  
  /**
   * Add a column to the table
   * @param name The name of the column, which will be included in the solution
   */
  public void addColumn(ColumnName name) {
    addColumn(name, true);
  }
  
  /**
   * Get the number of columns.
   * @return the number of columns
   */
  public int getNumberColumns() {
    return columns.size();
  }
  
  /**
   * Get the name of a given column as a string
   * @param index the index of the column
   * @return a string representation of the name
   */
  public String getColumnName(int index) {
    return columns.get(index).name.toString();
  }
  
  /**
   * Add a row to the table. 
   * @param values the columns that are satisfied by this row
   */
  public void addRow(boolean[] values) {
    Node<ColumnName> prev = null;
    for(int i=0; i < values.length; ++i) {
      if (values[i]) {
        ColumnHeader<ColumnName> top = columns.get(i);
        top.size += 1;
        Node<ColumnName> bottom = top.up;
        Node<ColumnName> node = new Node<ColumnName>(null, null, bottom, 
                                                     top, top);
        bottom.down = node;
        top.up = node;
        if (prev != null) {
          Node<ColumnName> front = prev.right;
          node.left = prev;
          node.right = front;
          prev.right = node;
          front.left = node;
        } else {
          node.left = node;
          node.right = node;
        }
        prev = node;
      }
    }
  }
  
  /**
   * Applications should implement this to receive the solutions to their 
   * problems.
   */
  public interface SolutionAcceptor<ColumnName> {
    /**
     * A callback to return a solution to the application.
     * @param value a List of List of ColumnNames that were satisfied by each
     *              selected row
     */
    void solution(List<List<ColumnName>> value);
  }
  
  /**
   * Find the column with the fewest choices.
   * @return The column header
   */
  private ColumnHeader<ColumnName> findBestColumn() {
    int lowSize = Integer.MAX_VALUE;
    ColumnHeader<ColumnName> result = null;
    ColumnHeader<ColumnName> current = (ColumnHeader<ColumnName>) head.right;
    while (current != head) {
      if (current.size < lowSize) {
        lowSize = current.size;
        result = current;
      }
      current = (ColumnHeader<ColumnName>) current.right;
    }
    return result;
  }
  
  /**
   * Hide a column in the table
   * @param col the column to hide
   */
  private void coverColumn(ColumnHeader<ColumnName> col) {
    LOG.debug("cover " + col.head.name);
    // remove the column
    col.right.left = col.left;
    col.left.right = col.right;
    Node<ColumnName> row = col.down;
    while (row != col) {
      Node<ColumnName> node = row.right;
      while (node != row) {
        node.down.up = node.up;
        node.up.down = node.down;
        node.head.size -= 1;
        node = node.right;
      }
      row = row.down;
    }
  }
  
  /**
   * Uncover a column that was hidden.
   * @param col the column to unhide
   */
  private void uncoverColumn(ColumnHeader<ColumnName> col) {
    LOG.debug("uncover " + col.head.name);
    Node<ColumnName> row = col.up;
    while (row != col) {
      Node<ColumnName> node = row.left;
      while (node != row) {
        node.head.size += 1;
        node.down.up = node;
        node.up.down = node;
        node = node.left;
      }
      row = row.up;
    }
    col.right.left = col;
    col.left.right = col;
  }
  
  /**
   * Get the name of a row by getting the list of column names that it 
   * satisfies.
   * @param row the row to make a name for
   * @return the list of column names
   */
  private List<ColumnName> getRowName(Node<ColumnName> row) {
    List<ColumnName> result = new ArrayList<ColumnName>();
    result.add(row.head.name);
    Node<ColumnName> node = row.right;
    while (node != row) {
      result.add(node.head.name);
      node = node.right;
    }
    return result;
  }
  
  /**
   * Find a solution to the problem.
   * @param partial a temporary datastructure to keep the current partial 
   *                answer in
   * @param output the acceptor for the results that are found
   * @return the number of solutions found
   */
  private int search(List<Node<ColumnName>> partial, SolutionAcceptor<ColumnName> output) {
    int results = 0;
    if (head.right == head) {
      List<List<ColumnName>> result = new ArrayList<List<ColumnName>>(partial.size());
      for(Node<ColumnName> row: partial) {
        result.add(getRowName(row));
      }
      output.solution(result);
      results += 1;
    } else {
      ColumnHeader<ColumnName> col = findBestColumn();
      if (col.size > 0) {
        coverColumn(col);
        Node<ColumnName> row = col.down;
        while (row != col) {
          partial.add(row);
          Node<ColumnName> node = row.right;
          while (node != row) {
            coverColumn(node.head);
            node = node.right;
          }
          results += search(partial, output);
          partial.remove(partial.size() - 1);
          node = row.left;
          while (node != row) {
            uncoverColumn(node.head);
            node = node.left;
          }
          row = row.down;
        }
        uncoverColumn(col);
      }
    }
    return results;
  }
  
  /**
   * Generate a list of prefixes down to a given depth. Assumes that the 
   * problem is always deeper than depth.
   * @param depth the depth to explore down
   * @param choices an array of length depth to describe a prefix
   * @param prefixes a working datastructure
   */
  private void searchPrefixes(int depth, int[] choices, 
                              List<int[]> prefixes) {
    if (depth == 0) {
      prefixes.add(choices.clone());
    } else {
      ColumnHeader<ColumnName> col = findBestColumn();
      if (col.size > 0) {
        coverColumn(col);
        Node<ColumnName> row = col.down;
        int rowId = 0;
        while (row != col) {
          Node<ColumnName> node = row.right;
          while (node != row) {
            coverColumn(node.head);
            node = node.right;
          }
          choices[choices.length - depth] = rowId;
          searchPrefixes(depth - 1, choices, prefixes);
          node = row.left;
          while (node != row) {
            uncoverColumn(node.head);
            node = node.left;
          }
          row = row.down;
          rowId += 1;
        }
        uncoverColumn(col);
      }
    }
  }
  
  /**
   * Generate a list of row choices to cover the first moves.
   * @param depth the length of the prefixes to generate
   * @return a list of integer arrays that list the rows to pick in order
   */
  public List<int[]> split(int depth) {
    int[] choices = new int[depth];
    List<int[]> result = new ArrayList<int[]>(100000);
    searchPrefixes(depth, choices, result);
    return result;
  }

  /**
   * Make one move from a prefix
   * @param goalRow the row that should be choosen
   * @return the row that was found
   */
  private Node<ColumnName> advance(int goalRow) {
    ColumnHeader<ColumnName> col = findBestColumn();
    if (col.size > 0) {
      coverColumn(col);
      Node<ColumnName> row = col.down;
      int id = 0;
      while (row != col) {
        if (id == goalRow) {
          Node<ColumnName> node = row.right;
          while (node != row) {
            coverColumn(node.head);
            node = node.right;
          }
          return row;
        }
        id += 1;
        row = row.down;
      }
    }
    return null;
  }
  
  /**
   * Undo a prefix exploration
   * @param row
   */
  private void rollback(Node<ColumnName> row) {
    Node<ColumnName> node = row.left;
    while (node != row) {
      uncoverColumn(node.head);
      node = node.left;
    }
    uncoverColumn(row.head);
  }
  
  /**
   * Given a prefix, find solutions under it.
   * @param prefix a list of row choices that control which part of the search
   *               tree to explore
   * @param output the output for each solution
   * @return the number of solutions
   */
  public int solve(int[] prefix, SolutionAcceptor<ColumnName> output) {
    List<Node<ColumnName>> choices = new ArrayList<Node<ColumnName>>();
    for(int i=0; i < prefix.length; ++i) {
      choices.add(advance(prefix[i]));
    }
    int result = search(choices, output);
    for(int i=prefix.length-1; i >=0; --i) {
      rollback(choices.get(i));
    }
    return result;
  }
  
  /**
   * Solve a complete problem
   * @param output the acceptor to receive answers
   * @return the number of solutions
   */
  public int solve(SolutionAcceptor<ColumnName> output) {
    return search(new ArrayList<Node<ColumnName>>(), output);
  }
  
}
