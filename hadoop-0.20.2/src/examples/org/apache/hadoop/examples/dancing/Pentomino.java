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

public class Pentomino {

  /**
   * This interface just is a marker for what types I expect to get back
   * as column names.
   */
  protected static interface ColumnName {
    // NOTHING
  }

  /**
   * Maintain information about a puzzle piece.
   */
  protected static class Piece implements ColumnName {
    private String name;
    private boolean [][] shape;
    private int[] rotations;
    private boolean flippable;
    
    public Piece(String name, String shape, 
                 boolean flippable, int[] rotations) {
      this.name = name;
      this.rotations = rotations;
      this.flippable = flippable;
      StringTokenizer parser = new StringTokenizer(shape, "/");
      List<boolean[]> lines = new ArrayList<boolean[]>();
      while (parser.hasMoreTokens()) {
        String token = parser.nextToken();
        boolean[] line = new boolean[token.length()];
        for(int i=0; i < line.length; ++i) {
          line[i] = token.charAt(i) == 'x';
        }
        lines.add(line);
      }
      this.shape = new boolean[lines.size()][];
      for(int i=0 ; i < lines.size(); i++) {
        this.shape[i] = (boolean[]) lines.get(i);
      }
    }
    
    public String getName() {
      return name;
    }
    
    public int[] getRotations() {
      return rotations;
    }
    
    public boolean getFlippable() {
      return flippable;
    }
    
    private int doFlip(boolean flip, int x, int max) {
      if (flip) {
        return max - x - 1;
      } else {
        return x;
      }
    }
    
    public boolean[][] getShape(boolean flip, int rotate) {
      boolean [][] result;
      if (rotate % 2 == 0) {
        int height = shape.length;
        int width = shape[0].length;
        result = new boolean[height][];
        boolean flipX = rotate == 2;
        boolean flipY = flip ^ (rotate == 2);
        for (int y = 0; y < height; ++y) {
          result[y] = new boolean[width];
          for (int x=0; x < width; ++x) {
            result[y][x] = shape[doFlip(flipY, y, height)]
                                 [doFlip(flipX, x, width)];
          }
        }
      } else {
        int height = shape[0].length;
        int width = shape.length;
        result = new boolean[height][];
        boolean flipX = rotate == 3;
        boolean flipY = flip ^ (rotate == 1);
        for (int y = 0; y < height; ++y) {
          result[y] = new boolean[width];
          for (int x=0; x < width; ++x) {
            result[y][x] = shape[doFlip(flipX, x, width)]
                                 [doFlip(flipY, y, height)];
          }
        }        
      }
      return result;
    }
  }

  /**
   * A point in the puzzle board. This represents a placement of a piece into
   * a given point on the board.
   */
  static class Point implements ColumnName {
    int x;
    int y;
    Point(int x, int y) {
      this.x = x;
      this.y = y;
    }
  }
  

  /**
   * Convert a solution to the puzzle returned by the model into a string
   * that represents the placement of the pieces onto the board.
   * @param width the width of the puzzle board
   * @param height the height of the puzzle board
   * @param solution the list of column names that were selected in the model
   * @return a string representation of completed puzzle board
   */
  public static String stringifySolution(int width, int height, 
                                         List<List<ColumnName>> solution) {
    String[][] picture = new String[height][width];
    StringBuffer result = new StringBuffer();
    // for each piece placement...
    for(List<ColumnName> row: solution) {
      // go through to find which piece was placed
      Piece piece = null;
      for(ColumnName item: row) {
        if (item instanceof Piece) {
          piece = (Piece) item;
          break;
        }
      }
      // for each point where the piece was placed, mark it with the piece name
      for(ColumnName item: row) {
        if (item instanceof Point) {
          Point p = (Point) item;
          picture[p.y][p.x] = piece.getName();
        }
      }
    }
    // put the string together
    for(int y=0; y < picture.length; ++y) {
      for (int x=0; x < picture[y].length; ++x) {
        result.append(picture[y][x]);
      }
      result.append("\n");
    }
    return result.toString();
  }
  
  public enum SolutionCategory {UPPER_LEFT, MID_X, MID_Y, CENTER}
  
  /**
   * Find whether the solution has the x in the upper left quadrant, the
   * x-midline, the y-midline or in the center.
   * @param names the solution to check
   * @return the catagory of the solution
   */
  public SolutionCategory getCategory(List<List<ColumnName>> names) {
    Piece xPiece = null;
    // find the "x" piece
    for(Piece p: pieces) {
      if ("x".equals(p.name)) {
        xPiece = p;
        break;
      }
    }
    // find the row containing the "x"
    for(List<ColumnName> row: names) {
      if (row.contains(xPiece)) {
        // figure out where the "x" is located
        int low_x = width;
        int high_x = 0;
        int low_y = height;
        int high_y = 0;
        for(ColumnName col: row) {
          if (col instanceof Point) {
            int x = ((Point) col).x;
            int y = ((Point) col).y;
            if (x < low_x) {
              low_x = x;
            }
            if (x > high_x) {
              high_x = x;
            }
            if (y < low_y) {
              low_y = y;
            }
            if (y > high_y) {
              high_y = y;
            }
          }
        }
        boolean mid_x = (low_x + high_x == width - 1);
        boolean mid_y = (low_y + high_y == height - 1);
        if (mid_x && mid_y) {
          return SolutionCategory.CENTER;
        } else if (mid_x) {
          return SolutionCategory.MID_X;
        } else if (mid_y) {
          return SolutionCategory.MID_Y;
        }
        break;
      }
    }
    return SolutionCategory.UPPER_LEFT;
  }
  
  /**
   * A solution printer that just writes the solution to stdout.
   */
  private static class SolutionPrinter 
                       implements DancingLinks.SolutionAcceptor<ColumnName> {
    int width;
    int height;
    
    public SolutionPrinter(int width, int height) {
      this.width = width;
      this.height = height;
    }
    
    public void solution(List<List<ColumnName>> names) {
      System.out.println(stringifySolution(width, height, names));
    }
  }
  
  protected int width;
  protected int height;

  protected List<Piece> pieces = new ArrayList<Piece>();
  
  /**
   * Is the piece fixed under rotation?
   */
  protected static final int [] oneRotation = new int[]{0};
  
  /**
   * Is the piece identical if rotated 180 degrees?
   */
  protected static final int [] twoRotations = new int[]{0,1};
  
  /**
   * Are all 4 rotations unique?
   */
  protected static final int [] fourRotations = new int[]{0,1,2,3};
  
  /**
   * Fill in the pieces list.
   */
  protected void initializePieces() {
    pieces.add(new Piece("x", " x /xxx/ x ", false, oneRotation));
    pieces.add(new Piece("v", "x  /x  /xxx", false, fourRotations));
    pieces.add(new Piece("t", "xxx/ x / x ", false, fourRotations));
    pieces.add(new Piece("w", "  x/ xx/xx ", false, fourRotations));
    pieces.add(new Piece("u", "x x/xxx", false, fourRotations));
    pieces.add(new Piece("i", "xxxxx", false, twoRotations));
    pieces.add(new Piece("f", " xx/xx / x ", true, fourRotations));
    pieces.add(new Piece("p", "xx/xx/x ", true, fourRotations));
    pieces.add(new Piece("z", "xx / x / xx", true, twoRotations));
    pieces.add(new Piece("n", "xx  / xxx", true, fourRotations));
    pieces.add(new Piece("y", "  x /xxxx", true, fourRotations));
    pieces.add(new Piece("l", "   x/xxxx", true, fourRotations));
  }
  
  /**
   * Is the middle of piece on the upper/left side of the board with 
   * a given offset and size of the piece? This only checks in one
   * dimension.
   * @param offset the offset of the piece
   * @param shapeSize the size of the piece
   * @param board the size of the board
   * @return is it in the upper/left?
   */
  private static boolean isSide(int offset, int shapeSize, int board) {
    return 2*offset + shapeSize <= board;
  }
  
  /**
   * For a given piece, generate all of the potential placements and add them 
   * as rows to the model.
   * @param dancer the problem model
   * @param piece the piece we are trying to place
   * @param width the width of the board
   * @param height the height of the board
   * @param flip is the piece flipped over?
   * @param row a workspace the length of the each row in the table
   * @param upperLeft is the piece constrained to the upper left of the board?
   *        this is used on a single piece to eliminate most of the trivial
   *        roations of the solution.
   */
  private static void generateRows(DancingLinks dancer,
                                   Piece piece,
                                   int width,
                                   int height,
                                   boolean flip,
                                   boolean[] row,
                                   boolean upperLeft) {
    // for each rotation
    int[] rotations = piece.getRotations();
    for(int rotIndex = 0; rotIndex < rotations.length; ++rotIndex) {
      // get the shape
      boolean[][] shape = piece.getShape(flip, rotations[rotIndex]);
      // find all of the valid offsets
      for(int x=0; x < width; ++x) {
        for(int y=0; y < height; ++y) {
          if (y + shape.length <= height && x + shape[0].length <= width &&
              (!upperLeft || 
                  (isSide(x, shape[0].length, width) && 
                   isSide(y, shape.length, height)))) {
            // clear the columns related to the points on the board
            for(int idx=0; idx < width * height; ++idx) {
              row[idx] = false;
            }
            // mark the shape
            for(int subY=0; subY < shape.length; ++subY) {
              for(int subX=0; subX < shape[0].length; ++subX) {
                row[(y + subY) * width + x + subX] = shape[subY][subX];
              }
            }
            dancer.addRow(row);
          }         
        }
      }
    }
  }
  
  private DancingLinks<ColumnName> dancer = new DancingLinks<ColumnName>();
  private DancingLinks.SolutionAcceptor<ColumnName> printer;
  
  {
    initializePieces();
  }

  /**
   * Create the model for a given pentomino set of pieces and board size.
   * @param width the width of the board in squares
   * @param height the height of the board in squares
   */
  public Pentomino(int width, int height) {
    initialize(width, height);
  }

  /**
   * Create the object without initialization.
   */
  public Pentomino() {
  }

  void initialize(int width, int height) {
    this.width = width;
    this.height = height;
    for(int y=0; y < height; ++y) {
      for(int x=0; x < width; ++x) {
        dancer.addColumn(new Point(x,y));
      }
    }
    int pieceBase = dancer.getNumberColumns();
    for(Piece p: pieces) {
      dancer.addColumn(p);
    }
    boolean[] row = new boolean[dancer.getNumberColumns()];
    for(int idx = 0; idx < pieces.size(); ++idx) {
      Piece piece = (Piece) pieces.get(idx);
      row[idx + pieceBase] = true;
      generateRows(dancer, piece, width, height, false, row, idx == 0);
      if (piece.getFlippable()) {
        generateRows(dancer, piece, width, height, true, row, idx == 0);
      }
      row[idx + pieceBase] = false;
    }
    printer = new SolutionPrinter(width, height);
  }
  
  /**
   * Generate a list of prefixes to a given depth
   * @param depth the length of each prefix
   * @return a list of arrays of ints, which are potential prefixes
   */
  public List<int[]> getSplits(int depth) {
    return dancer.split(depth);
  }
  
  /**
   * Find all of the solutions that start with the given prefix. The printer
   * is given each solution as it is found.
   * @param split a list of row indexes that should be choosen for each row
   *        in order
   * @return the number of solutions found
   */
  public int solve(int[] split) {
    return dancer.solve(split, printer);
  }
  
  /**
   * Find all of the solutions to the puzzle.
   * @return the number of solutions found
   */
  public int solve() {
    return dancer.solve(printer);
  }
  
  /**
   * Set the printer for the puzzle.
   * @param printer A call-back object that is given each solution as it is 
   * found.
   */
  public void setPrinter(DancingLinks.SolutionAcceptor<ColumnName> printer) {
    this.printer = printer;
  }
  
  /**
   * Solve the 6x10 pentomino puzzle.
   */
  public static void main(String[] args) {
    int width = 6;
    int height = 10;
    Pentomino model = new Pentomino(width, height);
    List splits = model.getSplits(2);
    for(Iterator splitItr=splits.iterator(); splitItr.hasNext(); ) {
      int[] choices = (int[]) splitItr.next();
      System.out.print("split:");
      for(int i=0; i < choices.length; ++i) {
        System.out.print(" " + choices[i]);
      }
      System.out.println();
      
      System.out.println(model.solve(choices) + " solutions found.");
    }
  }

}
