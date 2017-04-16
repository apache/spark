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

/**
 * Of the "normal" 12 pentominos, 6 of them have distinct shapes when flipped.
 * This class includes both variants of the "flippable" shapes and the
 * unflippable shapes for a total of 18 pieces. Clearly, the boards must have
 * 18*5=90 boxes to hold all of the solutions.
 */
public class OneSidedPentomino extends Pentomino {

  public OneSidedPentomino() {}

  public OneSidedPentomino(int width, int height) {
    super(width, height);
  }

  /**
   * Define the one sided pieces. The flipped pieces have the same name with
   * a capital letter.
   */
  protected void initializePieces() {
    pieces.add(new Piece("x", " x /xxx/ x ", false, oneRotation));
    pieces.add(new Piece("v", "x  /x  /xxx", false, fourRotations));
    pieces.add(new Piece("t", "xxx/ x / x ", false, fourRotations));
    pieces.add(new Piece("w", "  x/ xx/xx ", false, fourRotations));
    pieces.add(new Piece("u", "x x/xxx", false, fourRotations));
    pieces.add(new Piece("i", "xxxxx", false, twoRotations));
    pieces.add(new Piece("f", " xx/xx / x ", false, fourRotations));
    pieces.add(new Piece("p", "xx/xx/x ", false, fourRotations));
    pieces.add(new Piece("z", "xx / x / xx", false, twoRotations));
    pieces.add(new Piece("n", "xx  / xxx", false, fourRotations));
    pieces.add(new Piece("y", "  x /xxxx", false, fourRotations));
    pieces.add(new Piece("l", "   x/xxxx", false, fourRotations));
    pieces.add(new Piece("F", "xx / xx/ x ", false, fourRotations));
    pieces.add(new Piece("P", "xx/xx/ x", false, fourRotations));
    pieces.add(new Piece("Z", " xx/ x /xx ", false, twoRotations));
    pieces.add(new Piece("N", "  xx/xxx ", false, fourRotations));
    pieces.add(new Piece("Y", " x  /xxxx", false, fourRotations));
    pieces.add(new Piece("L", "x   /xxxx", false, fourRotations));
  }
  
  /**
   * Solve the 3x30 puzzle.
   * @param args
   */
  public static void main(String[] args) {
    Pentomino model = new OneSidedPentomino(3, 30);
    int solutions = model.solve();
    System.out.println(solutions + " solutions found.");
  }

}
