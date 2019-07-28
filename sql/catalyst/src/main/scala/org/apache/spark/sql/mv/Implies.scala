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
package org.apache.spark.sql.mv

import org.apache.spark.sql.catalyst.expressions._

class Implies(filters: Seq[Expression], mvFilters: Seq[Expression]) {

  def implies: Boolean = {
    // val queryDNF = convertToDNF(filters).getOrElse(return false)
    // val mvDNF = convertToDNF(mvFilters).getOrElse(return false)
    val queryDNF = toDNF(foldUsingConjunct(filters)).getOrElse(return false)
    val mvDNF = toDNF(foldUsingConjunct(mvFilters)).getOrElse(return false)
    impliesDNF(queryDNF, mvDNF)
  }

  private def foldUsingConjunct(exps: Seq[Expression]): Expression = {
    if (exps.size == 1) {
      exps.head
    }
    else {
      exps.tail.fold(exps.head)((exp1: Expression, exp2: Expression) => And(exp1, exp2))
    }
  }

  private def toDNF(expression: Expression): Option[Seq[Seq[Expression]]] = {
    expression match {
      case Or(left, right) =>
        val leftDNF = toDNF(left).getOrElse(return Option.empty)
        val rightDNF = toDNF(right).getOrElse(return Option.empty)
        Some(leftDNF ++ rightDNF)
      case And(left, right) =>
        val leftDNF = toDNF(left).getOrElse(return Option.empty)
        val rightDNF = toDNF(right).getOrElse(return Option.empty)
        var result: Seq[Seq[Expression]] = Seq.empty
        leftDNF.foreach(disjunct1 => {
          rightDNF.foreach(disjunct2 => {
            result = result :+ (disjunct1 ++ disjunct2)
          })
        })
        Some(result)
      case _ =>
        if (isAtomic(expression)) {
          Some(Seq(Seq(expression)))
        }
        else {
          None
        }
    }
  }

  // Check if expression does not contain AND OR expression
  private def isAtomic(expression: Expression): Boolean = {
    expression match {
      case Or(_, _) | And(_, _) =>
        return false
      case _ =>
        return expression.children.forall(child => isAtomic(child))
    }
  }

  private def convertToDNF(exps: Seq[Expression]): Option[Seq[Seq[Expression]]] = {
    // [[a & b & c],[d & e]] => [[a & b & c & d & e]]
    if (exps.forall(isConjunct)) {
      Some(Seq(exps.flatMap(getConjunctComponents(_))))

      // [[a | b ], [c | d]] => [[a & c], [a & d], [b & c], [b & d]]
    } else if (exps.forall(isDisjunct)) {
      Some(DNF(exps.map(getDisjunctsComponents(_))))

      // [[(a | b) & (c | d)]] => [[a & c], [a & d], [b & c], [b & d]]
    } else if (exps.size == 1 && isCNFExpression(exps.head)) {
      Some(DNF(convertCNFExpToDNF(exps.head)))

      // [[(a & b) | (c)]] => [[a & b ], [c]]
    } else if (exps.size == 1 && isDNFExpression(exps.head)) {
      Some(convertDNFExpToDNF(exps.head))
    } else None
  }

  def convertCNFExpToDNF(exp: Expression): Seq[Seq[Expression]] = {
    getConjunctComponents(exp).map(getDisjunctsComponents)
  }

  def convertDNFExpToDNF(exp: Expression): Seq[Seq[Expression]] = {
    getDisjunctsComponents(exp).map(getConjunctComponents)
  }

  def isCNFExpression(exp: Expression): Boolean = {
    getConjunctComponents(exp).forall(isDisjunct)
  }

  def isDNFExpression(exp: Expression): Boolean = {
    getDisjunctsComponents(exp).forall(isConjunct)
  }

  def isConjunct(exp: Expression): Boolean = { exp match {
    case _ @ And(left, right) => isConjunct(left) & isConjunct(right)
    case _ @ Or(_, _) => false
    case _ => true
  }}

  def isDisjunct(exp: Expression): Boolean = {exp match {
    case _ @ Or(left, right) => isDisjunct(left) & isDisjunct(right)
    case _ @ And(_, _) => false
    case _ => true
  }}

  def getDisjunctsComponents(exp: Expression): Seq[Expression] = {exp match {
    case Or(left, right) => getDisjunctsComponents(left) ++ getDisjunctsComponents(right)
    case other => other :: Nil
  }}

  def getConjunctComponents(exp: Expression): Seq[Expression] = {exp match {
    case And(left, right) => getConjunctComponents(left) ++ getConjunctComponents(right)
    case other => other :: Nil
  }}

   /**
    * Implies 2 DNFs. Each DNFs is specified as Seq of Conjuncts
    * and every Conjunct is a Seq of atomic Expression
    */
  private def impliesDNF(dnf1: Seq[Seq[Expression]], dnf2: Seq[Seq[Expression]]): Boolean = {
    dnf1.forall(conjunct => {
      dnf2.exists(impliesConjunction(conjunct, _))
    })
  }

   /**
    * Converts CNF to DNF
    */
  private def DNF(disjuncts: Seq[Seq[Expression]]): Seq[Seq[Expression]] = {
    disjuncts match {
      case Nil => Seq(Nil)
      case _ =>
        disjuncts.head.flatMap(exp => {
          DNF(disjuncts.tail).map(Seq(exp) ++  _)
        })
    }
  }

   /**
    * Checks if conjunctions implication.
    * Note it can have false negatives but not false positives.
    *
    * @param conjunct1 first conjunction
    * @param conjunct2 second conjunction
    * @return true if conjunt1 implies conjunct2, false means doesn't know
    */
  private def impliesConjunction(conjunct1: Seq[Expression], conjunct2: Seq[Expression])
  : Boolean = {

    val prepare = checkAndFixOrder(_: Expression) match {
      case Some(bc) => bc
      case None => return false
    }

    // Make all expressions of conjunct as `Operand Operator Literal`. And fail if not possible.
    val orderedConjuncts = conjunct1.map(prepare)
    val mvOrderedConjuncts = conjunct2.map(prepare)

    val reducedConjuncts = groupAndReduceConjunct(orderedConjuncts).getOrElse(return false)
    val mvReducedConjucts = groupAndReduceConjunct(mvOrderedConjuncts).getOrElse(return false)

    // AttributeExpressionLiteral.implies(reducedConjuncts, mvReducedConjucts)
    // orderedConjuncts.forall(c1 => mvOrderedConjuncts.forall(c2 => impliesPredicate(c1, c2)))
    mvReducedConjucts.forall(mvRc => reducedConjuncts.exists(rc => impliesPredicate(rc, mvRc)))
  }

   /**
    * When a single conjunct1 in query has multiple expressions of same operand and operator,
    * we need to find the only relevant one. Eg: (a > 10 & b < 50 & a > 20) => (a > 20 & b < 50).
    * This will be helpful when comparing against some MV conjunct1.
    * Does not reduce [[GreaterThan]] and [[GreaterThanOrEqual]] to a sinle element currently.
    * Similar for [[LessThan]] and [[LessThanOrEqual]]
    */
  private def groupAndReduceConjunct(orderedConjuncts: Seq[Expression]): Option[Seq[Expression]] = {
    val expandedConjuncts = orderedConjuncts.map(_ match {
      case bc @ BinaryComparison(left: Attribute, right: Literal) =>
        AttributeExpressionLiteral(left, bc, Some(right))
      case ue: UnaryExpression =>
        AttributeExpressionLiteral(ue.child.asInstanceOf[Attribute], ue, None)
    })

    val groupedConjunct = expandedConjuncts.groupBy(_.same)

    val reducedConjuncts = groupedConjunct.map(x => {
      val attrExpLiterals = x._2

      attrExpLiterals.last.exp match {
        case _: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual =>
          attrExpLiterals.sortWith(impliesHelper _).head.exp

        case _: EqualTo => attrExpLiterals.map(x => x.literal.get).toSet.size match {
          case 1 => attrExpLiterals.last.exp
          case _ => return None
        }
        case _: IsNotNull | _: IsNull => attrExpLiterals.last.exp
        case _ => return None
      }
    }).toSeq
    Some(reducedConjuncts)
  }

  def impliesHelper(ael1: AttributeExpressionLiteral, ael2: AttributeExpressionLiteral): Boolean = {
    impliesPredicate(ael1.exp, ael2.exp)
  }

  def impliesPredicate(expression1: Expression, expression2: Expression): Boolean = {
    expression1 match {
      case bc@BinaryComparison(left: Attribute, right: Expression) =>
        impliesForBinaryComparision(bc, expression2, left, right)
      case _@IsNotNull(left : Expression) =>
        impliesForNotNullCheck(expression2, left)
      case _ => false
    }
  }

  private def impliesForNotNullCheck(expression2: Expression, left: Expression): Boolean = {
    expression2 match {
      case _@IsNotNull(child: Expression) =>
        return left == child
      case _ => return false
    }
  }

   /** This does not handle the following currently:
    * 1. query: a > 10, mv: a >= 5 , will return false
    * 2. query: a > 10, mv: a > 10 , will return false
    */
  private def impliesForBinaryComparision(bc: BinaryComparison, expression2: Expression,
                                          left: Attribute, right: Expression): Boolean = {
    val evaluate = PartialFunction[Expression, Boolean] {
      case b@BinaryComparison(l: Attribute, r: Expression) =>
        if (l != left) {
          return false
        } else {
          bc match {
            case _: GreaterThan | _: GreaterThanOrEqual =>
              b match {
                case _: GreaterThan | _: GreaterThanOrEqual =>
                  GreaterThan(right, r).eval(null).asInstanceOf[Boolean]
              }
            case _: LessThan | _: LessThanOrEqual =>
              b match {
                case _: LessThan | _: LessThanOrEqual =>
                  LessThan(right, r).eval(null).asInstanceOf[Boolean]
              }
            case _: EqualTo =>
              b match {
                case _: GreaterThan | _: GreaterThanOrEqual =>
                  GreaterThan(right, r).eval(null).asInstanceOf[Boolean]
                case _: LessThan | _: LessThanOrEqual =>
                  LessThan(right, r).eval(null).asInstanceOf[Boolean]
                case _: EqualTo =>
                  EqualTo(right, r).eval(null).asInstanceOf[Boolean]
              }
          }
        }
      case _@IsNotNull(n) => n == left
    }
    return evaluate.applyOrElse(expression2, (_ => false) : scala.Function1[Expression, Boolean])
  }

  /* 1. Convert the expression to format "AttributeReference Operator Literal"
     * 2. If not possible, implies is false.
     * 3. isNotNull isNull should also only have attributes
     */
  private def checkAndFixOrder(exp: Expression): Option[Expression] = {
    exp match {
      case bc @ BinaryComparison(left, right) =>
        (left, right) match {
          case (_: Attribute, _: Literal) => Some(bc)
          case (left: Literal, right: Attribute) =>
            bc match {
              case g: GreaterThan => Some(LessThan(right, left))
              case ge: GreaterThanOrEqual => Some(LessThanOrEqual(right, left))
              case l: LessThan => Some(GreaterThan(right, left))
              case le: LessThanOrEqual => Some(GreaterThanOrEqual(right, left))
              case eq: EqualTo => Some(EqualTo(right, left))
              case _ => None
            }
          case _ => None
        }
      case inn: IsNotNull => unaryExpWithAttributeReference(inn)
      case in: IsNull => unaryExpWithAttributeReference(in)
      case _ => None
    }
  }

  private def unaryExpWithAttributeReference[T <: UnaryExpression](ue: T): Option[Expression] = {
    ue.child match {
      case _: Attribute => Some(ue)
      case _ => None
    }
  }
}
