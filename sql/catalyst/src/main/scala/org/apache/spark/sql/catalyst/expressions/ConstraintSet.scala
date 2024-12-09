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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ConstraintSetImplicit._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{ConstraintHelper, LogicalPlan}
import org.apache.spark.sql.types.DataType


/**
 * This class stores the constraints available at each node.
 * The constraint expressions are stored in canonicalized form.
 * The major way in which it differs from the [[ExpressionSet]] is that
 * in case of Project Node, it stores information about the aliases
 * and groups them on the basis of equivalence. In stock spark all the
 * constraints are pre-created pessimistically for all possible combinations
 * of equivalent aliases. While in this class only one constraint per filter
 * is present.
 * The core logic of new algorithm is as follows
 * The base constraints present in this object will always be composed of
 * as far as possible, of those attributes which were present in the incoming
 * set & are also part of the output set for the node. If any attribute or expression
 * is part of any outgoing alias, they will be added to either attribute equivalence list
 * or expression equivalence list, depending upon whether the Alias's child is attribute
 * or generic expression. The 0th element of each of the buffer in the attribute equivalence
 * list & expression equivalence list are special in the sense, that any constraint,
 * if it is referring to any attribute or expression in the two lists, is guaranteed to
 * use the 0th element and not any other members. An attempt is made to ensure that the
 * constraint survives when bubbling up. If an attribute or expression, which is part
 * of incoming constraint, but is not present in output set, makes the survival of the
 * constraint susceptible. In such case, the attribute equivalence list & expression
 * equivalence list are consulted & if found that the buffer containing 0th element as
 * the attribute which is getting removed, has another element, then that element ( the
 * 1th member) is chosen to replace the attribute being removed  in the constraint.
 * The constraint is updated to use the 1th element. This 1th element is then put in
 * 0th place of the buffer.
 * It is to be noted that attribute equivalence list will have buffers where each
 * element will be of type attribute only. While expression equivalence list will
 * have 0th element as of type generic expression, rest being attributes.
 * It is also to be noted, that the 0th element of expression equivalence list being
 * generic expression, itself is composed of attributes. And the expression equivalence
 * list needs to be updated, if any of the attribute it refers to is being eliminated
 * from the output set.
 *
 * For eg. consider an existing constraint a + b + c + d > 7
 * let the input set comprise of attributes a, b, c, d
 * Let the output set be  a, a as a1, a as a2, b + c as z, d as d1
 * In the above d , b  & c are getting eliminated
 * while a survives and also has a1 & a2.
 * d is referred as d1.
 * the initial attribute equivalence list will be
 * a, a1, a2
 * d, d1
 * expression equivalence list will be
 * b + c , z
 * Now for the constraint a + b + c + d > 7 to survive
 * b + c => can be replaced by z
 * d can be replaced by d1
 * so the constraint will be updated as
 * a + z + d1 > 7
 * the updated attribute equivalence list will be
 * a, a1, a2
 * since d1 will be left alone, it will no longer be part of the list
 * same is the case with expression equivalence list.
 * as b + c, will be removed, only z1 remains, so it will be removed
 * from expression equivalence list & it will be empty.
 *
 * @param baseSet                        [[mutable.Set[Expression]] which contains Canonicalized
 *                                       Constraint Expression
 * @param originals                      [[mutable.Buffer[Expression]] buffer containing the
 *                                       original constraint
 *                                       expression
 * @param attribRefBasedEquivalenceList  A List of List which contains grouping of equivalent
 *                                       Aliases referring to same Attribute. The 0th attribute
 *                                       has special significance
 *                                       as the constraint expressions will be canonicalized in
 *                                       terms of that.
 * @param expressionBasedEquivalenceList A List of List which contains grouping of equivalent
 *                                       Aliases referring to same Expression( which is not an
 *                                       Attribute). The 0th expression is
 *                                       necessarily NOT an attribute ( but a compound expression
 *                                       containing 1 or more attributes).
 *                                       All the other elements (except the 0th) will necessarily
 *                                       be attributes. A constraint expression
 *                                       if  given in terms of the attribute(s) of this list,
 *                                       will be canonicalized to
 *                                       contain the 0th expression.
 *                                       For eg. if  this list has entries like  a + b, x, y, z
 *                                       and if the constraint expression to be added to
 *                                       constraintset looked like
 *                                       x > 5, then it would be actually converted and stored as
 *                                       a + b > 5
 */

class ConstraintSet private(
    baseSet: mutable.Set[Expression],
    originals: mutable.Buffer[Expression] = new mutable.ArrayBuffer(),
    val attribRefBasedEquivalenceList: Seq[mutable.Buffer[Attribute]],
    val expressionBasedEquivalenceList: Seq[mutable.Buffer[Expression]])
  extends ExpressionSet(baseSet, originals) with Logging with ConstraintHelper {

  import ConstraintSetImplicit._

  def this(actuals: mutable.Buffer[Expression]) = this(
    actuals.map(_.canonicalized).toMutableSet(mutable.Set),
    actuals,
    Seq.empty[mutable.Buffer[Attribute]],
    Seq.empty[mutable.Buffer[Expression]])

  def this(
      actuals: mutable.Buffer[Expression],
      attribRefBasedEquivalenceList: Seq[mutable.Buffer[Attribute]],
      expressionBasedEquivalenceList: Seq[mutable.Buffer[Expression]]) =
    this(
      actuals.map(_.canonicalized).toMutableSet(mutable.Set),
      actuals,
      attribRefBasedEquivalenceList,
      expressionBasedEquivalenceList)

  def this(baseSet: mutable.Set[Expression], actuals: mutable.Buffer[Expression]) =
    this(baseSet, actuals, Seq.empty[mutable.Buffer[Attribute]],
      Seq.empty[mutable.Buffer[Expression]])

  def this() = this(mutable.Buffer.empty[Expression])

  override def clone(): ConstraintSet = new ConstraintSet(
    baseSet.clone(),
    originals.clone(),
    this.attribRefBasedEquivalenceList.map(_.clone()),
    this.expressionBasedEquivalenceList.map(_.clone()))

  override def union(that: ExpressionSet): ExpressionSet = {
    def commonEquivList[T <: Expression](
        thisList: Seq[mutable.Buffer[T]],
        thatList: Seq[mutable.Buffer[T]]): Seq[mutable.Buffer[T]] = {
      val zerothElems = thisList.map(_.head)
      val (common, other) = thatList.partition(buff => zerothElems.exists(
        buff.head.canonicalized fastEquals _.canonicalized))
      val copy = thisList.map(_.clone())
      common.foreach(commonBuff => {
        val copyBuff = copy.find(_.head.canonicalized fastEquals commonBuff.head.canonicalized).get
        commonBuff.drop(1).foreach(expr => if (!copyBuff.exists(
          _.canonicalized fastEquals expr.canonicalized)) {
          copyBuff += expr
        })
      })
      copy ++ other.map(_.clone())
    }

    def removeAnyResidualDuplicateAttribute(newAttribList: Seq[mutable.Buffer[Attribute]]):
    (Seq[mutable.Buffer[Attribute]], Boolean) = {
      val allAttribs = newAttribList.flatten
      var foundEmptyBuff = false
      var foundDuplicates = false
      allAttribs.foreach(attrib => {
        val buffs = newAttribList.filter(buff => buff.exists(_.canonicalized fastEquals
                                                               attrib.canonicalized))
        if (buffs.size > 1) {
          foundDuplicates = true
          val rests = buffs.drop(1)
          rests.foreach(buff => {
            ConstraintSetHelper.removeCanonicalizedExpressionFromBuffer(buff, attrib)
            if (buff.isEmpty) {
              foundEmptyBuff = true
            }
          })
        }
      })
      (if (foundEmptyBuff) {
        newAttribList.filterNot(_.isEmpty)
      } else {
        newAttribList
      }) -> foundDuplicates
    }

    val (newAttribList, newExpEquivList) = that match {
      case thatX: ConstraintSet =>
        (commonEquivList(this.attribRefBasedEquivalenceList, thatX.attribRefBasedEquivalenceList),
          commonEquivList(this.expressionBasedEquivalenceList,
                          thatX.expressionBasedEquivalenceList))
      case _ => (this.attribRefBasedEquivalenceList.map(_.clone()),
        this.expressionBasedEquivalenceList.map(_.clone()))
    }
    // clean up new attribute list for any duplicate attribute refs if any
    val (cleanedAttribList, foundDuplicates) = removeAnyResidualDuplicateAttribute(newAttribList)
    if (foundDuplicates) {
      val errorMessage = "Found same attribute ref present in more than 1 buffers." +
        "This indicates either a faulty plan involving same dataframe reference self joined" +
        "without alias or something murkier"
      throwError(errorMessage)
    }
    val newSet = new ConstraintSet(this.baseSet.clone(), this.originals.clone(), cleanedAttribList,
                                   newExpEquivList)
    ConstraintSet.addFiltersToConstraintSet(that, newSet)
    newSet
  }

  override def constructNew(
      newBaseSet: mutable.Set[Expression] = new mutable.HashSet(),
      newOriginals: mutable.Buffer[Expression] = new ArrayBuffer()): ExpressionSet =
    new ConstraintSet(newBaseSet, newOriginals, this.attribRefBasedEquivalenceList.map(_.clone()),
      this.expressionBasedEquivalenceList.map(_.clone()))

  /**
   * Converts the given expression to the canonicalized form, needed for ConstraintSet
   * For eg lets assume expression equivalence list contains a buffer with following
   * entries. Below a & b are attributes of the output & z is an alias of a + b
   * a + b, z
   * If the expression to be added to constraints is z > 10, then it should be
   * entered in the constraintset  in terms of primary attributes (i.e a + b)
   * so on canonicalization z > 10 will be converted to a + b > 10
   *
   * @param ele Expression to be canonicalized
   * @return Expression which is canonicalized
   */
  override def convertToCanonicalizedIfRequired(ele: Expression): Expression =
    if (this.baseSet.contains(ele.canonicalized)) {
      ele
    } else {
      // Though it is guranteed that attrib equiv list will not be empty
      // but for precaution using headOption as indicated in feedback
      val suspectAttribs = ele.references --
        AttributeSet(this.attribRefBasedEquivalenceList.map(_.headOption.map(Seq(_))).
          flatMap(_.getOrElse(Seq.empty)))
      if (suspectAttribs.isEmpty) {
        ele
      } else {
        // the buff.size always has to be > 1, but this is just that curse of god does not
        // hit in prodn
        val mappings =
          AttributeMap(suspectAttribs.map(attrib =>
            (this.attribRefBasedEquivalenceList ++ this.expressionBasedEquivalenceList).find(buff =>
             buff.size > 1 && buff.slice(1, buff.length).exists(
             _.canonicalized fastEquals attrib.canonicalized)).map(
              buff => attrib -> Option(buff.head)).getOrElse(attrib -> None)).filter {
                case (_, Some(_)) => true
                case _ => false
              }.toSeq)
        if (mappings.nonEmpty) {
          ele.transformUp {
            case attr: Attribute if mappings.contains(attr) => mappings(attr).getOrElse(attr)
          }
        } else {
          ele
        }
      }
    }

  /**
   * This function updates the existing non redundant, non trivial constraints stored
   * as per the basis of incoming attributes and outgoing attributes of the node.
   * If the attributes forming the constraints are not going to be part of the output set,
   * then attempt is made ,as much as possible, to see if the constraint can survive
   * by modifying it with 1st available alias for the attribute getting removed.
   * It also tracks the aliases of the attribute which are then used
   * for pruning in the contains function
   *
   * @param outputAttribs                The attributes which are part of the output set
   * @param inputAttribs                 The attributes which make up the incoming attributes
   * @param projectList                  The list of projections containing the NamedExpression
   * @param oldAliasedConstraintsCreator A partial function used for generating all
   *                                     combination of constraints as per old code.
   *                                     Used only when the un optimized constraint propagation
   *                                     is used. Used in ExpressionSet
   * @return The new valid ConstraintSet
   */
  override def updateConstraints(
      outputAttribs: Seq[Attribute],
      inputAttribs: Seq[Attribute],
      projectList: Seq[NamedExpression],
      oldAliasedConstraintsCreator: Option[Seq[NamedExpression] => ExpressionSet]):
    ConstraintSet = {
    val (aliasBasedTemp, _) = projectList.partition {
      case _: Alias => true
      case _ => false
    }

    val groupHeadToGroupMap: ExpressionMap[mutable.Buffer[Expression]] =
      new ExpressionMap[mutable.Buffer[Expression]]()

    this.attribRefBasedEquivalenceList.foreach(
      x => groupHeadToGroupMap += (x.head -> x.map(_.asInstanceOf[Expression]).toMutableBuffer(
        mutable.Buffer))
                                               )
    this.expressionBasedEquivalenceList.foreach(x => groupHeadToGroupMap += (x.head -> x.clone()))

    // the aliases containing expressions might have been written in terms
    // of existing aliases. so we need to normalize them
    // for eg if we had an alias a + b + t as K, and already expression
    // based equivalence list contained c + d as t , we need to convert
    // a + b + t to a + b + c +d. optimally it should be done in update
    // of expression equiv list. But since c or d might be getting removed
    // and that code handles that, it is better to replace here itself so
    // that if c or d is getting removed, it will be tackled.

    val aliasBased = aliasBasedTemp.map {
      case al: Alias => val refs = al.child.references
        val replacementMap = AttributeMap(refs.flatMap(
          attrib => this.expressionBasedEquivalenceList.find(
            buff => buff.exists(_.canonicalized == attrib.canonicalized)).map(
            buff => Seq(attrib -> buff.head)).getOrElse(Seq.empty)).toSeq)
        if (replacementMap.nonEmpty) {
          al.copy(child = al.child transformUp {
            case attr: Attribute if replacementMap.contains(attr) => replacementMap(attr)
          })(al.exprId, al.qualifier, al.explicitMetadata, al.nonInheritableMetadataKeys)
        } else {
          al
        }
    }

    // clone the keys so that the set obtained is static & detached from
    // the groupHeadToGroupMap
    val existingAttribGroupHead = groupHeadToGroupMap.keySet.toSet

    // add the incoming attribs list
    aliasBased.foreach(ne => ne match {
      case al: Alias =>
        // find the group to which this alias's child belongs to
        // if the child is an attribute
        val alChild = al.child
        val key = this.attribRefBasedEquivalenceList.find(_.exists(
          _.canonicalized fastEquals alChild.canonicalized)).map(_.head).getOrElse(
          this.expressionBasedEquivalenceList.find(buff => buff.exists(
            _.canonicalized fastEquals alChild.canonicalized)).map(_.head).getOrElse(alChild))

        groupHeadToGroupMap.get(key) match {
          case Some(seq) => seq += al.toAttribute
          case None =>
            // if key is a literal that indicates that the Alias may have
            // an exprID which is somehow getting repeated across multiple nodes.
            // However the code below checks for presence of the alias's exprID
            // irrespective of the Alias's child is Literal or any general Expression.
            // (At this point cannot completely rule out repetition of non Literals Alias)
            // In such case even if the key is not in groupHead , the exprId might still be
            // in attribute reference list. So before creating a new entry in the map
            // check if the exprId of the alias is present in the attribute equivalence list.
            // If it is present already, skip its entry
            if (!this.attribRefBasedEquivalenceList.exists(
              buff => buff.exists(_.canonicalized == al.toAttribute.canonicalized))) {
              val temp: mutable.Buffer[Expression] = mutable.ArrayBuffer(al.child, al.toAttribute)
              groupHeadToGroupMap += al.child -> temp
            }
        }
      case _ => // not expected
    })

    // Find those incoming attributes which are not projecting out
    val attribsRemoved = AttributeSet(inputAttribs.filterNot(
      attr => outputAttribs.exists(_.canonicalized == attr.canonicalized)))
    // for each of the attribute getting removed , find replacement if any
    val replaceableAttributeMap: ExpressionMap[Attribute] = new ExpressionMap[Attribute]()
    fillReplacementOrClearGroupHeadForRemovedAttributes(
      attribsRemoved, replaceableAttributeMap, groupHeadToGroupMap)
    val (attribBasedEquivalenceList, initialExprBasedEquivalenceList) = {
      val (attribBased, expressionBased) = groupHeadToGroupMap.values.partition(
        buff => buff.head match {
          case _: Attribute => true
          case _ => false
        })
      (attribBased.map(buff => buff.map(_.asInstanceOf[Attribute])).toMutableBuffer(mutable.Buffer),
        expressionBased.toMutableBuffer(mutable.Buffer))
    }

    // now work on expression (other than attribute based)
    val replaceableExpressionMap: ExpressionMap[Attribute] = new ExpressionMap[Attribute]()
    val exprBasedEquivalenceList =
      getUpdatedExpressionEquivalenceListWithSideEffects(attribsRemoved, replaceableAttributeMap,
        replaceableExpressionMap, attribBasedEquivalenceList, initialExprBasedEquivalenceList)

    // Now update or remove the filters depending upon which
    // can survive based on replacement available
    val updatedFilterExprs = getUpdatedConstraints(
      attribsRemoved, replaceableAttributeMap, replaceableExpressionMap)

    exprBasedEquivalenceList.foreach(buffer => {
      val expr = buffer.head
      if (!existingAttribGroupHead.contains(expr.canonicalized)) {
        val newConstraintOpt = if (expr.references.isEmpty) {
          buffer.remove(0)
          expr match {
            case NonNullLiteral(_, _) | _: NullIntolerant => Some(EqualTo(buffer.head, expr))
            case _ => Some(EqualNullSafe(buffer.head, expr))
          }
        } else {
          None
        }

        newConstraintOpt.foreach(newConstraint => if (!updatedFilterExprs.exists(
          _.canonicalized fastEquals newConstraint.canonicalized)) {
          updatedFilterExprs += newConstraint
        })
      }
    })

    // remove all mappings of constants as the expression
    // also identify those elements which are plain attribute based only
    // so that we transfer them to attribute ref list.Even if we do not transfer
    // it would be fine. but better transfer so that bug  can be
    // bug tested easily
    val (attribsOnly, newExprBasedEquivalenceList) = exprBasedEquivalenceList.filter(
      buff => buff.head.references.nonEmpty && buff.size > 1).
      partition(_.head match {
                  case _: Attribute => true
                  case _ => false
                })

    // Now filter the attribBasedEquivalenceList which only has 1 element
    // This is because if there is only 1 element in the buffer, it cannot
    // be of any help in making a constraint survive, in case that attribute
    // is not part of output set, so no point in keeping it in the attribute
    // equivalence list.
    val newAttribBasedEquivalenceList = (attribBasedEquivalenceList ++
      attribsOnly.map(_.map(_.asInstanceOf[Attribute]))).filter(_.size > 1)
    val canonicalized = updatedFilterExprs.map(_.canonicalized).toMutableSet(mutable.Set)

    // Size not matching is an Error situation. This is Unexpected
    // The code below is just to handle unanticipated situation
    if (canonicalized.size != updatedFilterExprs.size) {
      this.logWarning(s"Canonicalized filter expression set" +
                        s" not matching with updated filter expressions, indicating duplicate " +
                        s"filters.")
      val duplicateFilters = mutable.ArrayBuffer[Seq[Expression]]()
      canonicalized.foreach(canon => {
        val tempExprs = updatedFilterExprs.filter(_.canonicalized fastEquals canon)
        if (tempExprs.size > 1) {
          duplicateFilters += tempExprs.toSeq
        }
      })
      val errorMessage = s"Found following duplicate filters." +
        s" Recording only first 4..." +
        s" ${duplicateFilters.flatten.take(4).map(_.toString).mkString(",")}"
      throwError(errorMessage)
      duplicateFilters.foreach(duplicates => {
        duplicates.drop(1).foreach(x => {
          val indx = updatedFilterExprs.indexWhere(_ fastEquals x)
          if (indx != -1) {
            updatedFilterExprs.remove(indx)
          }
        })
      })
    }
    new ConstraintSet(canonicalized, updatedFilterExprs, newAttribBasedEquivalenceList.toSeq,
                      newExprBasedEquivalenceList.toSeq)
  }

  private def getUpdatedExpressionEquivalenceListWithSideEffects(
      attribsRemoved: AttributeSet,
      replaceableAttributeMap: ExpressionMap[Attribute],
      replaceableExpressionMap: ExpressionMap[Attribute],
      attribBasedEquivalenceList: mutable.Buffer[mutable.Buffer[Attribute]],
      initialExprBasedEquivalenceList: mutable.Buffer[mutable.Buffer[Expression]]):
    mutable.Buffer[mutable.Buffer[Expression]] = {
    initialExprBasedEquivalenceList.map(buff => {
      val zerothElem = buff.head
      val refs = zerothElem.references
      if (refs.nonEmpty) {
        val removedAttribsReferedByExp = refs.intersect(attribsRemoved)
        if (!removedAttribsReferedByExp.isEmpty) {
          // if the replaceable attrib contains all the removed attrs referred, well & good
          // else the 0th expression needs to be removed as its non-surviving
          // attributes cannot be replaced by surviving attributes
          val replacementsExistsForAll = removedAttribsReferedByExp.forall(
            replaceableAttributeMap.contains(_))
          if (replacementsExistsForAll) {
            val newZeroth = zerothElem.transformUp {
              case attr: Attribute if replaceableAttributeMap.contains(attr) =>
                replaceableAttributeMap.get(attr).get
            }
            buff(0) = newZeroth
            buff
          } else {
            val removedExpression = buff.remove(0)
            if (buff.nonEmpty) {
              replaceableExpressionMap += (removedExpression -> buff.head
                .asInstanceOf[Attribute])
            }
            // If the buffer size after removal is > 1
            // transfer the remaining attributes in the buffer to attrib equivalent list
            // If the buffer size == 1, then it will be removed in the final filtration
            // as the buffer size == 1 implies that the 0th position expression cannot
            // survive up the chain, if any of the attribute it is referencing is lost,
            // as there is no alias to support it
            if (buff.size > 1) {
              val checkTrue = buff.forall {
                case _: Attribute => true
                case _ => false
              }
              if (!checkTrue) {
                val errorMessage = "Report Bug. ExpressionEquivalentList after removal of " +
                  "0th element still contains expression which are not of type attribute." +
                  s"the expressions are ${buff.map(_.toString).mkString(",")}"
                throwError(errorMessage)
                // remove expression other than attribute from the buffer
                var keepGoing = true
                while (keepGoing) {
                  val index = buff.indexWhere(x => x match {
                    case _: Attribute => false
                    case _ => true
                  })
                  if (index == -1) {
                    keepGoing = false
                  } else {
                    buff.remove(index)
                  }
                }
              }
              if (buff.nonEmpty) {
                val preexistingExprs = buff.filter(expr => attribBasedEquivalenceList.exists(
                  buffx => buffx.exists(_.canonicalized fastEquals expr.canonicalized)))
                preexistingExprs.foreach(expr => {
                  val index = buff.indexWhere(_.canonicalized fastEquals expr.canonicalized)
                  buff.remove(index)
                })
                if (buff.nonEmpty) {
                  attribBasedEquivalenceList += buff.map(_.asInstanceOf[Attribute])
                }
              }
              mutable.Buffer.empty[Expression]
            } else {
              buff
            }
          }
        } else {
          buff
        }
      } else {
        attribsRemoved.foreach(ConstraintSetHelper.removeCanonicalizedExpressionFromBuffer(buff, _))
        buff
      }
    }).filter(_.size > 1)
    // The above filtering ensures that if the buffer size after removal is 1,
    // then purge the buffer
  }

  private def getUpdatedConstraints(
      attribsRemoved: AttributeSet,
      replaceableAttributeMap: ExpressionMap[Attribute],
      replaceableExpressionMap: ExpressionMap[Attribute]): mutable.Buffer[Expression] = {
    this.originals.flatMap(filterExpr => {
      val attribRefs = filterExpr.references
      if (attribRefs.isEmpty) {
        Set.empty[Expression]
      } else {
        val attribsToHandle = attribRefs.intersect(attribsRemoved)
        if (!attribsToHandle.isEmpty) {
          var numReplacementExists = 0
          attribsToHandle.foreach(x => if (replaceableAttributeMap.contains(x)) {
            numReplacementExists += 1
          })
          if (numReplacementExists > 0) {
            val newConstraintExpr = filterExpr.transformUp {
              case attr: Attribute if replaceableAttributeMap.contains(attr) =>
                replaceableAttributeMap.get(attr).get
            }
            if (numReplacementExists == attribsToHandle.size) {
              Set(newConstraintExpr).filterNot(
                x => this.originals.exists(_.canonicalized == x.canonicalized))
            } else {
              // if filter still contains attribs which will be removed,
              // below code checks if filter can survive by replacement with a complex expression
              val attributesSeen = mutable.Buffer[Attribute]()
              val newerConstraintExpr = newConstraintExpr.transformDown {
                case attr: Attribute => attributesSeen += attr
                  attr
                case expr: Expression =>
                  replaceableExpressionMap.get(expr) match {
                    case Some(x) => x
                    case None => expr
                  }
              }
              if (attribsRemoved.intersect(AttributeSet(attributesSeen)).nonEmpty) {
                Set.empty[Expression]
              } else {
                Set(newerConstraintExpr).filterNot(
                  x => this.originals.exists(_.canonicalized == x.canonicalized))
              }
            }
          } else {
            if (replaceableExpressionMap.isEmpty) {
              Set.empty[Expression]
            } else {
              // if filter still contains attribs which will be removed,
              // below code checks if filter can survive by replacement with a complex expression
              val attributesSeen = mutable.Buffer[Attribute]()

              val newConstraintExpr = filterExpr.transformDown {
                case attr: Attribute => attributesSeen += attr
                  attr
                case expr: Expression =>
                  replaceableExpressionMap.get(expr) match {
                    case Some(x) => x
                    case None => expr
                  }
              }
              if (attribsRemoved.intersect(AttributeSet(attributesSeen)).nonEmpty) {
                Set.empty[Expression]
              } else {
                Set(newConstraintExpr).filterNot(
                  x => this.originals.exists(_.canonicalized == x.canonicalized))
              }
            }
          }
        } else {
          Set(filterExpr)
        }
      }
    })
  }

  private def fillReplacementOrClearGroupHeadForRemovedAttributes(
      attribsRemoved: AttributeSet,
      replaceableAttributeMap: ExpressionMap[Attribute],
      groupHeadToGroupMap: ExpressionMap[mutable.Buffer[Expression]]): Unit = {
    attribsRemoved.foreach(attrib => {
      groupHeadToGroupMap.get(attrib) match {
        case Some(buff) =>
          if (attrib.canonicalized == buff.head.canonicalized) {
            buff.remove(0)
          } else {
            val errorMessage = s"GroupHead =${attrib.toString} not" +
              s" matching with the buffer head = ${buff.head.toString}." +
              s"Not removing the head from the buffer"
            throwError(errorMessage)
          }

          // remove any attributes which may be in non zero position in buffer
          attribsRemoved.foreach(
            x => ConstraintSetHelper.removeCanonicalizedExpressionFromBuffer(buff, x))
          // if there is no replacement and the attribute being removed
          // was the only one present, then the buffer is purged
          // else replaced by updated key
          groupHeadToGroupMap.remove(attrib)
          if (buff.nonEmpty) {
            replaceableAttributeMap += attrib -> buff.head.asInstanceOf[Attribute]
            groupHeadToGroupMap.put(buff.head.asInstanceOf[Attribute], buff)
          }
        case None => // there may be attributes which are removed but lying in
          // position other than 0th
          // which may be such that zeroth attrib is not present in the list of attrib being
          // removed & hence escaped in above op. so we need to again filter the map
          // at this point it is guaranteed that once the filtering has happened , there will
          // be no buffer which can be empty

          val errorKeys = mutable.ArrayBuffer[Expression]()
          groupHeadToGroupMap.foreach {
            case (key, buffer) => val initialHead = buffer.head
              // The operation below should not touch the head of any buffer
              ConstraintSetHelper.removeCanonicalizedExpressionFromBuffer(buffer, attrib)
              // Buffer being empty or head being changed is an Error situation.
              // This is Unexpected.
              // The code below is just to handle unanticipated situation
              if (buffer.isEmpty || initialHead != buffer.head) {
                errorKeys += key
                this.logWarning(
                  s"for non GroupHead key attribute" +
                    s" ${attrib.toString}, It still modified the 0th position of " +
                    s"buffer with group head key $key. The initial head of buffer was" +
                    s" ${initialHead.toString}")
              }
          }
          if (errorKeys.nonEmpty) {
            val errorMessage = s"for non GroupHead key attribute" +
              s" ${attrib.toString}, found following modified group head keys." +
              s" ${errorKeys.map(_.toString).mkString(",")}"
            throwError(errorMessage)
            errorKeys.foreach(key => {
              val oldValOpt = groupHeadToGroupMap.remove(key)
              oldValOpt.foreach(buff => if (buff.nonEmpty) {
                groupHeadToGroupMap.put(buff.head, buff)
              })
            })
          }
      }
    })
  }

  /**
   * This is used by Union to merge the constraints from the two legs.
   * The expression based equivalence lists cannot be merged so it will be dropped
   *
   * @param filters               - Merged constraints from the two legs
   * @param attribEquivalenceList - Merged attrib based equivalence list from two legs
   */
  override def withNewConstraints(filters: ExpressionSet,
    attribEquivalenceList: Seq[mutable.Buffer[Attribute]]): ConstraintSet = {
    val newConstraintSet = new ConstraintSet(
      mutable.Buffer[Expression](), attribEquivalenceList, Seq.empty[mutable.Buffer[Expression]])
    ConstraintSet.addFiltersToConstraintSet(filters, newConstraintSet)
    newConstraintSet
  }


  override def attributesRewrite(mapping: AttributeMap[Attribute]): ConstraintSet = {
    val transformer: PartialFunction[Expression, Expression] = {
      case a: Attribute => mapping(a)
    }
    val newOriginals = this.originals.map(x => x.transformUp(transformer))
    val newAttribBasedEquiList = this.attribRefBasedEquivalenceList.map(
      buff => buff.map(mapping))
    val newExpBasedEquiList = this.expressionBasedEquivalenceList.map(
      buff => buff.map(_.transformUp(transformer)))
    val newConstraintSet = new ConstraintSet(
      mutable.Buffer[Expression](), newAttribBasedEquiList, newExpBasedEquiList)
    ConstraintSet.addFiltersToConstraintSet(newOriginals, newConstraintSet)
    newConstraintSet
  }

  /**
   * This function is used during pruning and also when any new condition is being
   * added to the constraintset. The idea is that existing conditions in the constraintset
   * are the bare minimum essential (non redundant) constraints. So any filter to be checked
   * if it can be pruned or not can be checked using this function, if that filter is
   * derivable using the constraints available. If it is derivable it means the filter is
   * redundant and can be pruned. Also if any new constraint is being added to the
   * constraintset that also can be checked if it is redundant or not. If redundant,
   * it will not get added. This method converts the incoming expression into its
   * constituents attributes before being checked.
   * For. eg if the incoming expression is say z + c > 10, where z is an alias of base
   * attributes a + b. And say constraintset already contains a condition
   * a + b + c > 10. Then z + c > 10, is converted into a + b + c > 10 making use
   * of tacking data of aliases, and it will be found in the constraintset &
   * contains will return as true
   *
   * @param elem Expression to be checked if it is redundant or not.
   * @return boolean true if it already exists in constraintset( is redundant)
   */
  override def contains(elem: Expression): Boolean = {
    if (super.contains(elem)) {
      true
    } else {
      // check canonicalized
      // find all attribs ref in all base expressions
      val baseAttribs = elem.references
      val checkList = this.attribRefBasedEquivalenceList.map(
        buff => buff.head -> buff.slice(1, buff.length)) ++
        this.expressionBasedEquivalenceList.map(buff => buff.head -> buff.slice(1, buff.length))
      // collect all the list of canonicalized attributes for these base attribs
      val substitutables = AttributeMap(
        baseAttribs.map(x => {
          val seqContainingAttrib = checkList.filter {
            case (_, buff) => buff.exists(_.canonicalized == x.canonicalized) }
          if (!(seqContainingAttrib.isEmpty || seqContainingAttrib.size == 1)) {
            val errorMessage = s"Attribute ${x.toString} found in more than 1 buffers"
            throwError(errorMessage)
          }
          if (seqContainingAttrib.nonEmpty) {
            x -> seqContainingAttrib.head._1
          } else {
            x -> null
          }
        }).filter { case (_, replacement) => replacement ne null }.toSeq)
      if (substitutables.nonEmpty) {
        val canonicalizedExp = elem.transformUp {
          case att: Attribute => substitutables.getOrElse(att, att)
        }
        super.contains(canonicalizedExp)
      } else {
        false
      }
    }
  }

  /**
   * This gives all the constraints whose references are subset of canonicalized
   * attributes of interests
   *
   * @param expressionsOfInterest A sequence of expression for which constraints are desired &
   *                              constraints should be such that its references are subset of
   *                              the canonicalized version of attributes in the passed sequence
   * @return Sequence of constraint expressions of compound types.
   */
  override def getConstraintsSubsetOfAttributes(expressionsOfInterest: Iterable[Expression]):
    Seq[Expression] = {
    val canonicalAttribsMapping =
      ExpressionMap(expressionsOfInterest.map(
        expr =>
          (this.attribRefBasedEquivalenceList ++ this.expressionBasedEquivalenceList).
            find(buff => buff.exists(_.canonicalized fastEquals expr.canonicalized)).map(
            buff => buff.head -> expr).getOrElse(expr -> expr)))
    val refsOfInterest = canonicalAttribsMapping.keySet.map(_.references).reduce(_ ++ _)
    this.originals.collect {
      case expr => val refs = expr.references
        if (refs.subsetOf(refsOfInterest) && refs.nonEmpty && expr.deterministic) {
          expr -> true
        } else expr -> false
    }.filter(_._2).toSeq.map(_._1.transformUp {
      case x => canonicalAttribsMapping.getOrElse(x, x)
    })
  }

  /**
   * Consider a new filter generated out of constraints of the form
   * IsNotNull(case....a....b...c) where the case expressions are
   * complex. Since new filters generated out of constraints are always canonicalized
   * it is possible that they are not compact as they are written in terms of
   * basic attributes. If an alias to this complex expression is present, then it
   * makes sense to rewrite the newly generated filter as IsNotNull(alias.attribute)
   * to avoid expensive calculation, especially that we have large case optimization.
   * This function simply tries to compact the expression where possible by replacing
   * the expression with an alias's attribute.
   *
   * @param expr Expression to compact ( decanonicalize)
   * @return Expression which is compacted, if possible
   */
  override def rewriteUsingAlias(expr: Expression): Expression =
    if (this.expressionBasedEquivalenceList.isEmpty) {
      expr
    } else {
      val canonicalized = convertToCanonicalizedIfRequired(expr)
      canonicalized.transformDown {
        case x: Attribute => x
        case x => getDecanonicalizedAttributeForExpression(x)
      }
    }


  /**
   * Decanonicalizes the NullIntolerant Expression.
   * The need for this arises, because when spark is attempting to generate
   * new NotNull constraints from the existing constraint, it may not return
   * any NotNull constraints, if the underlying subexpression is not of type
   * NullIntolerant.
   * Consider following two cases:
   * Lets say the base  canonicalized constraint is of the form a + b > 5.
   * A GreaterThan expression is NullIntolerant, so spark delves deep
   * and finds, it is composed of a & b attributes, & thus returns two
   * new IsNotNull constraints, namely IsNotNull(a) and IsNotNull(b).
   * But if the base canonicalized constraint is of the form
   * case(a....., b...) > 5, in this situation because case expression
   * does not implement NullIntolerant, spark does not go deep & hence
   * returns 0 not null constraints.
   * This function handles this situation, by replacing an underlying
   * canonicalized complex expression with an alias's attribute so
   * that NotNull constraint can be generated.
   * Thus case (a.....b) > 5 will be temporarily converted into z > 5.
   * Once an IsNotNull(z) is returned as a new constraint, we store
   * IsNotNull(z) in the Constraint set, again as canonicalized constraint,
   * that is IsNotNull(case...a...b), which will ensure that pruning logic
   * works fine.
   *
   * The logic to get a Filter expression with most NullIntolerant expressions
   * works this way
   * Our aim here was to get maxmimum number of null intolerant attributes. for that we were
   * traversing bottoms up , so that we get maximum attributes ( which otherwise would not be
   * possible from top normally).
   * But realized the following
   * Consider an expression A -> B -> C -> D -> E -> F
   * suppose
   * A is NullIntolerant
   * B is Null Intolerant
   * C is Not Null Intolerant
   * D, E , F are say Expressions which have attribute and can be NullIntolerant
   * Now the thing is that if we traverse from bottom to top, we are able to replace D , E F as
   * NullIntolerant attribute.
   * But because C is not Null Intolerant , so if we are unable to find replacement of C , then D
   * E F even if they have corresponding attributes for replacement, they are useless.
   *
   * So if I traverse from top to bottom, the moment I find C , I do not want to traverse further
   * down from C, unless I can find a replacement for C.
   * So the ExpressionHider wraps C & ExpressionHider masquerades as Leaf expression , which
   * prevents traversal down.
   * Then we see that if ExpressionHider is present, then just canonicalize from C , and then
   * decanonicalize only from point C, and replace the ExpressionHider with the new
   * decanonicalized C.
   * else just replace it with C, if nothing can be done.
   *
   * @return Set of constraint expressions where underlying NullIntolerant
   *         Expressions have been decanonicalized.
   */
  override def getConstraintsWithDecanonicalizedNullIntolerant: ExpressionSet = {
    if (this.expressionBasedEquivalenceList.isEmpty) {
      this
    } else {
      ExpressionSet(this.originals.map(expr => expr match {
        case _: NullIntolerant =>
          var hasHidden = false
          val temp = expr.transformDown {
            case x: NullIntolerant => x
            case x =>
              // check for constants/literals etc, they may be under cast
              val hasAttribDependency = x.references.nonEmpty
              if (hasAttribDependency) {
                hasHidden = true
                ExpressionHider(x)
              } else {
                x
              }
          }
          if (hasHidden) {
            val modified = temp.transformUp {
              case ExpressionHider(hiddenChild) =>
                // hiddenChild is already canonicalzed
                val y = getDecanonicalizedAttributeForExpression(hiddenChild)
                if (y ne hiddenChild) {
                  y
                } else {
                  ExpressionHider(hiddenChild)
                }
              case x => // check if any of the child is still hidden
                val hiddenChildIndexes = x.children.map(
                  _ match {
                    case _: ExpressionHider => true
                    case _ => false
                  })
                // if any of the child are hidden then only
                // look for decanonicalization of current
                if (hiddenChildIndexes.exists(b => b)) {
                  val newChildren = x.children.zipWithIndex.map {
                    case (child, i) => if (hiddenChildIndexes(i)) {
                      child.asInstanceOf[ExpressionHider].hiddenChild
                    } else {
                      convertToCanonicalizedIfRequired(child)
                    }
                  }
                  val fullCanonicalized = x.withNewChildren(newChildren)
                  getDecanonicalizedAttributeForExpression(fullCanonicalized)
                } else {
                  x
                }
            }
            modified
          } else {
            expr
          }
        case _ => expr
      }))
    }
  }

  override def getAttribEquivalenceList: Seq[mutable.Buffer[Attribute]] =
    this.attribRefBasedEquivalenceList

  private def getDecanonicalizedAttributeForExpression(canonicalizedExpr: Expression):
    Expression = {
    val bufferIndex = this.expressionBasedEquivalenceList.indexWhere(
      buff => buff.head.references.equals(canonicalizedExpr.references) &&
        buff.head.fastEquals(canonicalizedExpr))
    if (bufferIndex != -1) {
      val buff = this.expressionBasedEquivalenceList(bufferIndex)
      // buffer size will always be >= 2
      // due to the decanonicalization being used in withUnion code
      // to handle multi ref constraints , with equivalent expr list being lost,
      // the attribute for decanonicalization has to be the first encountered attribute,
      // as that attribute will be transferred to the attribute equivalence list
      // the buff size is guaranteed to be >= 2
      // but just so as not cause unexpected production issue
      if (buff.size < 2) {
        canonicalizedExpr
      } else {
        buff(1)
      }
    } else {
      canonicalizedExpr
    }
  }

  private def throwError(errorMessage: String): Unit = {
    // scalastyle:off throwerror
    throw new AssertionError(errorMessage)
    // scalastyle:on throwerror
  }
}

object ConstraintSet extends ConstraintHelper {
  private def addFiltersToConstraintSet(filters: Iterable[Expression],
    constraintSet: ConstraintSet): Unit = {
    filters.foreach(expr => {
      val conditionedElement = constraintSet.convertToCanonicalizedIfRequired(expr)
      constraintSet.add(conditionedElement)
    })
  }

  /**
   * Aim 1) To find common expressions across two legs of union
   * 2) To find all expressions which are exactly having 1 ref & are
   * not common , for each Leg
   * Union = {
   * Leg1
   * select a, a as a1,a as a2,a as a3,a as a4,a as a5,a as a6,
   * b, b as b1, b as b2, b as b3 from tab
   *
   * Leg2
   * select a, a as a1, a2 ,a2 as a3, a4, a4 as a5, a6,
   * b , b as b1, b2, b2 as b3 from tab2
   *
   * }
   *
   * Leg1 =>
   * a----a1----a2----a3---a4---a5---a6
   * b----b1-----b2----b3
   * filter a > 10
   *
   * Leg2
   * a---a1
   * a2---a3
   * a4---a5
   * a6
   * b----b1
   * b2---b3
   * filter a2 > 10
   *
   * Common groups. :  obtained by crossing all the lists
   * we also keep track of the respective heads of attrib equiv list
   * which were used to obtain these mappings. As the actual expressions would
   * be composed only of these variables. so basically we are finding what are
   * the common elements for given two attrib equiv list. i.e
   * If Leg1_0th_attribute is that attribute in terms of which constraint
   * expressions are canonicalized on leg1.
   * and Leg2_0th_attribute is that attribute in terms of which constraint
   * expressions are canonicalized on leg2
   * then
   * (Leg1 0th attribute, Leg2 0th attr) -> union output equivalence list.
   * (a, a)   -> a---a1
   * (a, a2)  -> a2---a3
   * (a, a4)  ->  a4---a5
   * (a, a6) -> a6
   * (b, b) -> b---b1
   * (b, b2) -> b2---b3
   *
   * Output of union = select a , a as a1, a2, a2 as a3, a2 as a4, a5, a5 as a6, a5 as a7 ,
   * b , b as b1 , b2, b2 as b3
   *
   * We want to partition each Leg's attribute equiv list into parts such that
   * each part will/can be part of Union output node's attribute equiv list
   * Processing of Union Leg1
   * Get all the references across all the constraints in Leg1.
   * They can only be composed of  a, b
   * Processing of Union Leg2
   * Get all the references across all the constraints in Leg1.
   * They can only be composed of  a, a2, a4, a6, b, b2
   * Partition each Leg's attribute eqiv list so that it is broken down
   * in terms of common output attribute equiv list and rest.
   * Now Group constraints of each Leg based on structural similarity
   * for eg a + b > 5  & a2 + b2 > 5 are structurally similar.
   * To identify common constraints across two legs, One required condition
   * is their structural similarity
   * This is done by replacing all the attributes across each leg , in terms of
   * dummy attribute's which represent each of the data type of attribute
   * occurring in tree.
   * This will result in some thing like
   * Leg1                Leg2
   * condition1              condition1'
   * condition2     key1     condition2'
   * condition3
   *
   * Leg1                    Leg2
   * condition4              condition4'
   * condition5     key2     condition5'
   * any common condition can be only within each group
   * Then we pick each condition in Leg1, collect sequential attribute list(i.e attributes
   * collected in a  deterministic order from the expressions)
   * Similarly we check with each of the condition in Leg2, collect sequential attribute list
   * For 1 : 1 attribute Seq, we check the common group mapping.
   * If there is an entry available for each pair of refs, we have a solution , given
   * by the 0th element of the common list
   *
   * @param headConstraint      The ConstraintSet of the first Leg say Leg1
   * @param otherConstraintNode The Logical Plan for the second(other) Union Leg, say Leg2
   * @param reference           The output attributes of Union which are represented in terms of
   *                            Leg1
   * @return ConstraintSet for the union
   */
  def unionWith(headConstraint: ConstraintSet, otherConstraintNode: LogicalPlan,
      reference: Seq[Attribute]): ConstraintSet = {
    val otherConstraint = otherConstraintNode.constraints.asInstanceOf[ConstraintSet]
    val nodeOutput = otherConstraintNode.output
    require(nodeOutput.size == reference.size)
    val attributeRewrites = AttributeMap[Attribute](nodeOutput.zip(reference))

    // convert leg2 constraints in terms of Leg1 attributes
    val preparedConstraint2 = prepareConstraintForUnion(otherConstraint, Some(attributeRewrites))
    val preparedConstraint1 = prepareConstraintForUnion(headConstraint, None)
    val leg1RefsOfInterest = preparedConstraint1.originals.map(_.references).
      foldLeft(AttributeSet.empty)(_ ++ _)
    val leg2RefsOfInterest = preparedConstraint2.originals.map(_.references).
      foldLeft(AttributeSet.empty)(_ ++ _)
    // augment the equivalence list by adding stand alone refs too, which are part
    // of constraints, so that common attribs for each ref which is not part of
    // output union equivalence are also correctly identified
    val allRefsOfInterest = leg1RefsOfInterest ++ leg2RefsOfInterest
    /*
    Standalone attributes are those attributes for which there is no presence in attrib equiv
    list. Pls note that attrib equiv list may contain those refs, too, for which there are no
    constraints defined( in either of the legs) but we need those to be part of output attrib
    equiv list.
    However those attributes in a leg, for which there may be constraints such that constraint
    is function of only that attribute and there are no aliases for that attribute, then that
    attribute is a standalone attribute.
    AND the other attribs for which there are no constraints present, and also DO NOT have any
      aliases are also STAND ALONE attributes.
    Theoretically I can add all those attributes to generate mappings. But they will bloat the Map.
    So instead I only add those to Mapping which are StandAlone and have presence in constraints
    of either Leg1 ore Leg2. that helps in reducing the bloat.

    It would not be wrong to add only those Standalone attribs in Leg1, for which there are
    constraints only on Leg1.

    But for the future possibility of enhancing the Union constraint, it would be better to add
    any refs encountered in either Leg1 or Leg2 constraint, and has no aliases, as stand alone
    attribute.
   */
    val standAloneRefsOfLeg1 = allRefsOfInterest.toSeq.diff(
      preparedConstraint1.attribRefBasedEquivalenceList.
        foldLeft(Seq.empty[Attribute])(_ ++ _)).map(Seq(_))
    val standAloneRefsOfLeg2 = allRefsOfInterest.toSeq.diff(
      preparedConstraint2.attribRefBasedEquivalenceList.
        foldLeft(Seq.empty[Attribute])(_ ++ _)).map(Seq(_))
    // identify the  attribute equivalence lists for the Union node
    // this has the alias relationships which is same across both the legs
    val commonAttribListMapping = mergeEquivalenceList(
      preparedConstraint1.attribRefBasedEquivalenceList.map(_.toSeq) ++ standAloneRefsOfLeg1,
      preparedConstraint2.attribRefBasedEquivalenceList.map(_.toSeq) ++ standAloneRefsOfLeg2)

    // group structurally similar constraints on each side
    // for this create a new exprID which replaces all attributes
    val templateAttributeGenerator = new TemplateAttributeGenerator()

    val templatizedConstraintsMapLeg1 = templatizedConstraints(templateAttributeGenerator,
      preparedConstraint1.originals.toSeq)
    val templatizedConstraintsMapLeg2 = templatizedConstraints(templateAttributeGenerator,
      preparedConstraint2.originals.toSeq)

    val netCommonSols = generateCommonSolutions(
      templatizedConstraintsMapLeg1,
      templatizedConstraintsMapLeg2,
      commonAttribListMapping.map {
        case (attrs, attribBuff) => attrs -> attribBuff.toSeq
      })
    val commonAttribList = commonAttribListMapping.values

    val decomposedAttribEquivListsOfLeg1 = calculateDecomposedAttribEquivLists(
      leg1RefsOfInterest, preparedConstraint1.attribRefBasedEquivalenceList, commonAttribList)

    val decomposedAttribEquivListsOfLeg2 = calculateDecomposedAttribEquivLists(
      leg2RefsOfInterest, preparedConstraint2.attribRefBasedEquivalenceList, commonAttribList)

    // generator for single refs expression in terms of solution space
    val singleRefExprGenerator: (ConstraintSet, AttributeMap[Set[Attribute]]) => ExpressionSet =
      (constraintSet, solutionSpace) => {
      val singleRefs = constraintSet.originals.filter(_.references.size == 1)
      ExpressionSet(singleRefs.flatMap(expr => {
        val attr = expr.references.head
        solutionSpace.get(attr).map(atts => atts.map(att => expr transformDown {
          case _: Attribute => att
        })).getOrElse(Seq(expr))
      }))
    }

    val singleRefExprsLeg1 = singleRefExprGenerator(
      preparedConstraint1, decomposedAttribEquivListsOfLeg1)

    val singleRefExprsLeg2 = singleRefExprGenerator(
      preparedConstraint2, decomposedAttribEquivListsOfLeg2)
    val other1 = singleRefExprsLeg1.diff(netCommonSols).groupBy(_.references.head)
    val other2 = singleRefExprsLeg2.diff(netCommonSols).groupBy(_.references.head)
    // loose the constraints by: A1 && B1 || A2 && B2  ->  (A1 || A2) && (B1 || B2)
    val others = (other1.keySet intersect other2.keySet).map {
      attr => Or(other1(attr).reduceLeft(And), other2(attr).reduceLeft(And))
    }

    headConstraint.withNewConstraints(
      netCommonSols ++ others, commonAttribList.filterNot(_.size < 2).toSeq)
  }

  /**
   * Prepares the Constraint for the union. This involves creating a new constraint
   * object which satisfies the below requirements.
   * If attributeRewriteOpt is defined, then it rewrites the constraintset in terms of
   * mappings provided. It also decanonicalizes multi ref constraints into decanonicalized
   * attribute so as to reduce the number of references used in the filter.
   * Transfers the attributes contained in the expression equivalence list to attribute
   * equivalence list.
   *
   * @param constraintSet       Existing ConstraintSet
   * @param attributeRewriteOpt The re-write attributes mapping
   * @return ConstraintSet
   */
  private def prepareConstraintForUnion(constraintSet: ConstraintSet,
    attributeRewriteOpt: Option[AttributeMap[Attribute]]): ConstraintSet = {
    val rewriter = (expr: Expression, attributeRewrites: AttributeMap[Attribute]) =>
      expr transformDown {
        case attr: Attribute => attributeRewrites(attr)
      }

    // first find all original filters which are multi ref and see which can be decanonicalized
    val (multiRefs, singleRefs) = constraintSet.originals.partition(_.references.size > 1)
    val finalConstraints = if (multiRefs.nonEmpty) {
      // convert any multi ref constraints to decanonicalized form
      val modifiedMultiRefs = multiRefs.map(constraintSet.rewriteUsingAlias)
      val newConditions = ExpressionSet(modifiedMultiRefs).
        diff(ExpressionSet(multiRefs))
      val anyNewNotNulls = newConditions.flatMap(inferIsNotNullConstraints(_))
      singleRefs ++ anyNewNotNulls ++ modifiedMultiRefs
    } else {
      constraintSet.originals
    }
    // transfer the attributes of expression equiv list to attribute equivlist
    val newAttribEquivList = constraintSet.attribRefBasedEquivalenceList ++
      constraintSet.expressionBasedEquivalenceList.filter(_.size > 2).
        map(_.clone().drop(1).map(_.asInstanceOf[Attribute]))
    // convert leg2 constraints in terms of attributes names used in leg1
    val (attribEquivList, constraintsList) = attributeRewriteOpt.map(rewriteMap => {
      newAttribEquivList.map(buff => buff.map(rewriteMap)) ->
        finalConstraints.map(rewriter(_, rewriteMap))
    }).getOrElse((newAttribEquivList, finalConstraints))
    new ConstraintSet(constraintsList, attribEquivList, Seq.empty)
  }

  /**
   * This is used by Union to merge the attribute based equivalence lists of its two legs.
   * An attribute equivalency can only be added to the Union's equivalency list if it is true
   * on both sides of the Union. Before going into this function, it is assumed that the
   * equivalency lists have been rewritten to use the Union's output attributes.
   * The algorithm to determine this is as follows.
   * Cross multiply all the attribute equivalence list and identify all the intersection
   * possible across all the combinations
   * The key of the Map represents the canonicalized attribute ref of Leg1 & leg2
   * equivalence lists , from which the common set was derived.
   * for eg if a --- a1-----a2-----a3-----a4  is leg 1 equiv list
   * and       a3----a4    is leg2 attrib equivalence list , then common output list a3---a4
   * is obtained from the (a, a3) where a is the original attrib equiv 0th element
   * of leg1 attrib equiv list and a3 is the 0th element of Leg2 attrib equiv list.
   * this essentially means that if a filter expression on leg1 involved a
   * and filter expression on leg2 involved a3, then for the union output perspective
   * the common solution will be a3--a4
   *
   * @param leg1EquivList one of attrib equivalence list to merge
   * @param leg2EquivList the other attrib equivalence list to merge
   * @return A Map containing the mappings of the attributes forming the expression
   *         and the attributes which are aliases to the attributes in the mapping
   */
  private def mergeEquivalenceList(leg1EquivList: Seq[Seq[Attribute]],
    leg2EquivList: Seq[Seq[Attribute]]): Map[(Attribute, Attribute), mutable.Buffer[Attribute]] = {
    var newAttribRefBasedEquivalenceList =
      Seq.empty[((Attribute, Attribute), mutable.Buffer[Attribute])]

    leg1EquivList.foreach { equivalenceList =>
      leg2EquivList.foreach { otherEquivalenceList =>
        val _0thKeyLeg1 = equivalenceList.head.canonicalized.asInstanceOf[Attribute]
        val commonAttribs = equivalenceList.map(_.canonicalized).intersect(
          otherEquivalenceList.map(_.canonicalized)).map(
          canonicalizedAttrib => equivalenceList.find(
            _.exprId == canonicalizedAttrib.asInstanceOf[Attribute].exprId).get).toBuffer
        if (commonAttribs.nonEmpty) {
          val _0thKeyLeg2 = otherEquivalenceList.head.canonicalized.asInstanceOf[Attribute]
          newAttribRefBasedEquivalenceList = ((_0thKeyLeg1, _0thKeyLeg2) -> commonAttribs) +:
            newAttribRefBasedEquivalenceList
        }
      }
    }
    newAttribRefBasedEquivalenceList.toMap
  }

  /**
   * templatize the constraint so that structurally similar constraints are grouped under
   * the same templatized constraint key. This will help in reducing the space of constraints
   * between two legs, in which we have to find the identical common constraints.
   * So if there are 2 constraints in a leg
   * a + b + c > 10     &&  d + e + f > 10 where (a & d) are int , (b & e) are double
   * and (c & f) are int,
   * they will be transformed to same templatized expression like
   * x1 + Y1 + x2 > 10  where X are of types Int, Y are of types double
   *
   * The return type is an ExpressionMap where key is the templatized expression and
   * value is a 2D Array of Attributes.
   * Each Row of 2D array represents the actual constraint when combined with the templatized
   * constraint  expression key in the ExpressionMap.
   * The elements contained in the 1D Array  of each row are the attribute
   * references present in the actual constraint expression.
   * for eg, in the above case ,the key = x1 + Y1 + x2 > 10 , will have a 2D array of Attribute refs
   * where Row 1 will have refs of expression1 & Row2 of expression2
   * i.e the value would be
   * a, b, c
   * d, e, f
   * Note: This function is made public for testing purposes.
   *
   * @param templateAttributeGenerator The instance of {{TemplateAttributeGenerator}} which
   *                                   maintains the state so as to ensure that Attributes for
   *                                   templatization are returned correctly and in order
   * @param expressions                The Sequence of Constraints which are to be templatized
   * @return an instance of ExpressionMap which stores key as the templatized constraint expression
   *         and value a 2D array of attributes representing attributes found in each constraint
   */
  def templatizedConstraints(templateAttributeGenerator: TemplateAttributeGenerator,
    expressions: Seq[Expression]): Map[Expression, Array[Array[Attribute]]] = {
    val templatizedExprsToSolutionSpace = expressions.map(origExpr => {
      templateAttributeGenerator.reset()
      // contains the refs comprising each of the constraint as collected
      // when transforming the constraint to the templatized form
      val valueList = mutable.ArrayBuffer[Attribute]()
      val templatized = origExpr.transformDown {
        case attr: AttributeReference => valueList += attr
          templateAttributeGenerator.getNext(attr.dataType)
      }
      templatized -> valueList.toArray
    })
    val groupedData = templatizedExprsToSolutionSpace.groupBy(_._1.canonicalized).map {
      case (_, keyVals) => keyVals.head._1 -> keyVals.map(_._2).toArray
    }
    Map(groupedData.toSeq: _*)
  }

  /**
   * Consider the following example
   * For a given leg let the constraint be a > 5.
   * so the attrib of interest is 'a'
   * let the attrib equiv list be
   * a , a1, a2, a3, a4 , a5 , a6, a7
   *
   * let the common output attrib equiv lists based on the leg2 data is such that
   * a1--a2
   * a5 -- a6
   *
   * so this function will break the attrib equiv list of leg1 into
   * a, a1, a3, a4, a5, a7
   * and return a AttributeMap which will look like
   * a -> Set (a, a1, a3, a4, a5, a7)
   *
   * @param refsOfInterest                Atributes which are present in the constraint expressions
   * @param attribRefBasedEquivalenceList the input attribute equiv list which needs
   *                                      to be decomposed
   * @param commonAttribList              The output attribute equivalence list
   * @return AttributeMap containing a Set of Attributes which are created
   *         after breaking the incoming attribute equivalence list into
   *         parts which are present in common AttribList and rest as
   *         stand alone element
   */
  private def calculateDecomposedAttribEquivLists(
    refsOfInterest: AttributeSet,
    attribRefBasedEquivalenceList: Seq[mutable .Buffer[Attribute]],
    commonAttribList: Iterable[mutable.Buffer[Attribute]]): AttributeMap[Set[Attribute]] = {
    val totalRefsOfInterest = AttributeSet(
      refsOfInterest.toSeq ++ attribRefBasedEquivalenceList.map(_.head))
    AttributeMap(totalRefsOfInterest.map(attr => {
      val equivListOpt = attribRefBasedEquivalenceList.find(
        _.head.canonicalized == attr.canonicalized)
      val sol = equivListOpt.map(completeList => {
        var currentList = completeList
        val subspaces = (for (common <- commonAttribList) yield {
          if (currentList.exists(_.canonicalized == common.head.canonicalized)) {
            currentList = currentList.diff(common)
            Seq(common.head)
          } else {
            Seq.empty
          }
        }).flatten
        (subspaces ++ currentList).toSet
      }).getOrElse(Set[Attribute](attr))
      attr -> sol
    }).toSeq)
  }

  /**
   * For each common templatized expression key in the two maps,
   * do a cross of two Array(Array(Attribute)) of each side
   * i.e if 2dArr1 = {
   * {a, b, c}
   * {d, e, f}
   * }
   * if 2dArr2 = {
   * {x, y, z}
   * {p, q, r}
   * }
   * so we take each row of either side
   * and zip it  such that we get pairs of say
   * (a,x), (b,y) , (c,f)
   * each pair is looked into the Map commonAttribListMapping
   * If there exists an entry in the Map for each of the pair
   * we have a valid solution which is created by taking the 0th
   * element of the common attrib list.
   * The templatized expression is then regenerated using the solution
   *
   * @param templatizedConstraintsMapLeg1 Map containing templatized expression key
   *                                      & 2d array containg the actual expression
   *                                      bindings
   * @param templatizedConstraintsMapLeg2 Map containg templatized expression key
   *                                      & 2d array containg the actual expression
   *                                      bindings
   * @param commonAttribListMapping       The mapping of individual attrib equivalence list's 0th
   *                                      element
   *                                      of either leg to the common solution
   * @return ExpressionSet containing common filters for the union output
   */
  private def generateCommonSolutions(
      templatizedConstraintsMapLeg1: Map[Expression, Array[Array[Attribute]]],
      templatizedConstraintsMapLeg2: Map[Expression, Array[Array[Attribute]]],
      commonAttribListMapping: Map[(Attribute, Attribute), Seq[Attribute]]): ExpressionSet = {
    val results = (for ((templatizedExpr1, bindings1) <- templatizedConstraintsMapLeg1) yield {
      // For each templatized constraint on leg1 check if there exists an equivalent
      // templatized constraint on leg2. If there is , then we need to find common
      // solution by checking the bindings of each side.
      // The way we do is that we take a cross of the 2d Array, comparing each 1d Row Array
      // of leg1, with every other 1dRowArray of leg2.
      // Between two 1d Array for comparison, we take pair of attribute from each
      // side of the 1d Array, paired using the index position, and see the Mappings
      // if the pair exists as key in the map. If each pair has a valid mapping, then
      // we have a solution and we use the valid solution for each attribute as replacement
      // value in templatized constraint, to get the valid constraint
      templatizedConstraintsMapLeg2.get(templatizedExpr1).map(bindings2 => {
        (for (refs1 <- bindings1; refs2 <- bindings2) yield {
          assert(refs1.length == refs2.length)
          val zippedRefs = refs1.zip(refs2)
          val solution = (for (pair <- zippedRefs) yield {
            val (canonicalRef1, canonicalRef2) = (pair._1.canonicalized.asInstanceOf[Attribute],
              pair._2.canonicalized.asInstanceOf[Attribute])
            // check from common attrib list mapping if we have a common
            // solution from the output equiv list, if yes then take its
            // 0th element
            commonAttribListMapping.get(canonicalRef1 -> canonicalRef2).
              map(_.head)
          }).toBuffer
          if (solution.forall(_.isDefined)) {
            Seq(
              templatizedExpr1.transformDown {
                case _: Attribute => solution.remove(0).get
              })
          } else {
            Seq.empty[Expression]
          }
        }).flatten
      }).getOrElse(Array.empty[Expression])
    }).flatten
    ExpressionSet(results)
  }

  /**
   * generates the AttributeRefs for templatization
   * The first time this runs, it runs by generating the templates for leg1 constraints.
   * It works by mapping Datatypes to a list of attributes (all with name "none") and the size of
   * this list of attributes will be equal to the maximum number of times an attribute of this
   * given type is used in the constraints.
   * So if there is a constraint => f(int, int, int, double, double) and
   * g(int, int, double, double, double), then the
   * int attribute list size = 3 and double attribute list size = 3.
   * Also the first int of any expression following corresponds to first element in the int list,
   * second int corresponds to second element in int list etc.
   * The purpose of copying on reset is to make sure this correspondence of first int in a
   * constraint to first element in the int attribute list.
   * Clone on reset is to keep the original mapping intact and to templatize, keep
   * removing from the cloned data. when the cloned data is exhausted ,
   * the new ids are generated and added to the original mapping.
   */
  class TemplateAttributeGenerator {
    private val mappingKeys: mutable.Map[DataType, mutable.ListBuffer[Attribute]] = mutable.Map()
    private var cloneOnReset: Map[DataType, mutable.ListBuffer[Attribute]] = _

    def getNext(dataType: DataType): Attribute =
      cloneOnReset.get(dataType).map(buff => if (buff.nonEmpty) {
        buff.remove(0)
      } else {
        val templatizedAttrNew = AttributeReference("none", dataType)()
        val originalBuffer = mappingKeys(dataType)
        originalBuffer += templatizedAttrNew
        templatizedAttrNew
      }).getOrElse {
        val templatizedAttrNew = AttributeReference("none", dataType)()
        val originalBuffer = mappingKeys.getOrElse(dataType, {
          val newBuff = mutable.ListBuffer[Attribute]()
          mappingKeys += dataType -> newBuff
          newBuff
        })
        originalBuffer += templatizedAttrNew
        templatizedAttrNew
      }

    def reset(): Unit = this.cloneOnReset = this.mappingKeys.map(kv => kv._1 -> kv._2.clone()).toMap
  }
}

object ConstraintSetHelper {
  def removeCanonicalizedExpressionFromBuffer(buff: mutable.Buffer[_ <: Expression],
    expr: Expression): Unit = {
    var keepGoing = true
    while (keepGoing) {
      val indx = buff.indexWhere(_.canonicalized fastEquals expr.canonicalized)
      if (indx == -1) {
        keepGoing = false
      } else {
        buff.remove(indx)
      }
    }
  }
}

/**
 * A helper class which is used to hide the child node so
 * as to prevent tree traversal below this node in case of
 * transformDown method. It is used to stop the traversal down
 * further
 *
 * @param hiddenChild the expression which we want to hide and prevent
 *                    from being traversed down further
 */
case class ExpressionHider(hiddenChild: Expression) extends LeafExpression {
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException(" not implemented")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(" not implemented")

  override def dataType: DataType = hiddenChild.dataType
}
