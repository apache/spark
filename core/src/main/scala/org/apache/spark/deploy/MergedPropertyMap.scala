package org.apache.spark.deploy

import scala.collection._
/**
 * Created by dale on 22/09/2014.
 */

object MergedPropertyMap {

  /**
   * Flatten a map of maps out into a single map, later maps in the propList
   * have priority over older ones
   * @param propList list of property maps to merge
   */
  def mergePropertyMaps( propList: Vector[Map[String, String]]): mutable.Map[String, String] = {
    val propMap = new mutable.HashMap[String, String]()
    // loop through each entry of each map in order of priority
    // and add it to our propMap
    propList.foreach {
      _.foreach{ case(k,v) => propMap.getOrElseUpdate(k,v)}
    }
    propMap
  }

  /**
   * Given an map of (old prop Name -> new propName)
   * will grab properties from propSource and add them to the output under the new name
   * the existing old property will still be copied out as well
   * @param propAliases Map[old Propname -> New PropName]
   * @param propSource Map[PropName -> PropValue]
   * @return new entries as per propAliases
   */
  def applyAliases( propAliases: Map[String, String], propSource: Map[String, String]): Map[String, String] = {
    def entries: Map[String, String]  = for{
      (oldPropName, newPropName) <- propAliases
      propValue = propSource.get(oldPropName)
    } yield (newPropName -> propValue.get)
    entries
  }
}