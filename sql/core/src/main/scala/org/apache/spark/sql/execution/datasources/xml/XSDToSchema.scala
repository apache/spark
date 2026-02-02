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
package org.apache.spark.sql.execution.datasources.xml

import java.io.StringReader

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader
import org.apache.ws.commons.schema._
import org.apache.ws.commons.schema.constants.Constants

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.xml.{ValidatorUtil, XmlOptions}
import org.apache.spark.sql.types._

/**
 * Utility to generate a Spark schema from an XSD. Not all XSD schemas are simple tabular schemas,
 * so not all elements or XSDs are supported.
 */
object XSDToSchema extends Logging{

  /**
   * Reads a schema from an XSD path.
   * Note that if the schema consists of one complex parent type which you want to use as
   * the row tag schema, then you will need to extract the schema of the single resulting
   * struct in the resulting StructType, and use its StructType as your schema.
   *
   * @param xsdPath XSD path
   * @return Spark-compatible schema
   */
  def read(xsdPath: Path): StructType = {
    val in = ValidatorUtil.openSchemaFile(xsdPath)
    val xmlSchemaCollection = new XmlSchemaCollection()
    xmlSchemaCollection.setBaseUri(xsdPath.toString)
    val xmlSchema = xmlSchemaCollection.read(new InputStreamReader(in))
    getStructType(xmlSchema)
  }

  /**
   * Reads a schema from an XSD as a string.
   * Note that if the schema consists of one complex parent type which you want to use as
   * the row tag schema, then you will need to extract the schema of the single resulting
   * struct in the resulting StructType, and use its StructType as your schema.
   *
   * @param xsdString XSD as a string
   * @return Spark-compatible schema
   */
  def read(xsdString: String): StructType = {
    val xmlSchema = new XmlSchemaCollection().read(new StringReader(xsdString))
    getStructType(xmlSchema)
  }


  private def getStructField(xmlSchema: XmlSchema, schemaType: XmlSchemaType): StructField = {
    schemaType match {
      // xs:simpleType
      case simpleType: XmlSchemaSimpleType =>
        val schemaType = simpleType.getContent match {
          case restriction: XmlSchemaSimpleTypeRestriction =>
            val qName = simpleType.getQName match {
              case null => restriction.getBaseTypeName
              case n => n
            }

            // Hacky, is there a better way? see if the type is known as a custom
            // type and use that if so, assuming along the way it's a simple restriction
            val typeName = xmlSchema.getSchemaTypes.asScala.get(qName).map { s =>
                s.asInstanceOf[XmlSchemaSimpleType].
                  getContent.asInstanceOf[XmlSchemaSimpleTypeRestriction].getBaseTypeName
              }.getOrElse(qName)

            typeName match {
              case Constants.XSD_BOOLEAN => BooleanType
              case Constants.XSD_DECIMAL =>
                val facets = restriction.getFacets.asScala
                val fracDigits = facets.collectFirst {
                  case facet: XmlSchemaFractionDigitsFacet => facet.getValue.toString.toInt
                }.getOrElse(18)
                val totalDigits = facets.collectFirst {
                  case facet: XmlSchemaTotalDigitsFacet => facet.getValue.toString.toInt
                }.getOrElse(38)
                DecimalType(totalDigits, math.min(totalDigits, fracDigits))
              case Constants.XSD_UNSIGNEDLONG |
                   Constants.XSD_INTEGER |
                   Constants.XSD_NEGATIVEINTEGER |
                   Constants.XSD_NONNEGATIVEINTEGER |
                   Constants.XSD_NONPOSITIVEINTEGER |
                   Constants.XSD_POSITIVEINTEGER => DecimalType(38, 0)
              case Constants.XSD_DOUBLE => DoubleType
              case Constants.XSD_FLOAT => FloatType
              case Constants.XSD_BYTE => ByteType
              case Constants.XSD_SHORT |
                   Constants.XSD_UNSIGNEDBYTE => ShortType
              case Constants.XSD_INT |
                   Constants.XSD_UNSIGNEDSHORT => IntegerType
              case Constants.XSD_LONG |
                   Constants.XSD_UNSIGNEDINT => LongType
              case Constants.XSD_DATE => DateType
              case Constants.XSD_DATETIME => TimestampType
              case _ => StringType
            }
          case _ => StringType
        }
        StructField("baseName", schemaType)

      // xs:complexType
      case complexType: XmlSchemaComplexType =>
        complexType.getContentModel match {
          case content: XmlSchemaSimpleContent =>
            // xs:simpleContent
            content.getContent match {
              case extension: XmlSchemaSimpleContentExtension =>
                val baseStructField = getStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                val value = StructField("_VALUE", baseStructField.dataType)
                val attributes = extension.getAttributes.asScala.map {
                  case attribute: XmlSchemaAttribute =>
                    val baseStructField = getStructField(xmlSchema,
                      xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName))
                    StructField(s"_${attribute.getName}", baseStructField.dataType,
                      attribute.getUse != XmlSchemaUse.REQUIRED)
                }.toSeq
                StructField(complexType.getName, StructType(value +: attributes))
              case unsupported =>
                throw new IllegalArgumentException(s"Unsupported content: $unsupported")
            }
          case content: XmlSchemaComplexContent =>
            val complexContent = content.getContent
            complexContent match {
              case extension: XmlSchemaComplexContentExtension =>
                val baseStructField = getStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                val baseFields = baseStructField.dataType match {
                  case structType: StructType => structType.fields
                  case others =>
                    throw new IllegalArgumentException(
                      s"Non-StructType in ComplexContentExtension: $others"
                    )
                }

                val extendedFields = getStructFieldsFromParticle(extension.getParticle, xmlSchema)
                StructField(
                  schemaType.getQName.getLocalPart,
                  StructType(baseFields ++ extendedFields)
                )
              case unsupported =>
                throw new IllegalArgumentException(s"Unsupported content: $unsupported")
            }
          case null =>
            val childFields = getStructFieldsFromParticle(complexType.getParticle, xmlSchema)
            val attributes = complexType.getAttributes.asScala.map {
              case attribute: XmlSchemaAttribute =>
                val attributeType = attribute.getSchemaTypeName match {
                  case null =>
                    StringType
                  case t =>
                    getStructField(xmlSchema, xmlSchema.getParent.getTypeByQName(t)).dataType
                }
                StructField(s"_${attribute.getName}", attributeType,
                  attribute.getUse != XmlSchemaUse.REQUIRED)
            }.toSeq
            StructField(complexType.getName, StructType(childFields ++ attributes))
          case unsupported =>
            throw new IllegalArgumentException(s"Unsupported content model: $unsupported")
        }
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported schema element type: $unsupported")
    }
  }

  private def getStructType(xmlSchema: XmlSchema): StructType = {
    StructType(xmlSchema.getElements.asScala.toSeq.map { case (_, schemaElement) =>
      val schemaType = schemaElement.getSchemaType
      // if (schemaType.isAnonymous) {
      //   schemaType.setName(qName.getLocalPart)
      // }
      val rootType = getStructField(xmlSchema, schemaType)
      StructField(schemaElement.getName, rootType.dataType, schemaElement.getMinOccurs == 0)
    })
  }

  private def getStructFieldsFromParticle(
      particle: XmlSchemaParticle,
      xmlSchema: XmlSchema
  ): Seq[StructField] = {
    particle match {
        // xs:all
        case all: XmlSchemaAll =>
          all.getItems.asScala.map {
            case element: XmlSchemaElement =>
              val baseStructField = getStructField(xmlSchema, element.getSchemaType)
              val nullable = element.getMinOccurs == 0
              if (element.getMaxOccurs == 1) {
                StructField(element.getName, baseStructField.dataType, nullable)
              } else {
                StructField(element.getName, ArrayType(baseStructField.dataType), nullable)
              }
          }.toSeq
        // xs:choice
        case choice: XmlSchemaChoice =>
          choice.getItems.asScala.map {
            case element: XmlSchemaElement =>
              val baseStructField = getStructField(xmlSchema, element.getSchemaType)
              if (element.getMaxOccurs == 1) {
                StructField(element.getName, baseStructField.dataType, true)
              } else {
                StructField(element.getName, ArrayType(baseStructField.dataType), true)
              }
            case any: XmlSchemaAny =>
              val dataType = if (any.getMaxOccurs > 1) ArrayType(StringType) else StringType
              StructField(XmlOptions.DEFAULT_WILDCARD_COL_NAME, dataType, true)
          }.toSeq
        // xs:sequence
        case sequence: XmlSchemaSequence =>
          // flatten xs:choice nodes
          sequence.getItems.asScala.flatMap {
            _ match {
              case choice: XmlSchemaChoice =>
                choice.getItems.asScala.map { e =>
                  val xme = e.asInstanceOf[XmlSchemaElement]
                  val baseType = getStructField(xmlSchema, xme.getSchemaType).dataType
                  val dataType = if (xme.getMaxOccurs > 1) ArrayType(baseType) else baseType
                  StructField(xme.getName, dataType, true)
                }
              case e: XmlSchemaElement =>
                val refQName = e.getRef.getTargetQName
                val baseType =
                  if (refQName != null) {
                    getStructField(
                      xmlSchema,
                      xmlSchema.getParent.getElementByQName(refQName).getSchemaType).dataType
                  }
                  else getStructField(xmlSchema, e.getSchemaType).dataType
                val dataType = if (e.getMaxOccurs > 1) ArrayType(baseType) else baseType
                val nullable = e.getMinOccurs == 0
                val structFieldName =
                  Option(refQName).map(_.getLocalPart).getOrElse(e.getName)
                Seq(StructField(structFieldName, dataType, nullable))
              case any: XmlSchemaAny =>
                val dataType =
                  if (any.getMaxOccurs > 1) ArrayType(StringType) else StringType
                val nullable = any.getMinOccurs == 0
                Seq(StructField(XmlOptions.DEFAULT_WILDCARD_COL_NAME, dataType, nullable))
              case unsupported =>
                throw new IllegalArgumentException(s"Unsupported item: $unsupported")
            }
          }.toSeq
        case null =>
          Seq.empty
        case unsupported =>
          throw new IllegalArgumentException(s"Unsupported particle: $unsupported")
      }
  }
}
