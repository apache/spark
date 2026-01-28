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

package org.apache.spark.sql.protobuf.utils

import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import com.google.protobuf.{DescriptorProtos, Descriptors, DynamicMessage, ExtensionRegistry, InvalidProtocolBufferException, Message}
import com.google.protobuf.DescriptorProtos.{FileDescriptorProto, FileDescriptorSet}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.TypeRegistry

import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

private[sql] object ProtobufUtils extends Logging {

  /** Wrapper for a pair of matched fields, one Catalyst and one corresponding Protobuf field. */
  private[sql] case class ProtoMatchedField(
      catalystField: StructField,
      catalystPosition: Int,
      fieldDescriptor: FieldDescriptor)

  /**
   * Container for a Protobuf descriptor and associated extensions.
   *
   * @param descriptor
   *   The descriptor for the top-level message.
   * @param extensionRegistry
   *   An extension registry populated with all discovered extensions.
   * @param fullNamesToExtensions
   *   A map from message full names to their extension fields.
   */
  private[sql] case class DescriptorWithExtensions(
      descriptor: Descriptor,
      extensionRegistry: ExtensionRegistry,
      fullNamesToExtensions: Map[String, Seq[FieldDescriptor]])

  /**
   * Helper class to perform field lookup/matching on Protobuf schemas.
   *
   * This will match `descriptor` against `catalystSchema`, attempting to find a matching field in
   * the Protobuf descriptor for each field in the Catalyst schema and vice-versa, respecting
   * settings for case sensitivity. The match results can be accessed using the getter methods.
   *
   * @param descriptor
   *   The descriptor in which to search for fields. Must be of type Descriptor.
   * @param catalystSchema
   *   The Catalyst schema to use for matching.
   * @param protoPath
   *   The seq of parent field names leading to `protoSchema`.
   * @param catalystPath
   *   The seq of parent field names leading to `catalystSchema`.
   * @param extensionFields
   *   Optional sequence of extension fields to include in field matching.
   */
  class ProtoSchemaHelper(
      descriptor: Descriptor,
      catalystSchema: StructType,
      protoPath: Seq[String],
      catalystPath: Seq[String],
      extensionFields: Seq[FieldDescriptor] = Seq.empty) {
    if (descriptor.getName == null) {
      throw QueryCompilationErrors.unknownProtobufMessageTypeError(
        descriptor.getName,
        descriptor.getContainingType().getName)
    }

    // Combine regular fields with extension fields
    private[this] val protoFieldArray =
      (descriptor.getFields.asScala ++ extensionFields).toArray
    private[this] val fieldMap = protoFieldArray
      .groupBy(_.getName.toLowerCase(Locale.ROOT))
      .transform((_, v) => v.toSeq) // toSeq needed for scala 2.13

    /** The fields which have matching equivalents in both Protobuf and Catalyst schemas. */
    val matchedFields: Seq[ProtoMatchedField] = catalystSchema.zipWithIndex.flatMap {
      case (sqlField, sqlPos) =>
        getFieldByName(sqlField.name).map(ProtoMatchedField(sqlField, sqlPos, _))
    }

    /**
     * Validate that there are no Catalyst fields which don't have a matching Protobuf field,
     * throwing [[AnalysisException]] if such extra fields are found. If `ignoreNullable` is
     * false, consider nullable Catalyst fields to be eligible to be an extra field; otherwise,
     * ignore nullable Catalyst fields when checking for extras.
     */
    def validateNoExtraCatalystFields(ignoreNullable: Boolean): Unit =
      catalystSchema.fields.foreach { sqlField =>
        if (getFieldByName(sqlField.name).isEmpty &&
          (!ignoreNullable || !sqlField.nullable)) {
          throw QueryCompilationErrors.cannotFindCatalystTypeInProtobufSchemaError(
            toFieldStr(catalystPath :+ sqlField.name))
        }
      }

    /**
     * Validate that there are no Protobuf fields which don't have a matching Catalyst field,
     * throwing [[AnalysisException]] if such extra fields are found. Only required (non-nullable)
     * fields are checked; nullable fields are ignored.
     */
    def validateNoExtraRequiredProtoFields(): Unit = {
      val extraFields = protoFieldArray.toSet -- matchedFields.map(_.fieldDescriptor)
      extraFields.filter(_.isRequired).foreach { extraField =>
        throw QueryCompilationErrors.cannotFindProtobufFieldInCatalystError(
          toFieldStr(protoPath :+ extraField.getName()))
      }
    }

    /**
     * Extract a single field from the contained Protobuf schema which has the desired field name,
     * performing the matching with proper case sensitivity according to SQLConf.resolver.
     *
     * @param name
     *   The name of the field to search for.
     * @return
     *   `Some(match)` if a matching Protobuf field is found, otherwise `None`.
     */
    private[protobuf] def getFieldByName(name: String): Option[FieldDescriptor] = {

      // get candidates, ignoring case of field name
      val candidates = fieldMap.getOrElse(name.toLowerCase(Locale.ROOT), Seq.empty)

      // search candidates, taking into account case sensitivity settings
      candidates.filter(f => SQLConf.get.resolver(f.getName(), name)) match {
        case Seq(protoField) => Some(protoField)
        case Seq() => None
        case matches =>
          throw QueryCompilationErrors.protobufFieldMatchError(
            name,
            toFieldStr(protoPath),
            s"${matches.size}",
            matches.map(_.getName()).mkString("[", ", ", "]"))
      }
    }
  }

  /**
   * Builds a Protobuf descriptor along with an ExtensionRegistry containing all extensions found
   * in the FileDescriptorSet.
   *
   * @param messageName
   *   Protobuf message name or Java class name (when binaryFileDescriptorSet is None).
   * @param binaryFileDescriptorSet
   *   When the binary `FileDescriptorSet` is provided, the descriptor, extensions, and registry
   *   are read from it. When None, the descriptor is loaded from the Java class and extensions
   *   are empty.
   * @return
   *   DescriptorWithExtensions containing descriptor, registry, and extension fields map
   */
  def buildDescriptor(
      messageName: String,
      binaryFileDescriptorSet: Option[Array[Byte]]): DescriptorWithExtensions =
    binaryFileDescriptorSet match {
      case Some(bytes) =>
        val fileDescriptors = parseFileDescriptorSet(bytes)
        val descriptor = fileDescriptors
          .flatMap { fileDesc =>
            fileDesc.getMessageTypes.asScala.find { desc =>
              desc.getName == messageName || desc.getFullName == messageName
            }
          }
          .headOption
          .getOrElse {
            throw QueryCompilationErrors.unableToLocateProtobufMessageError(messageName)
          }
        val (extensionRegistry, fullNamesToExtensions) = buildExtensionRegistry(fileDescriptors)
        DescriptorWithExtensions(descriptor, extensionRegistry, fullNamesToExtensions)
      case None =>
        // Given a Java class, we can only access the descriptor for the proto file it is defined
        // in. Extensions in other files will not be picked up. As such, we choose to disable
        // extension support when we fall back to the Java class.
        DescriptorWithExtensions(
          buildDescriptorFromJavaClass(messageName),
          ExtensionRegistry.getEmptyRegistry,
          Map.empty)
    }

  /**
   * Loads the given protobuf class and returns Protobuf descriptor for it.
   */
  def buildDescriptorFromJavaClass(protobufClassName: String): Descriptor = {

    // Default 'Message' class here is shaded while using the package (as in production).
    // The incoming classes might not be shaded. Check both.
    val shadedMessageClass = classOf[Message] // Shaded in prod, not in unit tests.
    val missingShadingErrorMessage = "The jar with Protobuf classes needs to be shaded " +
      s"(com.google.protobuf.* --> ${shadedMessageClass.getPackage.getName}.*)"

    val protobufClass = try {
      Utils.classForName(protobufClassName)
    } catch {
      case e: ClassNotFoundException =>
        val explanation =
          if (protobufClassName.contains(".")) "Ensure the class include in the jar"
          else "Ensure the class name includes package prefix"
        throw QueryCompilationErrors.protobufClassLoadError(protobufClassName, explanation, e)

      case e: NoClassDefFoundError if e.getMessage.matches("com/google/proto.*Generated.*") =>
        // This indicates the Java classes are not shaded.
        throw QueryCompilationErrors.protobufClassLoadError(
          protobufClassName, missingShadingErrorMessage, e)
    }

    if (!shadedMessageClass.isAssignableFrom(protobufClass)) {
      // Check if this extends 2.x Message class included in spark, that does not work.
      val unshadedMessageClass = Utils.classForName(
        // Generate "com.google.protobuf.Message". Using join() is a trick to escape from
        // jar shader. Otherwise, it will be replaced with 'org.sparkproject...'.
        String.join(".", "com", "google", "protobuf", "Message")
      )
      val explanation =
        if (unshadedMessageClass.isAssignableFrom(protobufClass)) {
          s"$protobufClassName does not extend shaded Protobuf Message class " +
          s"${shadedMessageClass.getName}. $missingShadingErrorMessage"
        } else s"$protobufClassName is not a Protobuf Message type"
      throw QueryCompilationErrors.protobufClassLoadError(protobufClassName, explanation)
    }

    // Extract the descriptor from Protobuf message.
    val getDescriptorMethod = try {
      protobufClass
        .getDeclaredMethod("getDescriptor")
    } catch {
      case e: NoSuchMethodError => // This is usually not expected.
        throw QueryCompilationErrors.protobufClassLoadError(
          protobufClassName, "Could not find getDescriptor() method", e)
    }

    getDescriptorMethod
      .invoke(null)
      .asInstanceOf[Descriptor]
  }

  private def parseFileDescriptorSet(bytes: Array[Byte]): List[Descriptors.FileDescriptor] = {
    var fileDescriptorSet: DescriptorProtos.FileDescriptorSet = null
    try {
      fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(bytes)
    } catch {
      case ex: InvalidProtocolBufferException =>
        throw QueryCompilationErrors.descriptorParseError(ex)
    }
    val fileDescriptorProtoIndex = createDescriptorProtoMap(fileDescriptorSet)

    // Mutated across invocations of buildFileDescriptor.
    val builtDescriptors = mutable.Map[String, Descriptors.FileDescriptor]()
    val fileDescriptorList: List[Descriptors.FileDescriptor] =
      fileDescriptorSet.getFileList.asScala.map { fileDescriptorProto =>
        buildFileDescriptor(fileDescriptorProto, fileDescriptorProtoIndex, builtDescriptors)
      }.distinctBy(_.getFullName).toList
    fileDescriptorList
  }

  /**
   * Recursively constructs file descriptors for all dependencies for given FileDescriptorProto
   * and return.
   */
  private def buildFileDescriptor(
      fileDescriptorProto: FileDescriptorProto,
      fileDescriptorProtoMap: Map[String, FileDescriptorProto],
      builtDescriptors: mutable.Map[String, Descriptors.FileDescriptor])
      : Descriptors.FileDescriptor = {
    // Storing references to constructed descriptors is crucial because descriptors are compared
    // by reference inside in the Protobuf library.
    builtDescriptors.getOrElseUpdate(
      fileDescriptorProto.getName, {
        val fileDescriptorList = fileDescriptorProto.getDependencyList().asScala.map {
          dependency =>
            fileDescriptorProtoMap.get(dependency) match {
              case Some(dependencyProto) =>
                if (dependencyProto.getName == "google/protobuf/any.proto"
                  && dependencyProto.getPackage == "google.protobuf") {
                  // For Any, use the descriptor already included as part of the Java dependency.
                  // Without this, JsonFormat used for converting Any fields fails when
                  // an Any field in input is set to `Any.getDefaultInstance()`.
                  com.google.protobuf.AnyProto.getDescriptor
                  // Should we do the same for timestamp.proto and empty.proto?
                } else {
                  buildFileDescriptor(dependencyProto, fileDescriptorProtoMap, builtDescriptors)
                }
              case None =>
                throw QueryCompilationErrors.protobufDescriptorDependencyError(dependency)
            }
        }
        Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, fileDescriptorList.toArray)
      })
  }

  /**
   * Returns a map from descriptor proto name as found inside the descriptors to protos.
   */
  private def createDescriptorProtoMap(
    fileDescriptorSet: FileDescriptorSet): Map[String, FileDescriptorProto] = {
    fileDescriptorSet.getFileList().asScala.map { descriptorProto =>
      descriptorProto.getName() -> descriptorProto
    }.toMap[String, FileDescriptorProto]
  }

  /**
   * Convert a sequence of hierarchical field names (like `Seq(foo, bar)`) into a human-readable
   * string representing the field, like "field 'foo.bar'". If `names` is empty, the string
   * "top-level record" is returned.
   */
  private[protobuf] def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top-level record"
    case n => s"field '${n.mkString(".")}'"
  }

  /** Builds [[TypeRegistry]] with all the messages found in the descriptor set. */
  private[protobuf] def buildTypeRegistry(descriptorBytes: Array[Byte]): TypeRegistry = {
    val registryBuilder = TypeRegistry.newBuilder()
    for (fileDesc <- parseFileDescriptorSet(descriptorBytes)) {
      registryBuilder.add(fileDesc.getMessageTypes)
    }
    registryBuilder.build()
  }

  /** Builds [[TypeRegistry]] with the descriptor and the others from the same proto file. */
  private[protobuf] def buildTypeRegistry(descriptor: Descriptor): TypeRegistry = {
    TypeRegistry
      .newBuilder()
      .add(descriptor) // This adds any other descriptors in the associated proto file.
      .build()
  }

  /**
   * Builds an ExtensionRegistry and an index from full name to field descriptor for all extensions
   * found in the list of provided file descriptors.
   * 
   * This method will traverse the AST to ensure extensions in nested scopes are registered as well.
   *
   * @param fileDescriptors
   *   List of all file descriptors to process
   * @return
   *   The populated ExtensionRegistry and a map from message full names to extension fields
   *   sorted by field number
   */
  private def buildExtensionRegistry(fileDescriptors: List[Descriptors.FileDescriptor])
      : (ExtensionRegistry, Map[String, Seq[FieldDescriptor]]) = {
    val registry = ExtensionRegistry.newInstance()
    val fullNameToExtensions = mutable.Map[String, ArrayBuffer[FieldDescriptor]]()

    // Adds an extension to both the registry and map.
    def addExtension(extField: FieldDescriptor): Unit = {
      val extendeeName = extField.getContainingType.getFullName
      // For message-type extensions, we need to provide a default instance.
      if (extField.getJavaType == JavaType.MESSAGE) {
        val defaultInstance = DynamicMessage.getDefaultInstance(extField.getMessageType)
        registry.add(extField, defaultInstance)
      } else {
        registry.add(extField)
      }
      fullNameToExtensions
        .getOrElseUpdate(extendeeName, mutable.ArrayBuffer())
        .append(extField)
    }

    for (fileDesc <- fileDescriptors) {
      fileDesc.getExtensions.asScala.foreach(addExtension)

      // Recursively add nested extensions.
      def collectNestedExtensions(msgDesc: Descriptor): Unit = {
        msgDesc.getExtensions.asScala.foreach(addExtension)
        msgDesc.getNestedTypes.asScala.foreach(collectNestedExtensions)
      }
      fileDesc.getMessageTypes.asScala.foreach(collectNestedExtensions)
    }

    // Sort extension fields by field number for consistent ordering.
    val sortedMap = fullNameToExtensions.map { case (name, extensions) =>
      name -> extensions.sortBy(_.getNumber).toSeq
    }.toMap
    (registry, sortedMap)
  }
}
