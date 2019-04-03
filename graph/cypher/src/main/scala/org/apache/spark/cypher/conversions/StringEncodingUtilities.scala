package org.apache.spark.cypher.conversions

import scala.annotation.tailrec

object StringEncodingUtilities {

  val propertyPrefix: String = "property_"

  val relTypePrefix: String = "relType_"

  protected val maxCharactersInHexStringEncoding: Int = 4 // Hex string encoding of a `Char` is up to 4 characters

  implicit class CharOps(val c: Char) extends AnyVal {
    def isAscii: Boolean = c.toInt <= 127
  }

  implicit class StringOps(val s: String) extends AnyVal {

    def toPropertyColumnName: String = {
      s"$propertyPrefix${s.encodeSpecialCharacters}"
    }

    def isPropertyColumnName: Boolean = s.startsWith(propertyPrefix)

    def toProperty: String = {
      if (s.isPropertyColumnName) {
        s.drop(propertyPrefix.length).decodeSpecialCharacters
      } else {
        s
      }
    }

    def toRelTypeColumnName: String = {
      s"$relTypePrefix${s.encodeSpecialCharacters}"
    }

    def isRelTypeColumnName: Boolean = s.startsWith(relTypePrefix)

    /**
      * Encodes special characters in a string.
      *
      * The encoded string contains only ASCII letters, numbers, '_', and '@'. The encoded string is compatible
      * with both SQL column names and file paths.
      *
      * @return encoded string
      */
    def encodeSpecialCharacters: String = {
      val sb = new StringBuilder

      @tailrec def recEncode(index: Int): Unit = {
        if (index < s.length) {
          val charToEncode = s(index)
          if (charToEncode == '_' || (charToEncode.isLetterOrDigit && charToEncode.isAscii)) {
            sb.append(charToEncode)
          } else {
            sb.append("@")
            val hexString = charToEncode.toHexString
            // Pad left to max encoded length with '0's
            for (_ <- 0 until maxCharactersInHexStringEncoding - hexString.length) sb.append('0')
            sb.append(hexString)
          }
          recEncode(index + 1)
        }
      }

      recEncode(0)
      sb.toString
    }

    /**
      * Recovers the original string from a string encoded with [[encodeSpecialCharacters]].
      *
      * @return original string
      */
    def decodeSpecialCharacters: String = {
      val sb = new StringBuilder

      @tailrec def recDecode(index: Int): Unit = {
        if (index < s.length) {
          val charToDecode = s(index)
          val nextIndex = if (charToDecode == '@') {
            val encodedHexStringStart = index + 1
            val indexAfterHexStringEnd = encodedHexStringStart + maxCharactersInHexStringEncoding
            val hexString = s.substring(encodedHexStringStart, indexAfterHexStringEnd)
            sb.append(hexString.parseHex)
            indexAfterHexStringEnd
          } else {
            sb.append(charToDecode)
            index + 1
          }
          recDecode(nextIndex)
        }
      }

      recDecode(0)
      sb.toString
    }

    def parseHex: Char = Integer.parseInt(s, 16).toChar

  }

}
