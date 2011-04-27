/* NSC -- new Scala compiler
 * Copyright 2005-2010 LAMP/EPFL
 * @author Paul Phillips
 */

package spark.repl

import scala.tools.nsc
import scala.tools.nsc._
import scala.tools.nsc.interpreter
import scala.tools.nsc.interpreter._

/** This has a lot of duplication with other methods in Symbols and Types,
 *  but repl completion utility is very sensitive to precise output.  Best
 *  thing would be to abstract an interface for how such things are printed,
 *  as is also in progress with error messages.
 */
trait SparkCompletionOutput {
  self: SparkCompletion =>
  
  import global._
  import definitions.{ NothingClass, AnyClass, isTupleType, isFunctionType, isRepeatedParamType }
  
  /** Reducing fully qualified noise for some common packages.
   */
  val typeTransforms = List(
    "java.lang." -> "",
    "scala.collection.immutable." -> "immutable.",
    "scala.collection.mutable." -> "mutable.",
    "scala.collection.generic." -> "generic."
  )
    
  def quietString(tp: String): String =
    typeTransforms.foldLeft(tp) {
      case (str, (prefix, replacement)) =>
        if (str startsWith prefix) replacement + (str stripPrefix prefix)
        else str
    }
  
  class MethodSymbolOutput(method: Symbol) {
    val pkg       = method.ownerChain find (_.isPackageClass) map (_.fullName) getOrElse ""
    
    def relativize(str: String): String = quietString(str stripPrefix (pkg + "."))
    def relativize(tp: Type): String    = relativize(tp.normalize.toString)
    def relativize(sym: Symbol): String = relativize(sym.info)
    
    def braceList(tparams: List[String]) = if (tparams.isEmpty) "" else (tparams map relativize).mkString("[", ", ", "]")
    def parenList(params: List[Any])  = params.mkString("(", ", ", ")")
    
    def methodTypeToString(mt: MethodType) =
      (mt.paramss map paramsString mkString "") + ": " + relativize(mt.finalResultType)
    
    def typeToString(tp: Type): String = relativize(
      tp match {
        case x if isFunctionType(x)           => functionString(x)
        case x if isTupleType(x)              => tupleString(x)
        case x if isRepeatedParamType(x)      => typeToString(x.typeArgs.head) + "*"
        case mt @ MethodType(_, _)            => methodTypeToString(mt)
        case x                                => x.toString
      }
    )

    def tupleString(tp: Type) = parenList(tp.normalize.typeArgs map relativize)
    def functionString(tp: Type) = tp.normalize.typeArgs match {
      case List(t, r) => t + " => " + r
      case xs         => parenList(xs.init) + " => " + xs.last
    }

    def tparamsString(tparams: List[Symbol])  = braceList(tparams map (_.defString))
    def paramsString(params: List[Symbol])    = {
      def paramNameString(sym: Symbol)  = if (sym.isSynthetic) "" else sym.nameString + ": "
      def paramString(sym: Symbol)      = paramNameString(sym) + typeToString(sym.info.normalize)

      val isImplicit = params.nonEmpty && params.head.isImplicit
      val strs = (params map paramString) match {
        case x :: xs if isImplicit  => ("implicit " + x) :: xs
        case xs                     => xs
      }
      parenList(strs)
    }    

    def methodString() =
      method.keyString + " " + method.nameString + (method.info.normalize match {
        case PolyType(Nil, resType)             => ": " + typeToString(resType) // nullary method
        case PolyType(tparams, resType)         => tparamsString(tparams) + typeToString(resType)
        case mt @ MethodType(_, _)              => methodTypeToString(mt)
        case x                                  => 
          DBG("methodString(): %s / %s".format(x.getClass, x))            
          x.toString
      })
  }  
}
