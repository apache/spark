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

package org.apache.spark.sql.connect.service

import scala.util.control.NonFatal

import io.grpc.{Context, Metadata, ServerCall, ServerCallHandler, ServerInterceptor}
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{BYTE_SIZE, CLASS_NAME, SESSION_ID, USER_ID}
import org.apache.spark.sql.connect.utils.{ErrorUtils, PlanCompressionUtils}

/**
 * Interceptor that decompresses compressed requests before they reach downstream handlers.
 *
 * This interceptor currently handles:
 *   - ExecutePlanRequest with compressed plans
 *   - AnalyzePlanRequest with compressed plans (Schema, Explain, TreeString, IsLocal,
 *     IsStreaming, InputFiles, SemanticHash, and SameSemantics analysis types)
 *
 * Compressed plan size metrics are tracked in gRPC Context for use by handlers.
 */
class RequestDecompressionInterceptor extends ServerInterceptor with Logging {

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {

    // Create a listener that will intercept and decompress the message
    val listener = next.startCall(call, headers)

    new SimpleForwardingServerCallListener[ReqT](listener) {
      override def onMessage(message: ReqT): Unit = {
        message match {
          case req: proto.ExecutePlanRequest =>
            handleRequestWithDecompression(
              req.getUserContext.getUserId,
              req.getSessionId,
              () => decompressExecutePlanRequest(req))

          case req: proto.AnalyzePlanRequest =>
            handleRequestWithDecompression(
              req.getUserContext.getUserId,
              req.getSessionId,
              () => decompressAnalyzePlanRequest(req))

          case other =>
            // Forward all other message types as-is (no decompression or error handling needed)
            super.onMessage(other)
        }
      }

      private def handleRequestWithDecompression[T](
          userId: String,
          sessionId: String,
          decompressRequest: () => (T, Seq[Option[Long]])): Unit = {
        val (decompressedReq, compressedSizes) =
          try {
            decompressRequest()
          } catch {
            case NonFatal(e) =>
              // Handle decompression errors
              logError(
                log"Plan decompression failed: " +
                  log"userId=${MDC(USER_ID, userId)}, " +
                  log"sessionId=${MDC(SESSION_ID, sessionId)}",
                e)
              ErrorUtils.handleError("planDecompression", call, userId, sessionId)(e)
              return
          }

        // Set compressed sizes in context
        val contextToUse = Context.current().withValue(
          RequestDecompressionContext.COMPRESSED_SIZES_KEY, compressedSizes)

        // Run the rest of the call chain with the context
        val prev = contextToUse.attach()
        try {
          super.onMessage(decompressedReq.asInstanceOf[ReqT])
        } finally {
          contextToUse.detach(prev)
        }
      }
    }
  }

  private def decompressExecutePlanRequest(request: proto.ExecutePlanRequest)
      : (proto.ExecutePlanRequest, Seq[Option[Long]]) = {
    if (!request.hasPlan) {
      return (request, Seq.empty)
    }
    val (decompressedReq, size) = decompressPlanGeneric(
      request,
      request.getUserContext.getUserId,
      request.getSessionId,
      (r: proto.ExecutePlanRequest) => r.getPlan,
      (req: proto.ExecutePlanRequest, plan: proto.Plan) => req.toBuilder.setPlan(plan).build())

    (decompressedReq, Seq(size))
  }

  private def decompressAnalyzePlanRequest(request: proto.AnalyzePlanRequest)
      : (proto.AnalyzePlanRequest, Seq[Option[Long]]) = {
    val userId = request.getUserContext.getUserId
    val sessionId = request.getSessionId

    // Helper to decompress a single-plan analysis type
    def decompress(
        req: proto.AnalyzePlanRequest,
        getPlan: proto.AnalyzePlanRequest => proto.Plan,
        rebuild: (proto.AnalyzePlanRequest, proto.Plan) => proto.AnalyzePlanRequest) = {
      decompressPlanGeneric(req, userId, sessionId, getPlan, rebuild)
    }

    // NOTE: All AnalyzePlanRequest cases are explicitly listed here.
    // The default case throws an exception to catch new cases at runtime and fail CI tests.
    request.getAnalyzeCase match {
      // Cases with Plan fields - decompress if compressed
      case proto.AnalyzePlanRequest.AnalyzeCase.SCHEMA =>
        val (req, size) = decompress(
          request,
          _.getSchema.getPlan,
          (r, p) => r.toBuilder.setSchema(r.getSchema.toBuilder.setPlan(p)).build())
        (req, Seq(size))

      case proto.AnalyzePlanRequest.AnalyzeCase.EXPLAIN =>
        val (req, size) = decompress(
          request,
          _.getExplain.getPlan,
          (r, p) => r.toBuilder.setExplain(r.getExplain.toBuilder.setPlan(p)).build())
        (req, Seq(size))

      case proto.AnalyzePlanRequest.AnalyzeCase.TREE_STRING =>
        val (req, size) = decompress(
          request,
          _.getTreeString.getPlan,
          (r, p) => r.toBuilder.setTreeString(r.getTreeString.toBuilder.setPlan(p)).build())
        (req, Seq(size))

      case proto.AnalyzePlanRequest.AnalyzeCase.IS_LOCAL =>
        val (req, size) = decompress(
          request,
          _.getIsLocal.getPlan,
          (r, p) => r.toBuilder.setIsLocal(r.getIsLocal.toBuilder.setPlan(p)).build())
        (req, Seq(size))

      case proto.AnalyzePlanRequest.AnalyzeCase.IS_STREAMING =>
        val (req, size) = decompress(
          request,
          _.getIsStreaming.getPlan,
          (r, p) => r.toBuilder.setIsStreaming(r.getIsStreaming.toBuilder.setPlan(p)).build())
        (req, Seq(size))

      case proto.AnalyzePlanRequest.AnalyzeCase.INPUT_FILES =>
        val (req, size) = decompress(
          request,
          _.getInputFiles.getPlan,
          (r, p) => r.toBuilder.setInputFiles(r.getInputFiles.toBuilder.setPlan(p)).build())
        (req, Seq(size))

      case proto.AnalyzePlanRequest.AnalyzeCase.SEMANTIC_HASH =>
        val (req, size) = decompress(
          request,
          _.getSemanticHash.getPlan,
          (r, p) => r.toBuilder.setSemanticHash(r.getSemanticHash.toBuilder.setPlan(p)).build())
        (req, Seq(size))

      case proto.AnalyzePlanRequest.AnalyzeCase.SAME_SEMANTICS =>
        // Special case: has two Plan fields (target_plan and other_plan)
        val (reqWithTarget, targetSize) = decompress(
          request,
          _.getSameSemantics.getTargetPlan,
          (r, p) =>
            r.toBuilder.setSameSemantics(r.getSameSemantics.toBuilder.setTargetPlan(p)).build())

        val (finalReq, otherSize) = decompress(
          reqWithTarget,
          _.getSameSemantics.getOtherPlan,
          (r, p) =>
            r.toBuilder.setSameSemantics(r.getSameSemantics.toBuilder.setOtherPlan(p)).build())

        (finalReq, Seq(targetSize, otherSize))

      // Cases with Relation fields - currently not compressed
      case proto.AnalyzePlanRequest.AnalyzeCase.PERSIST |
          proto.AnalyzePlanRequest.AnalyzeCase.UNPERSIST |
          proto.AnalyzePlanRequest.AnalyzeCase.GET_STORAGE_LEVEL =>
        (request, Seq.empty)

      // Cases with no Plan or Relation fields - safe to pass through
      case proto.AnalyzePlanRequest.AnalyzeCase.SPARK_VERSION |
          proto.AnalyzePlanRequest.AnalyzeCase.DDL_PARSE |
          proto.AnalyzePlanRequest.AnalyzeCase.JSON_TO_DDL =>
        (request, Seq.empty)

      // No analysis case set - safe to pass through, will be handled in handler
      case proto.AnalyzePlanRequest.AnalyzeCase.ANALYZE_NOT_SET =>
        (request, Seq.empty)

      case _ =>
        // Unhandled case - fail to catch new cases during testing
        throw new UnsupportedOperationException(
          s"Unhandled AnalyzePlanRequest case: ${request.getAnalyzeCase}. " +
            s"RequestDecompressionInterceptor must be updated to handle this case explicitly. " +
            s"If the case contains Plan fields, add decompression logic. " +
            s"Otherwise, add it to the safe passthrough cases.")
    }
  }

  private def decompressPlanGeneric[Req](
      request: Req,
      userId: String,
      sessionId: String,
      getPlan: Req => proto.Plan,
      rebuild: (Req, proto.Plan) => Req): (Req, Option[Long]) = {

    val plan = getPlan(request)

    if (plan.getOpTypeCase != proto.Plan.OpTypeCase.COMPRESSED_OPERATION) {
      return (request, None)
    }
    val compressedSize = plan.getCompressedOperation.getData.size().toLong
    val requestType = request.getClass.getSimpleName
    logInfo(
      log"Received compressed plan in ${MDC(CLASS_NAME, requestType)} " +
        log"(size=${MDC(BYTE_SIZE, compressedSize)} bytes): " +
        log"userId=${MDC(USER_ID, userId)}, sessionId=${MDC(SESSION_ID, sessionId)}")

    val decompressedPlan = PlanCompressionUtils.decompressPlan(plan)

    logInfo(
      log"Plan decompression completed " +
        log"(compressed=${MDC(BYTE_SIZE, compressedSize)} bytes -> " +
        log"decompressed=${MDC(BYTE_SIZE, decompressedPlan.getSerializedSize.toLong)} bytes, " +
        log"userId=${MDC(USER_ID, userId)}, sessionId=${MDC(SESSION_ID, sessionId)}")

    val decompressedRequest = rebuild(request, decompressedPlan)
    (decompressedRequest, Some(compressedSize))
  }
}

/**
 * Context holder for passing decompression metrics from interceptor to ExecuteHolder. Uses gRPC
 * Context to properly propagate values across async boundaries.
 */
object RequestDecompressionContext {

  /**
   * Context key for storing compressed sizes. This is set by RequestDecompressionInterceptor
   * when compressed requests are encountered, and read by handlers for metrics.
   *
   * The sequence contains Option[Long] entries corresponding to each plan in the request:
   * - For ExecutePlan and single-plan Analyze requests: Seq(Some(size)) if compressed, or
   *   Seq.empty if not
   * - For AnalyzePlanRequest SameSemantics with two plans: Seq(target_size, other_size) where
   *   each is Some(size) if that plan was compressed, None if not
   *
   * The sequence length matches the number of plans, with explicit None for uncompressed plans.
   */
  val COMPRESSED_SIZES_KEY: Context.Key[Seq[Option[Long]]] = Context.key("compressed-sizes")

  /**
   * Get all compressed sizes from the current gRPC context. Returns empty sequence if no
   * compressed sizes were set.
   */
  def getCompressedSizes: Seq[Option[Long]] = {
    Option(COMPRESSED_SIZES_KEY.get()).getOrElse(Seq.empty)
  }

  /**
   * Get the first compressed size from the current gRPC context. Returns None if no compressed
   * size was set (request was not compressed).
   *
   * This is the primary size for ExecutePlan and single-plan Analyze requests, and the
   * target_plan size for SameSemantics requests.
   */
  def getCompressedSize: Option[Long] = {
    getCompressedSizes.headOption.flatten
  }

  /**
   * Get the second compressed size from the current gRPC context. Returns None if no second
   * compressed size was set.
   *
   * This is only set for AnalyzePlanRequest SameSemantics with a compressed other_plan.
   */
  def getOtherCompressedSize: Option[Long] = {
    val sizes = getCompressedSizes
    if (sizes.length > 1) sizes(1) else None
  }
}
