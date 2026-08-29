/**
 * Pure-Java IEEE 754 Binary Integer Decimal (BID) decimal64 and decimal128.
 *
 * <p>This is not Intel's C libbid and is not an Apache Spark module. BID64 and
 * BID128 kernels are ported from Intel RDFP under BSD-3-Clause. Decimal256 is
 * out of scope for this release. Raw kernels live in {@link Bid64Raw} and
 * {@link Bid128Raw}; DBR JNI shapes in {@link DecFloat16Compat} and
 * {@link DecFloat34Compat}. IEEE binary128 / DPML is {@code org.bidfp.binary128}
 * ({@code org.bidfp:binary128}).
 */
package org.bidfp;
