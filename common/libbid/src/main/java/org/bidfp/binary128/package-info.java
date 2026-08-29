/**
 * IEEE 754 binary128 representation and the Intel DPML float128 kernels used
 * by this project's BID64/BID128 transcendental functions.
 *
 * <p>This artifact has no dependency on BID. Decimal wrappers that convert
 * BID64/BID128 through this type live in {@code org.bidfp} ({@code libbid-java}).
 *
 * <p>This package is the bounded Java port of libbid's emulated
 * {@code bid_f128_*} engine. It is not a complete general-purpose binary128
 * language binding: only the representation, arithmetic, and kernel families
 * required by this libbid port are supported. It does not provide decimal
 * parsing or formatting, a C ABI, binary80, or the full IEEE utility surface.
 * Java users seeking the libbid API should use {@code org.bidfp.Bid64} and
 * {@code org.bidfp.Bid128}.
 */
package org.bidfp.binary128;
