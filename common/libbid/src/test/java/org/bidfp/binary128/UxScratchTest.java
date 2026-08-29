/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

final class UxScratchTest {
  private static final RoundingMode RN = RoundingMode.TIES_TO_EVEN;

  @Test
  void nestedAcquisitionsUseDistinctFramesAndReuseByDepth() {
    UxScratch.Frame outer = UxScratch.acquire();
    UxScratch.Frame inner;
    try {
      inner = UxScratch.acquire();
      try {
        assertNotSame(outer, inner);
        assertNotSame(outer.unpacked(0), inner.unpacked(0));
      } finally {
        UxScratch.release(inner);
      }
    } finally {
      UxScratch.release(outer);
    }

    UxScratch.Frame reused = UxScratch.acquire();
    try {
      assertEquals(outer, reused);
    } finally {
      UxScratch.release(reused);
    }
  }

  @Test
  void nestedKernelsAreStableAcrossThreads() throws Exception {
    Binary128 gammaInput = Binary128.fromBinary64(-2.25);
    Binary128 powBase = Binary128.fromBinary64(1.125);
    Binary128 powExponent = Binary128.fromBinary64(-3.75);
    Binary128 angle = Binary128.fromBinary64(123456.75);
    Binary128 expectedGamma = Dpml.tgamma(gammaInput, RN, new StatusFlags());
    Binary128 expectedPow = Dpml.pow(powBase, powExponent, RN, new StatusFlags());
    Binary128 expectedSin = Dpml.sin(angle, RN, new StatusFlags());

    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Callable<Void>> tasks = new ArrayList<>();
      for (int thread = 0; thread < 16; thread++) {
        tasks.add(() -> {
          for (int iteration = 0; iteration < 1_000; iteration++) {
            assertEquals(expectedGamma, Dpml.tgamma(gammaInput, RN, new StatusFlags()));
            assertEquals(expectedPow, Dpml.pow(
                powBase, powExponent, RN, new StatusFlags()));
            assertEquals(expectedSin, Dpml.sin(angle, RN, new StatusFlags()));
          }
          return null;
        });
      }
      for (Future<Void> future : executor.invokeAll(tasks)) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
