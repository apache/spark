package org.example.spark.skew;

import org.apache.spark.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Driver-side scheduling skew detector for Spark.
 *
 * Idea:
 * - Collect task durations by stageId
 * - Calculate mean and variance at stage completion
 * - If variance is significantly greater than mean, we assume there is skew
 * - Log a warning + structured event (for a "self-healing" agent)
 */
public class SkewDetectionListener extends SparkListener {

    private static final Logger LOG = LoggerFactory.getLogger(SkewDetectionListener.class);

    // stageId -> durations (ms)
    private final Map<Integer, List<Long>> stageDurations = new HashMap<>();

    private final double varianceFactorThreshold;
    private final int minTasksForAnalysis;

    public SkewDetectionListener() {
        this(1.0, 3); // default: variance > 1 * mean, min 3 tasks
    }

    public SkewDetectionListener(double varianceFactorThreshold, int minTasksForAnalysis) {
        this.varianceFactorThreshold = varianceFactorThreshold;
        this.minTasksForAnalysis = minTasksForAnalysis;
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        int stageId = taskEnd.stageId();
        long duration = taskEnd.taskInfo().duration();

        stageDurations
                .computeIfAbsent(stageId, k -> new ArrayList<>())
                .add(duration);
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        int stageId = stageCompleted.stageInfo().stageId();

        List<Long> durations = stageDurations.get(stageId);
        if (durations == null || durations.size() < minTasksForAnalysis) {
            return;
        }

        Stats stats = computeStats(durations);

        boolean skewSuspected = stats.variance > stats.mean * varianceFactorThreshold;

        if (skewSuspected) {
            // 1) Human-readable log
            LOG.warn(
                    "Scheduling skew detected in stage {}: tasks={}, meanDurationMs={}, variance={}, thresholdFactor={}",
                    stageId, durations.size(), stats.mean, stats.variance, varianceFactorThreshold
            );

            // 2) Machine-readable log
            LOG.warn("SCHEDULING_SKEW_DETECTED " +
                    "stageId={} tasks={} meanMs={} variance={} factor={}",
                    stageId, durations.size(), stats.mean, stats.variance, varianceFactorThreshold
            );
        } else {
            LOG.info(
                    "Stage {} completed without significant skew: tasks={}, meanDurationMs={}, variance={}",
                    stageId, durations.size(), stats.mean, stats.variance
            );
        }

        stageDurations.remove(stageId);
    }

    private Stats computeStats(List<Long> values) {
        int n = values.size();
        double mean = values.stream().mapToLong(v -> v).average().orElse(0.0);

        double variance = 0.0;
        for (Long v : values) {
            double diff = v - mean;
            variance += diff * diff;
        }
        variance = variance / n;

        return new Stats(mean, variance);
    }

    private static class Stats {
        final double mean;
        final double variance;

        Stats(double mean, double variance) {
            this.mean = mean;
            this.variance = variance;
        }
    }
}
