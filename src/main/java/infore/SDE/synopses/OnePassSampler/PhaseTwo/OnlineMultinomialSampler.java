package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Implements the online multinomial sampler needed by One-pass* Phase 2.
 *
 * It has two stages:
 *
 * 1. During add(...):
 *    Maintain an ordered weighted reservoir sample without replacement.
 *    This uses the Efraimidis-Spirakis exponential key:
 *
 *        key = -log(U) / weight
 *
 *    Smaller key is better.
 *
 * 2. During finish():
 *    Convert the ordered without-replacement reservoir into a multinomial
 *    sample with replacement. Therefore duplicates are allowed.
 *
 * This class knows nothing about joins, root tuples, Phase 1 indexes,
 * aliases, or SDE. It only receives:
 *
 *     item + weight
 *
 * and returns a weighted with-replacement sample.
 */
public final class OnlineMultinomialSampler<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int sampleSize;
    private final long baseSeed;
    private final Random reservoirRandom;

    /*
     * Priority queue stores the current worst selected reservoir entry at the head.
     * Because smaller keys are better, the worst entry is the one with the largest key.
     */
    private final PriorityQueue<WeightedReservoirEntry<T>> reservoir;

    private long arrivalCounter;
    private long itemsSeen;
    private long positiveItemsSeen;
    private double totalWeight;

    public OnlineMultinomialSampler(int sampleSize, String seed) {
        if (sampleSize <= 0) {
            throw new IllegalArgumentException("sampleSize must be positive");
        }

        this.sampleSize = sampleSize;
        this.baseSeed = stableSeed(seed);
        this.reservoirRandom = new Random(baseSeed + 17L);
        this.reservoir = new PriorityQueue<WeightedReservoirEntry<T>>(
                sampleSize,
                worstEntryFirst()
        );
    }

    /**
     * Adds one streamed item with its sampling weight.
     *
     * weight == 0:
     *   skipped
     *
     * weight < 0, NaN, Infinity:
     *   rejected
     */
    public void add(T item, double weight) {
        itemsSeen++;

        validateWeight(weight);

        if (weight == 0.0d) {
            return;
        }

        positiveItemsSeen++;
        totalWeight = checkedAdd(totalWeight, weight);

        double key = createExponentialKey(weight);

        WeightedReservoirEntry<T> candidate =
                new WeightedReservoirEntry<T>(
                        item,
                        weight,
                        key,
                        arrivalCounter++
                );

        if (reservoir.size() < sampleSize) {
            reservoir.add(candidate);
            return;
        }

        WeightedReservoirEntry<T> currentWorst = reservoir.peek();

        if (currentWorst != null && isBetter(candidate, currentWorst)) {
            reservoir.poll();
            reservoir.add(candidate);
        }
    }

    /**
     * Returns the final multinomial sample.
     *
     * Important:
     * This method is deterministic and repeatable. It creates a fresh output RNG
     * every time, so calling finish() twice returns the same output.
     */
    public OnlineMultinomialSample<T> finish() {
        List<WeightedReservoirEntry<T>> orderedReservoir = getOrderedReservoir();

        List<T> output = new ArrayList<T>(sampleSize);
        List<WeightedReservoirEntry<T>> selectedDistinctEntries =
                new ArrayList<WeightedReservoirEntry<T>>();

        Random outputRandom = new Random(baseSeed + 31L);

        double selectedDistinctWeight = 0.0d;
        int nextOrderedReservoirIndex = 0;

        for (int sampleIndex = 0; sampleIndex < sampleSize; sampleIndex++) {
            if (orderedReservoir.isEmpty()) {
                break;
            }

            /*
             * The ordered reservoir gives us candidate new items in weighted
             * without-replacement order.
             *
             * At each output position:
             *
             * - With probability introducedWeight / totalWeight,
             *   repeat one of the already introduced entries, proportional
             *   to its weight.
             *
             * - Otherwise, introduce the next entry from the ordered reservoir.
             *
             * This produces with-replacement/multinomial output.
             */
            double u = nextPositiveDouble(outputRandom) * totalWeight;

            boolean repeatPrevious =
                    selectedDistinctWeight > 0.0d && u < selectedDistinctWeight;

            boolean noMoreReservoirEntries =
                    nextOrderedReservoirIndex >= orderedReservoir.size();

            WeightedReservoirEntry<T> selected;
            if (repeatPrevious || noMoreReservoirEntries) {
                selected = drawFromIntroduced(
                        selectedDistinctEntries,
                        selectedDistinctWeight,
                        outputRandom
                );

            } else {
                selected = orderedReservoir.get(nextOrderedReservoirIndex);

                nextOrderedReservoirIndex++;

                selectedDistinctEntries.add(selected);
                selectedDistinctWeight = checkedAdd(
                        selectedDistinctWeight,
                        selected.getWeight()
                );

            }
            output.add(selected.getItem());
        }

        return new OnlineMultinomialSample<T>(
                sampleSize,
                itemsSeen,
                positiveItemsSeen,
                totalWeight,
                output,
                orderedReservoir
        );
    }

    /**
     * Returns the intermediate weighted reservoir in best-to-worst order.
     */
    public List<WeightedReservoirEntry<T>> getOrderedReservoir() {
        List<WeightedReservoirEntry<T>> ordered =
                new ArrayList<>(reservoir);

        ordered.sort(bestEntryFirst());

        return ordered;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public long getItemsSeen() {
        return itemsSeen;
    }

    public long getPositiveItemsSeen() {
        return positiveItemsSeen;
    }

    public double getTotalWeight() {
        return totalWeight;
    }

    private double createExponentialKey(double weight) {
        double u = nextPositiveDouble(reservoirRandom);
        return -Math.log(u) / weight;
    }

    private WeightedReservoirEntry<T> drawFromIntroduced(
            List<WeightedReservoirEntry<T>> introducedEntries,
            double introducedWeight,
            Random outputRandom) {

        if (introducedEntries.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot draw from an empty introduced set"
            );
        }

        double u = nextPositiveDouble(outputRandom) * introducedWeight;
        double cumulative = 0.0d;

        for (WeightedReservoirEntry<T> entry : introducedEntries) {
            cumulative += entry.getWeight();

            if (u < cumulative) {
                return entry;
            }
        }

        /*
         * Numerical fallback for rare floating-point boundary cases.
         */
        return introducedEntries.get(introducedEntries.size() - 1);
    }

    private static void validateWeight(double weight) {
        if (Double.isNaN(weight) || Double.isInfinite(weight)) {
            throw new IllegalArgumentException("weight must be finite");
        }

        if (weight < 0.0d) {
            throw new IllegalArgumentException("weight must be non-negative");
        }
    }

    private static double checkedAdd(double left, double right) {
        double result = left + right;

        if (Double.isNaN(result) || Double.isInfinite(result)) {
            throw new IllegalArgumentException("weight sum is invalid");
        }

        return result;
    }

    private static double nextPositiveDouble(Random random) {
        double u = random.nextDouble();

        while (u <= 0.0d) {
            u = random.nextDouble();
        }

        return u;
    }

    private static boolean isBetter(
            WeightedReservoirEntry<?> candidate,
            WeightedReservoirEntry<?> currentWorst) {

        int byKey = Double.compare(candidate.getKey(), currentWorst.getKey());

        if (byKey != 0) {
            return byKey < 0;
        }

        return candidate.getArrivalOrder() < currentWorst.getArrivalOrder();
    }

    private static long stableSeed(String seed) {
        String s = seed == null ? "" : seed;

        long h = 1125899906842597L;

        for (int i = 0; i < s.length(); i++) {
            h = 31L * h + s.charAt(i);
        }

        return h;
    }

    private static <T> Comparator<WeightedReservoirEntry<T>> bestEntryFirst() {
        return new Comparator<WeightedReservoirEntry<T>>() {
            public int compare(
                    WeightedReservoirEntry<T> left,
                    WeightedReservoirEntry<T> right) {

                int byKey = Double.compare(left.getKey(), right.getKey());

                if (byKey != 0) {
                    return byKey;
                }

                return Long.compare(
                        left.getArrivalOrder(),
                        right.getArrivalOrder()
                );
            }
        };
    }

    private static <T> Comparator<WeightedReservoirEntry<T>> worstEntryFirst() {
        return new Comparator<WeightedReservoirEntry<T>>() {
            public int compare(
                    WeightedReservoirEntry<T> left,
                    WeightedReservoirEntry<T> right) {

                int byKey = Double.compare(right.getKey(), left.getKey());

                if (byKey != 0) {
                    return byKey;
                }

                return Long.compare(
                        right.getArrivalOrder(),
                        left.getArrivalOrder()
                );
            }
        };
    }
}