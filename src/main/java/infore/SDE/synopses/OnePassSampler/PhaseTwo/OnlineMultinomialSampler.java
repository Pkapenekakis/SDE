package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

public final class OnlineMultinomialSampler<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int sampleSize;
    private final Random reservoirRandom;
    private final Random outputRandom;
    private final PriorityQueue<WeightedReservoirEntry<T>> reservoir;
    private long arrivalCounter;
    private long itemsSeen;
    private long positiveItemsSeen;
    private double totalWeight;

    public OnlineMultinomialSampler(int sampleSize, String seed) {
        if (sampleSize <= 0) throw new IllegalArgumentException("sampleSize must be positive");
        this.sampleSize = sampleSize;
        long baseSeed = stableSeed(seed);
        this.reservoirRandom = new Random(baseSeed + 17L);
        this.outputRandom = new Random(baseSeed + 31L);
        this.reservoir = new PriorityQueue<WeightedReservoirEntry<T>>(sampleSize, worstFirst());
    }

    public void add(T item, double weight) {
        itemsSeen++;
        validateWeight(weight);
        if (weight == 0.0d) return;
        positiveItemsSeen++;
        totalWeight = checkedAdd(totalWeight, weight);
        WeightedReservoirEntry<T> entry = new WeightedReservoirEntry<T>(item, weight,
                -Math.log(nextPositiveDouble(reservoirRandom)) / weight, arrivalCounter++);
        if (reservoir.size() < sampleSize) {
            reservoir.add(entry);
        } else if (bestFirst().compare(entry, reservoir.peek()) < 0) {
            reservoir.poll();
            reservoir.add(entry);
        }
    }

    public OnlineMultinomialSample<T> finish() {
        List<WeightedReservoirEntry<T>> ordered = getOrderedReservoir();
        List<T> out = new ArrayList<T>(sampleSize);
        List<WeightedReservoirEntry<T>> introduced = new ArrayList<WeightedReservoirEntry<T>>();
        double introducedWeight = 0.0d;
        int next = 0;
        for (int j = 0; j < sampleSize; j++) {
            if (ordered.isEmpty()) break;
            double u = nextPositiveDouble(outputRandom) * totalWeight;
            if ((introducedWeight > 0.0d && u < introducedWeight) || next >= ordered.size()) {
                out.add(drawIntroduced(introduced, introducedWeight).getItem());
            } else {
                WeightedReservoirEntry<T> selected = ordered.get(next++);
                introduced.add(selected);
                introducedWeight = checkedAdd(introducedWeight, selected.getWeight());
                out.add(selected.getItem());
            }
        }
        return new OnlineMultinomialSample<T>(sampleSize, itemsSeen, positiveItemsSeen, totalWeight, out, ordered);
    }

    public List<WeightedReservoirEntry<T>> getOrderedReservoir() {
        List<WeightedReservoirEntry<T>> ordered = new ArrayList<WeightedReservoirEntry<T>>(reservoir);
        Collections.sort(ordered, bestFirst());
        return ordered;
    }

    public int getSampleSize() { return sampleSize; }
    public long getItemsSeen() { return itemsSeen; }
    public long getPositiveItemsSeen() { return positiveItemsSeen; }
    public double getTotalWeight() { return totalWeight; }

    private WeightedReservoirEntry<T> drawIntroduced(List<WeightedReservoirEntry<T>> introduced, double weightSum) {
        if (introduced.isEmpty()) throw new IllegalStateException("No introduced entries available");
        double u = nextPositiveDouble(outputRandom) * weightSum;
        double cumulative = 0.0d;
        for (WeightedReservoirEntry<T> entry : introduced) {
            cumulative += entry.getWeight();
            if (u < cumulative) return entry;
        }
        return introduced.get(introduced.size() - 1);
    }

    private static void validateWeight(double weight) {
        if (Double.isNaN(weight) || Double.isInfinite(weight)) throw new IllegalArgumentException("weight must be finite");
        if (weight < 0.0d) throw new IllegalArgumentException("weight must be non-negative");
    }

    private static double checkedAdd(double a, double b) {
        double result = a + b;
        if (Double.isNaN(result) || Double.isInfinite(result)) throw new IllegalArgumentException("weight sum is invalid");
        return result;
    }

    private static double nextPositiveDouble(Random random) {
        double u = random.nextDouble();
        while (u <= 0.0d) u = random.nextDouble();
        return u;
    }

    private static long stableSeed(String seed) {
        String s = seed == null ? "" : seed;
        long h = 1125899906842597L;
        for (int i = 0; i < s.length(); i++) h = 31L * h + s.charAt(i);
        return h;
    }

    private static <T> Comparator<WeightedReservoirEntry<T>> bestFirst() {
        return new Comparator<WeightedReservoirEntry<T>>() {
            public int compare(WeightedReservoirEntry<T> a, WeightedReservoirEntry<T> b) {
                int byKey = Double.compare(a.getKey(), b.getKey());
                if (byKey != 0) return byKey;
                return Long.compare(a.getArrivalOrder(), b.getArrivalOrder());
            }
        };
    }

    private static <T> Comparator<WeightedReservoirEntry<T>> worstFirst() {
        return new Comparator<WeightedReservoirEntry<T>>() {
            public int compare(WeightedReservoirEntry<T> a, WeightedReservoirEntry<T> b) {
                int byKey = Double.compare(b.getKey(), a.getKey());
                if (byKey != 0) return byKey;
                return Long.compare(b.getArrivalOrder(), a.getArrivalOrder());
            }
        };
    }
}
