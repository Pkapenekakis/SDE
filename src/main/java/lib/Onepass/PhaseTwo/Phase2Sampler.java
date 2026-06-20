package lib.Onepass.PhaseTwo;

import com.fasterxml.jackson.databind.JsonNode;
import javassist.tools.rmi.Sample;

import java.util.*;

/**
 * Phase 2 sampler that implements weighted reservoir sampling
 * using the Efraimidis–Spirakis method:
 *  For each tuple i with weight w_i:
 *      - draw U ~ Uniform(0,1)
 *      - compute key K_i = U^(1 / w_i)
 *  Keep the tuples with the smallest K_i values in a reservoir of fixed size.
 */
public class Phase2Sampler {

    private static class SampleEntry implements Comparable<SampleEntry> {
        final double K;
        final JsonNode tuple;

        SampleEntry(double K, JsonNode tuple) {
            this.K = K;
            this.tuple = tuple;
        }

        @Override
        public int compareTo(SampleEntry other) {
            return Double.compare(other.K, this.K); // max-heap behavior if needed
        }

        @Override
        public String toString() {
            return "SampleEntry{K=" + K + "}";
        }
    }

    private final int sampleSize;
    private final PriorityQueue<SampleEntry> reservoir;
    private final Random random;

    public Phase2Sampler(int sampleSize) {
        this.sampleSize = sampleSize;
        // Max-heap based on K: at the top is the greatest K
        this.reservoir = new PriorityQueue<>(Comparator.comparingDouble(e -> -e.K));
        this.random = new Random();
    }

    /**
     * Adds a new tuple with the given weight based on efraimidis-spirakis.
     */
    public void addTuple(double weight, JsonNode fullTuple) {
        if (weight <= 0.0) {
            return;
        }

        double u = random.nextDouble();
        if (u == 0.0) {
            u = Double.MIN_VALUE;
        }

        double K = Math.pow(u, 1.0 / weight);

        if (reservoir.size() < sampleSize) {
            reservoir.add(new SampleEntry(K, fullTuple.deepCopy()));
        } else {
            SampleEntry worst = reservoir.peek();
            if (worst != null && K < worst.K) {
                reservoir.poll();
                reservoir.add(new SampleEntry(K, fullTuple.deepCopy()));
            }
        }
        //System.out.println("[Phase2Sampler] addTuple weight=" + weight +"\t K: " + K);

        /* Debug code when the final node proccessed had weight of 10
        if (weight == 10) {
            System.out.println("Reservoir (priority order):");
            PriorityQueue<SampleEntry> copy = new PriorityQueue<>(reservoir);
            while (!copy.isEmpty()) {
                System.out.println(copy.poll()); // uses SampleEntry.toString()
            }

        } */
    }

    /**
     * Returns current sample as JsonNode.
     */
    public List<JsonNode> getCurrentSample() {
        List<JsonNode> out = new ArrayList<>(reservoir.size());
        for (SampleEntry e : reservoir) {
            out.add(e.tuple);
        }
        return out;
    }

    /**
     * For now just return full sample.
     */
    public Object estimate(String[] params) {
        return getCurrentSample();
    }
}
