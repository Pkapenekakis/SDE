package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class OnlineMultinomialSample<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int requestedSampleSize;
    private final long itemsSeen;
    private final long positiveItemsSeen;
    private final double totalWeight;
    private final List<T> samples;
    private final List<WeightedReservoirEntry<T>> orderedReservoir;

    public OnlineMultinomialSample(int requestedSampleSize, long itemsSeen, long positiveItemsSeen,
                                   double totalWeight, List<T> samples,
                                   List<WeightedReservoirEntry<T>> orderedReservoir) {
        this.requestedSampleSize = requestedSampleSize;
        this.itemsSeen = itemsSeen;
        this.positiveItemsSeen = positiveItemsSeen;
        this.totalWeight = totalWeight;
        this.samples = Collections.unmodifiableList(new ArrayList<T>(samples));
        this.orderedReservoir = Collections.unmodifiableList(new ArrayList<WeightedReservoirEntry<T>>(orderedReservoir));
    }

    public int getRequestedSampleSize() { return requestedSampleSize; }
    public long getItemsSeen() { return itemsSeen; }
    public long getPositiveItemsSeen() { return positiveItemsSeen; }
    public double getTotalWeight() { return totalWeight; }
    public List<T> getSamples() { return samples; }
    public List<WeightedReservoirEntry<T>> getOrderedReservoir() { return orderedReservoir; }
    public boolean hasFullSample() { return samples.size() == requestedSampleSize; }
}
