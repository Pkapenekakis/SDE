package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import java.io.Serializable;

public final class WeightedReservoirEntry<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final T item;
    private final double weight;
    private final double key;
    private final long arrivalOrder;

    public WeightedReservoirEntry(T item, double weight, double key, long arrivalOrder) {
        this.item = item;
        this.weight = weight;
        this.key = key;
        this.arrivalOrder = arrivalOrder;
    }

    public T getItem() {
        return item;
    }

    public double getWeight() {
        return weight;
    }

    public double getKey() {
        return key;
    }

    public long getArrivalOrder() {
        return arrivalOrder;
    }

    @Override
    public String toString() {
        return "WeightedReservoirEntry{" +
                "weight=" + weight +
                ", key=" + key +
                ", arrivalOrder=" + arrivalOrder +
                '}';
    }
}
