package infore.SDE.synopses.OnePassSampler.PhaseTwo;

public final class OnlineMultinomialSampler<T> {
    private final int sampleSize;

    public OnlineMultinomialSampler(int sampleSize, String seed) {
        if (sampleSize <= 0) {
            throw new IllegalArgumentException("sampleSize must be positive");
        }
        this.sampleSize = sampleSize;
    }

    public int getSampleSize() {
        return sampleSize;
    }
}
