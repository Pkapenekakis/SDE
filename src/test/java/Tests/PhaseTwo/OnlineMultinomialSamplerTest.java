package Tests.PhaseTwo;

import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnlineMultinomialSample;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnlineMultinomialSampler;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OnlineMultinomialSamplerTest {

    @Test
    public void skipsZeroWeights() {
        OnlineMultinomialSampler<String> sampler =
                new OnlineMultinomialSampler<String>(10, "zero-test");

        sampler.add("zero", 0.0d);
        sampler.add("one", 1.0d);

        OnlineMultinomialSample<String> result = sampler.finish();

        assertEquals(2L, result.getItemsSeen());
        assertEquals(1L, result.getPositiveItemsSeen());
        assertEquals(1.0d, result.getTotalWeight(), 0.0d);
        assertEquals(10, result.getSamples().size());

        for (String value : result.getSamples()) {
            assertEquals("one", value);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeWeights() {
        OnlineMultinomialSampler<String> sampler =
                new OnlineMultinomialSampler<String>(5, "negative-test");

        sampler.add("bad", -1.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNaNWeights() {
        OnlineMultinomialSampler<String> sampler =
                new OnlineMultinomialSampler<String>(5, "nan-test");

        sampler.add("bad", Double.NaN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsInfiniteWeights() {
        OnlineMultinomialSampler<String> sampler =
                new OnlineMultinomialSampler<String>(5, "infinity-test");

        sampler.add("bad", Double.POSITIVE_INFINITY);
    }

    @Test
    public void sameSeedProducesSameResult() {
        OnlineMultinomialSampler<String> left =
                new OnlineMultinomialSampler<String>(20, "same-seed");

        OnlineMultinomialSampler<String> right =
                new OnlineMultinomialSampler<String>(20, "same-seed");

        for (int i = 0; i < 30; i++) {
            left.add("item-" + i, i + 1.0d);
            right.add("item-" + i, i + 1.0d);
        }

        assertEquals(left.finish().getSamples(), right.finish().getSamples());
    }

    @Test
    public void repeatedFinishReturnsSameResult() {
        OnlineMultinomialSampler<String> sampler =
                new OnlineMultinomialSampler<String>(20, "repeatable-finish");

        for (int i = 0; i < 30; i++) {
            sampler.add("item-" + i, i + 1.0d);
        }

        List<String> first = sampler.finish().getSamples();
        List<String> second = sampler.finish().getSamples();

        assertEquals(first, second);
    }

    @Test
    public void outputAllowsDuplicates() {
        OnlineMultinomialSampler<String> sampler =
                new OnlineMultinomialSampler<String>(50, "duplicate-test");

        sampler.add("A", 1.0d);
        sampler.add("B", 9.0d);

        List<String> samples = sampler.finish().getSamples();

        assertEquals(50, samples.size());
        assertEquals(50, count(samples, "A") + count(samples, "B"));

        assertTrue(
                "Expected at least one duplicate because this is with-replacement sampling",
                count(samples, "A") > 1 || count(samples, "B") > 1
        );
    }

    @Test
    public void distributionIsReasonableForWeightsOneAndNine() {
        OnlineMultinomialSampler<String> sampler =
                new OnlineMultinomialSampler<String>(10000, "distribution-test");

        sampler.add("light", 1.0d);
        sampler.add("heavy", 9.0d);

        List<String> samples = sampler.finish().getSamples();

        double lightRatio = count(samples, "light") / 10000.0d;
        double heavyRatio = count(samples, "heavy") / 10000.0d;

        assertTrue(
                "Expected light item ratio near 0.1, got " + lightRatio,
                lightRatio > 0.06d && lightRatio < 0.14d
        );

        assertTrue(
                "Expected heavy item ratio near 0.9, got " + heavyRatio,
                heavyRatio > 0.86d && heavyRatio < 0.94d
        );
    }

    private static int count(List<String> values, String target) {
        int c = 0;

        for (String value : values) {
            if (target.equals(value)) {
                c++;
            }
        }

        return c;
    }
}