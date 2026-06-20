package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OnlineMultinomialSamplerTest {

    @Test
    public void skipsZeroWeights() {
        OnlineMultinomialSampler<String> sampler = new OnlineMultinomialSampler<String>(10, "zero-test");
        sampler.add("zero", 0.0d);
        sampler.add("one", 1.0d);
        OnlineMultinomialSample<String> result = sampler.finish();
        assertEquals(2L, result.getItemsSeen());
        assertEquals(1L, result.getPositiveItemsSeen());
        assertEquals(10, result.getSamples().size());
        for (String value : result.getSamples()) {
            assertEquals("one", value);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeWeights() {
        OnlineMultinomialSampler<String> sampler = new OnlineMultinomialSampler<String>(2, "negative-test");
        sampler.add("x", -1.0d);
    }

    @Test
    public void allowsDuplicatesInOutput() {
        OnlineMultinomialSampler<String> sampler = new OnlineMultinomialSampler<String>(50, "duplicate-test");
        sampler.add("A", 1.0d);
        sampler.add("B", 9.0d);
        List<String> samples = sampler.finish().getSamples();
        assertEquals(50, samples.size());
        assertTrue(count(samples, "A") + count(samples, "B") == 50);
        assertTrue(count(samples, "A") > 1 || count(samples, "B") > 1);
    }

    @Test
    public void sameSeedProducesSameResult() {
        OnlineMultinomialSampler<String> left = new OnlineMultinomialSampler<String>(20, "same-seed");
        OnlineMultinomialSampler<String> right = new OnlineMultinomialSampler<String>(20, "same-seed");
        for (int i = 0; i < 30; i++) {
            left.add("item-" + i, i + 1.0d);
            right.add("item-" + i, i + 1.0d);
        }
        assertEquals(left.finish().getSamples(), right.finish().getSamples());
    }

    private static int count(List<String> values, String target) {
        int c = 0;
        for (String value : values) {
            if (target.equals(value)) c++;
        }
        return c;
    }
}
