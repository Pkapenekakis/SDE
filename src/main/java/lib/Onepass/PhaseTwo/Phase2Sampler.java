package lib.Onepass.PhaseTwo;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnlineMultinomialSample;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnlineMultinomialSampler;

import java.util.ArrayList;
import java.util.List;

public class Phase2Sampler {

    private final OnlineMultinomialSampler<JsonNode> sampler;

    public Phase2Sampler(int sampleSize) {
        this(sampleSize, "legacy-phase-two");
    }

    public Phase2Sampler(int sampleSize, String seed) {
        this.sampler = new OnlineMultinomialSampler<JsonNode>(sampleSize, seed);
    }

    public void addTuple(double weight, JsonNode fullTuple) {
        if (fullTuple == null) {
            return;
        }
        sampler.add(fullTuple.deepCopy(), weight);
    }

    public List<JsonNode> getCurrentSample() {
        OnlineMultinomialSample<JsonNode> sample = sampler.finish();
        List<JsonNode> out = new ArrayList<JsonNode>(sample.getSamples().size());
        for (JsonNode node : sample.getSamples()) {
            out.add(node.deepCopy());
        }
        return out;
    }

    public Object estimate(String[] params) {
        return getCurrentSample();
    }
}
