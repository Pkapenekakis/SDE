package infore.SDE.synopses.OnePassSampler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.synopses.Synopsis;
import lib.Onepass.PhaseTwo.Phase2Sampler;

public class OnePassPhaseTwo extends Synopsis {

    private final String joinKeyIndex;
    private final String groupIdIndex;
    private final String weightIndex;
    private final int sampleSize;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Phase2Sampler sampler;

    /**
     * parameters layout:
     *  [0] = keyIndex      (field name για key στο SDE)
     *  [1] = valueIndex    (field name για value στο SDE – αν χρειάζεται)
     *  [2] = joinKey field name  (μέσα στο inner JSON)
     *  [3] = group_id field name (μέσα στο inner JSON)
     *  [4] = weight field name   (μέσα στο inner JSON)
     *  [5] = sampleSize          (μέγεθος reservoir)
     */
    public OnePassPhaseTwo(int ID, String[] parameters) {
        super(ID, parameters[0], parameters[1]);

        this.joinKeyIndex = parameters[2];
        this.groupIdIndex = parameters[3];
        this.weightIndex  = parameters[4];
        this.sampleSize   = Integer.parseInt(parameters[5]);

        this.sampler = new Phase2Sampler(this.sampleSize);
    }

    @Override
    public void add(Object k) {
        //System.out.println("[OnePassPhaseTwo] add() called");
        JsonNode node = (JsonNode) k;

        //System.out.println("[OnePassPhaseTwo] node = " + node.toString());
        //System.out.println("[OnePassPhaseTwo] weightIndex = " + weightIndex);

        // If "node" is actually a JSON string, parse it into an object node
        if (node != null && node.isTextual()) {
            try {
                node = mapper.readTree(node.asText());
            } catch (Exception e) {
                System.out.println("[OnePassPhaseTwo] Failed to parse values JSON string: " + node.asText());
                return;
            }
        }

        JsonNode wNode = node.get(weightIndex);
        if (wNode == null || wNode.isNull()) {
            System.out.println("[OnePassPhaseTwo] Invalid weight: " + wNode + " node=" + node);
            return;
        }

        double w = wNode.asDouble();
        if (w <= 0.0) {
            return;
        }

        //Efraimidis–Spirakis logic is in the sampler
        sampler.addTuple(w, node);
    }

    @Override
    public Object estimate(Object k) {
        return null;
    }

    @Override
    public Estimation estimate(Request rq) {
        String[] params = rq.getParam();
        Object result = sampler.estimate(params);  //for now just the sample

        String uidString = Integer.toString(rq.getUID());
        return new Estimation(rq, result, uidString);
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        // TODO: merge for distributed
        return null;
    }
}
