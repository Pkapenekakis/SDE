package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.MapFunction;

/** Converts the special state-transfer Estimation payload to State Topic JSON. */
public final class OnePassStateTransferToJson
        implements MapFunction<Estimation, String> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String map(Estimation value) throws Exception {
        Object payload = value.getEstimation();

        if (payload instanceof String) {
            JsonNode parsed = MAPPER.readTree((String) payload);
            return MAPPER.writeValueAsString(parsed);
        }

        if (payload instanceof JsonNode) {
            return MAPPER.writeValueAsString(payload);
        }

        return MAPPER.writeValueAsString(MAPPER.valueToTree(payload));
    }
}