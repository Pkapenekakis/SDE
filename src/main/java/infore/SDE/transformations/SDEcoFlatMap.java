package infore.SDE.transformations;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.synopses.OnePassSampler.OnePassSamplerSdeSynopsis;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOne;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassRequestParser;
import lib.WDFT.controlBucket;
import lib.WLSH.Bucket;
import infore.SDE.synopses.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.messages.Datapoint;

public class SDEcoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Estimation> {

	private static final long serialVersionUID = 1L;
	private HashMap<String,ArrayList<Synopsis>> M_Synopses = new HashMap<>();
	private HashMap<String,ArrayList<ContinuousSynopsis>> MC_Synopses = new HashMap<>();
	private HashMap<String, Map<Integer, JsonNode>> onePassGlobalStateChunksByRef = new HashMap<String, Map<Integer, JsonNode>>();
	private HashMap<String, JsonNode> onePassGlobalStatesByRef = new HashMap<String, JsonNode>();
	private HashMap<String, Request> pendingOnePassInstallRequestsByRef = new HashMap<String, Request>();
	private int pId;
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final String ONEPASS_DATA_BARRIER_FIELD = "__onePassDataBarrier";
	private static final int ONEPASS_DATA_BARRIER_REQUEST_ID = 70;
	private static final int ONEPASS_SYNOPSIS_ID = 30;

	@Override
	public void flatMap1(Datapoint node, Collector<Estimation> collector) throws JsonProcessingException {
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(node.getKey());

		if (isOnePassGlobalStateChunk(node)) {
			handleOnePassGlobalStateChunk(node, Synopses, collector);
			return;
		}
		/*
		 * One-pass* data-path barrier.
		 *
		 * The test sends this marker through dataTopic after all tuples of a phase
		 * or Phase 3 alias. Since it goes through the same data path, receiving this
		 * ACK means all earlier records for the same Kafka key/partition have reached
		 * this operator.
		 */
		if (isOnePassDataBarrier(node)) {
			handleOnePassDataBarrier(node, Synopses, collector);
			return;
		}

		if (Synopses != null) {
			for (Synopsis ski : Synopses) {
				ski.add(node.getValues());
			}
		M_Synopses.put(node.getKey(),Synopses);
		} else{
			System.out.println("[SDEcoFlatMap DATA] No synopsis found for datapoint key=" + node.getKey() +
					", known keys=" + M_Synopses.keySet());
		}
		ArrayList<ContinuousSynopsis>  C_Synopses =  MC_Synopses.get(node.getKey());
		if (C_Synopses != null) {

			for (ContinuousSynopsis c_ski : C_Synopses) {

				Estimation e =c_ski.addEstimate(node.getValues());
				if(e!=null){
					if(e.getEstimation()!=null)
						collector.collect(e);
				}
				//Radius_Grid rg = (Radius_Grid)c_ski;
				//rg.add_and_provide_estimates(node);
			}
		MC_Synopses.put(node.getKey(),C_Synopses);
		}
		//System.out.println("[SDEcoFlatMap] flatMap1 got datapoint key=" + node.getKey());
	}

	@Override
	public void flatMap2(Request rq, Collector<Estimation> collector) throws Exception {
		System.out.println("[SDEcoFlatMap REQUEST] requestID=" + rq.getRequestID()
						+ ", synopsisID=" + rq.getSynopsisID() + ", uid=" + rq.getUID()
						+ ", key=" + rq.getKey() + ", known keys=" + M_Synopses.keySet());
		System.out.println(rq.toString());
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(rq.getKey());
		ArrayList<ContinuousSynopsis>  C_Synopses =  MC_Synopses.get(rq.getKey());

		if (rq.getRequestID() == 1 || rq.getRequestID() == 4 ) {
			if(Synopses==null){
				Synopses = new ArrayList<>();
				C_Synopses = new ArrayList<>();
			}

		Synopsis sketch = null;
		switch (rq.getSynopsisID()) {
			// countMin
			case 1:
				if (rq.getParam().length > 4)
					sketch = new CountMin(rq.getUID(), rq.getParam());
				//{ "1", "2", "0.0002", "0.99", "4" };
				Synopses.add(sketch);
			break;
			// BloomFliter
			case 2:
				if (rq.getParam().length > 3)
					sketch = new Bloomfilter(rq.getUID(), rq.getParam());
				//	String[] _tmp = { "1", "1", "100000", "0.0002" };
				Synopses.add(sketch);
			break;
			// AMS sketch
			case 3:
				if (rq.getParam().length > 3)
					sketch = new AMSsynopsis(rq.getUID(), rq.getParam());
				//	String[] _tmp = { "1", "2", "1000", "10" };
				Synopses.add(sketch);
			break;
			// DFT
			case 4:
				if (rq.getParam().length > 3)
					sketch = new MultySynopsisDFT(rq.getUID(), rq.getParam());
				//String[] _tmp = {"1", "2", "5", "30", "8"};
				Synopses.add(sketch);
			break;
			//LSH - unfinished
			case 5:
				sketch = new Bloomfilter(rq.getUID(), rq.getParam());
				Synopses.add(sketch);

			break;
			// lib.Coresets
			case 6:
				if (rq.getParam().length > 10)
					sketch = new FinJoinCoresets(rq.getUID(), rq.getParam());
				//	String[] _tmp = { "1","2", "5", "10" };
				Synopses.add(sketch);
			break;
			// HyperLogLog
			case 7:
				if (rq.getParam().length > 2)
					sketch = new HyperLogLogSynopsis(rq.getUID(), rq.getParam());
				//String[] _tmp = { "1", "1", "0.001" };
				Synopses.add(sketch);
			break;
			// StickySampling
			case 8:

				if (rq.getParam().length > 4)
					sketch = new StickySamplingSynopsis(rq.getUID(), rq.getParam());
				//String[] _tmp = { "1", "2", "0.01", "0.01", "0.0001"};
				Synopses.add(sketch);
			break;
			// LossyCounting
			case 9:

				if (rq.getParam().length > 2)
					sketch = new LossyCountingSynopsis(rq.getUID(), rq.getParam());
				//String[] _tmp = { "1", "2", "0.0001" };

				Synopses.add(sketch);
			break;
			// ChainSampler
			case 10:

				if (rq.getParam().length > 3)
					sketch = new ChainSamplerSynopsis(rq.getUID(), rq.getParam());
				//String[] _tmp = { "2", "2", "1000", "100000" };
				Synopses.add(sketch);
			break;
			// GKQuantiles
			case 11:

				if (rq.getParam().length > 3)
					sketch = new GKsynopsis(rq.getUID(), rq.getParam());
				//String[] _tmp = { "2", "2", "0.01"};
				Synopses.add(sketch);
			break;
			// lib.TopK
			case 13:
				if (rq.getParam().length > 3)
					sketch = new SynopsisTopK(rq.getUID(), rq.getParam());
				//String[] _tmp = { "2", "2", "0.01"};
				Synopses.add(sketch);
				System.out.println("Synopses Added");
			break;
			// windowQuantiles
			case 16:
				if (rq.getParam().length > 3)
					sketch = new windowQuantiles(rq.getUID(), rq.getParam());
				//String[] _tmp = { "2", "2", "0.01"};
				Synopses.add(sketch);
			break;
			// 6-> dynamic load sketch
			case 25:

				Object instance;

				if (rq.getParam().length == 4) {

					File myJar = new File(rq.getParam()[2]);
					URLClassLoader child = new URLClassLoader(new URL[]{myJar.toURI().toURL()},
					this.getClass().getClassLoader());
					Class<?> classToLoad = Class.forName(rq.getParam()[3], true, child);
					instance = classToLoad.getConstructor().newInstance();
					Synopses.add((Synopsis) instance);

				} else {

					File myJar = new File("C:\\Users\\ado.kontax\\Desktop\\flinkSketches.jar");
					URLClassLoader child = new URLClassLoader(new URL[]{myJar.toURI().toURL()},
					this.getClass().getClassLoader());
					Class<?> classToLoad = Class.forName("com.yahoo.sketches.sampling.NewSketch", true, child);
					instance = classToLoad.getConstructor().newInstance();
					Synopses.add((Synopsis) instance);

				}
			break;
			// FINJOIN
			case 26:

				if (rq.getParam().length > 3)
					sketch = new FinJoinSynopsis(rq.getUID(), rq.getParam());
				//String[] _tmp = { "0", "0", "10", "100", "8", "3" };
				Synopses.add(sketch);

			break;
			// COUNT
			case 27:

				if (rq.getParam().length > 3)
					sketch = new Counters(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = {"0", "0", "10", "100", "8", "3"};
					sketch = new Counters(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);
			break;
			//window lsh
			case 28:
				System.out.println("ADD-> _ " +rq.toString());
				if (rq.getParam().length > 3)
					sketch = new WLSHSynopses(rq.getUID(), rq.getParam());

				Synopses.add(sketch);
			break;
			//window pastDFT
			case 29:
				System.out.println("ADD-> _ " +rq.toString());
				if (rq.getParam().length > 3)
					sketch = new PastDFTSynopsis(rq.getUID(), rq.getParam());
				Synopses.add(sketch);
				break;
			//One-Pass
			case 30:
				System.out.println("ADD -> OnePassSamplerSdeSynopsis " + rq.toString());
				sketch = new OnePassSamplerSdeSynopsis(rq.getUID(), rq);
				Synopses.add(sketch);

				System.out.println("OnePassSamplerSdeSynopsis added for uid=" + rq.getUID() + ", key="+ rq.getKey());

				break;
			case 31:
				System.out.println("ADD -> OnePassPhaseOne " + rq.toString());

				OnePassParams params = OnePassRequestParser.parse(rq);
				CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);

				sketch = new OnePassPhaseOne(rq.getUID(), plan, params.getWeight());
				Synopses.add(sketch);

				System.out.println("OnePassPhaseOne added for uid=" + rq.getUID() + ", key=" + rq.getKey() +
						", queryName=" + params.getQueryName());

				break;
		}
			M_Synopses.put(rq.getKey(),Synopses);
		}
	//Continuous Synopsis
	else if(rq.getRequestID() == 5) {

			if (C_Synopses == null){
				C_Synopses = new ArrayList<>();
			}
			ContinuousSynopsis sketch = null;

			switch (rq.getSynopsisID()) {

				case 1:
					if (rq.getParam().length > 4)
						sketch = new ContinuousCM(rq.getUID(), rq, rq.getParam());
						//String[] _tmp = { "StockID", "Volume", "0.0002", "0.99", "4" };
						C_Synopses.add(sketch);
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				// RadiusSketch
				case 100:
					if (rq.getParam().length > 4)
						sketch = new Radius_Grid(rq);
					C_Synopses.add(sketch);
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				case 12:
					rq.setNoOfP(1);
					if (rq.getParam().length > 5)
						sketch = new ContinuousMaritimeSketches(rq.getUID(), rq, rq.getParam());
					//String[] _tmp = {"1", "1", "18000","10000","50","50"};
					C_Synopses.add(sketch);
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				case 15:
					if (rq.getParam().length > 5)
						sketch = new ISWoR(rq.getUID(), rq, rq.getParam());
					//String[] _tmp = {"1", "1", "18000","10000","50","50"};
					C_Synopses.add(sketch);
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;

			}
		}
		// OnePass* UPDATE handling for Phase Transitions
		else if (rq.getRequestID() == 7) {
			if (Synopses == null) {
				System.out.println("create Synopses first before OnePass UPDATE");
				return;
			}

			boolean handled = false;

			String command = "UNKNOWN";

			if (rq.getParam() != null && rq.getParam().length > 0) {
				command = rq.getParam()[0];
			}

			if ("INSTALL_GLOBAL_INDEX".equalsIgnoreCase(command)) {
				handleInstallGlobalIndexRequest(rq, Synopses, collector);
				M_Synopses.put(rq.getKey(), Synopses);
				return;
			}

			for (Synopsis syn : Synopses) {
				if (rq.getUID() == syn.getSynopsisID()) {
					if (syn instanceof OnePassSamplerSdeSynopsis) {
						OnePassSamplerSdeSynopsis onePass =
								(OnePassSamplerSdeSynopsis) syn;

						/*
						 * This mutates the internal OnePass lifecycle:
						 *
						 *   FINISH_PHASE_1 -> internal transition to PHASE_2
						 *   FINISH_PHASE_2 -> internal finalization
						 *
						 * We do NOT depend on the Kafka estimation output to carry
						 * the OnePass internal result. Kafka output is only used as
						 * a lightweight ACK for the test/client.
						 */
						Estimation internalResult = onePass.handleControlRequest(rq);

						if ("FINISH_PHASE_1".equalsIgnoreCase(command)) {
							int actualParallelism = 1;

							try {
								actualParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
							} catch (Exception ignored) {
								actualParallelism = 1;
							}

							int expectedWorkers = rq.getNoOfP() > 0 ? rq.getNoOfP() : actualParallelism;;

							String resultId = resolveOnePassResultId(rq, "PHASE1_RESULT_" + rq.getUID());

							String activeAlias = resolveOnePassPhaseOneAlias(rq);

							Estimation localPhaseOneResult =
									onePass.buildLocalPhaseOneResultEstimation(
											rq,
											pId,
											expectedWorkers,
											actualParallelism,
											resultId,
											activeAlias
									);

							collector.collect(localPhaseOneResult);

							System.out.println("[OnePass LOCAL_PHASE1_RESULT] emitted uid="
									+ rq.getUID()
									+ ", workerId=" + pId
									+ ", expectedWorkers=" + expectedWorkers
									+ ", resultId=" + resultId
									+ ", activeAlias=" + activeAlias);
						}
						System.out.println("[OnePass UPDATE] command = " + command);

						/*
						 * Return only a simple ACK through the existing SDE Estimation path.
						 * The current SDE Kafka writer serializes the request envelope, so
						 * the test should wait for FINISH_PHASE_1 / FINISH_PHASE_2 in param[].
						 */
						Estimation ack = new Estimation(rq, "ACK_" + command, Integer.toString(rq.getUID()));
						collector.collect(ack);

						System.out.println("[OnePass UPDATE] collected ACK for uid=" + rq.getUID() +
								", command=" + command);

						handled = true;
						break;
					} else {
						System.out.println("RequestID 7 is only supported for OnePassSamplerSdeSynopsis. uid="
								+ rq.getUID());

						handled = true;
						break;
					}
				}
			}

			if (!handled) {
				System.out.println("No synopsis found for OnePass UPDATE uid=" + rq.getUID() + ", key=" + rq.getKey());
			}

			M_Synopses.put(rq.getKey(), Synopses);
		}
		// Estimate - delete
		else {
			if(Synopses==null){
				System.out.println("create Synopses first before estimation");
			}else {
				for (Synopsis syn : Synopses) {

					if (rq.getUID() == syn.getSynopsisID()) {
						if (rq.getRequestID() % 10 == 2) {
							System.out.println("removed");
							Synopses.remove(syn);
							M_Synopses.put(rq.getKey(), Synopses);

						} else if ((rq.getRequestID() % 10 == 3) || (rq.getRequestID() % 10 == 6)) {

							Estimation e = syn.estimate(rq);
							if (e.getEstimation() != null) {
								if (rq.getSynopsisID() == 28) {

									HashMap<Integer, Bucket> buckets = (HashMap<Integer, Bucket>) e.getEstimation();

									for (Map.Entry<Integer, Bucket> entry : buckets.entrySet()) {
										Integer key = entry.getKey();
										Bucket value = entry.getValue();
										System.out.println("Bucket No. -> " + key + "Pid:" + pId + "\n INFO -> " + value.toString());
										e.setKey(e.getUID() + "_" + key);
										e.setEstimationkey(e.getUID() + "_" + key + "_" + pId);
										e.setEstimation(value);
										//Estimation e1 = new Estimation(e);
										collector.collect(e);

									}
								} else if (rq.getSynopsisID() == 29) {

									HashMap<String, controlBucket> buckets = (HashMap<String, controlBucket>) e.getEstimation();

									for (Map.Entry<String, controlBucket> entry : buckets.entrySet()) {

										String key = entry.getKey();
										//System.out.println("Keys -> " + key);
										controlBucket value = entry.getValue();
										if (value != null)
											System.out.println("Bucket BEFORE with KEY ->" + key + " INFO -> " + value.toString());
										e.setKey(key);
										e.setEstimationkey(e.getUID() + "_" + key + "_" + pId);
										e.setEstimation(value);
										//System.out.println(e.toString());
										collector.collect(e);
									}
								} else {
									collector.collect(e);
								}
							}

						}
					}

				}
			}
		}
	}
	public void open(Configuration config)  {
	 	pId = getRuntimeContext().getIndexOfThisSubtask();
	}

	private boolean isOnePassDataBarrier(Datapoint node) {
		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		JsonNode values = node.getValues();
		JsonNode barrierNode = values.get(ONEPASS_DATA_BARRIER_FIELD);
		return barrierNode != null && barrierNode.asBoolean(false);
	}

	private void handleOnePassDataBarrier(Datapoint node, ArrayList<Synopsis> synopses,
										  Collector<Estimation> collector) {
		JsonNode values = node.getValues();

		int uid = intField(values, "uid", -1);
		String barrierId = textField(values, "barrierId", "unknown");
		String phase = textField(values, "phase", "UNKNOWN");
		String alias = textField(values, "alias", "");

		int requestedExpectedWorkers = intField(values, "expectedWorkers", 1);

		int actualParallelism = 1;

		try {
			actualParallelism =
					getRuntimeContext().getNumberOfParallelSubtasks();
		} catch (Exception ignored) {
			actualParallelism = 1;
		}

		int expectedWorkers = requestedExpectedWorkers > 0 ? requestedExpectedWorkers : actualParallelism;

		if (expectedWorkers <= 0) {
			expectedWorkers = 1;
		}

		int workerId = pId;

		boolean foundOnePassSynopsis = false;

		if (synopses != null) {
			for (Synopsis syn : synopses) {
				if (syn instanceof OnePassSamplerSdeSynopsis
						&& (uid < 0 || uid == syn.getSynopsisID())) {
					foundOnePassSynopsis = true;
					break;
				}
			}
		}

		String ackJson = buildOnePassDataBarrierAckJson(uid, barrierId, phase,alias, workerId, expectedWorkers,
						actualParallelism, foundOnePassSynopsis);

		String[] param = new String[] {"DATA_BARRIER_ACK", barrierId, phase, alias, Integer.toString(workerId),
						Integer.toString(expectedWorkers)};

		Estimation ack = new Estimation(uid, Integer.toString(uid), ONEPASS_DATA_BARRIER_REQUEST_ID, ONEPASS_SYNOPSIS_ID,
						node.getKey(), ackJson, param, expectedWorkers);

		collector.collect(ack);

		System.out.println("[OnePass DATA BARRIER] " + "uid=" + uid + ", phase=" + phase + ", alias=" + alias
				+ ", barrierId=" + barrierId + ", workerId=" + workerId + ", expectedWorkers=" + expectedWorkers
				+ ", actualParallelism=" + actualParallelism + ", foundOnePassSynopsis=" + foundOnePassSynopsis);
	}

	private String buildOnePassDataBarrierAckJson(int uid, String barrierId, String phase, String alias,
	                                              int workerId, int expectedWorkers, int actualParallelism,
	                                              boolean foundOnePassSynopsis) {
		Map<String, Object> ack = new LinkedHashMap<String, Object>();

		ack.put("type", "DATA_BARRIER_ACK");
		ack.put("barrierId", barrierId);
		ack.put("phase", phase);
		ack.put("alias", alias);
		ack.put("uid", uid);
		ack.put("workerId", workerId);
		ack.put("expectedWorkers", expectedWorkers);
		ack.put("actualParallelism", actualParallelism);
		ack.put("foundOnePassSynopsis", foundOnePassSynopsis);

		try {
			return MAPPER.writeValueAsString(ack);
		} catch (Exception e) {
			throw new IllegalStateException("Could not serialize OnePass data barrier ACK", e);
		}
	}

	private static String textField(JsonNode node, String fieldName, String defaultValue) {
		if (node == null || node.isNull()) {
			return defaultValue;
		}

		JsonNode field = node.get(fieldName);

		if (field == null || field.isNull()) {
			return defaultValue;
		}

		String value = field.asText();

		if (value == null || value.trim().isEmpty()) {
			return defaultValue;
		}
		return value.trim();
	}

	private static int intField(JsonNode node, String fieldName, int defaultValue) {
		if (node == null || node.isNull()) {
			return defaultValue;
		}

		JsonNode field = node.get(fieldName);

		if (field == null || field.isNull()) {
			return defaultValue;
		}
		return field.asInt(defaultValue);
	}

	private static String resolveOnePassResultId(Request request, String defaultValue) {
		if (request == null) {
			return defaultValue;
		}

		JsonNode parameters = request.getParameters();

		if (parameters != null && !parameters.isNull()) {
			JsonNode resultIdNode = parameters.get("onePassResultId");

			if (resultIdNode != null && !resultIdNode.isNull()) {
				String value = resultIdNode.asText();

				if (value != null && !value.trim().isEmpty()) {
					return value.trim();
				}
			}
		}

		String[] param = request.getParam();

		if (param != null && param.length > 1 && param[1] != null) {
			String value = param[1];

			if (!value.trim().isEmpty()) {
				return value.trim();
			}
		}

		return defaultValue;
	}

	private boolean isOnePassGlobalStateChunk(Datapoint node) {
		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		JsonNode values = node.getValues();
		JsonNode typeNode = values.get("type");

		return typeNode != null
				&& "GLOBAL_STATE_CHUNK".equals(typeNode.asText(""));
	}

	private void handleOnePassGlobalStateChunk(
			Datapoint node,
			ArrayList<Synopsis> synopses,
			Collector<Estimation> collector) {

		JsonNode chunk = node.getValues();

		String stateRef = textField(chunk, "stateRef", "");
		int chunkId = intField(chunk, "chunkId", -1);
		int chunkCount = intField(chunk, "chunkCount", -1);

		if (stateRef == null || stateRef.trim().isEmpty()) {
			System.out.println("[OnePass GLOBAL_STATE_CHUNK] Ignoring chunk without stateRef: " + chunk);
			return;
		}

		if (chunkId < 0 || chunkCount <= 0) {
			System.out.println("[OnePass GLOBAL_STATE_CHUNK] Ignoring invalid chunk metadata: " + chunk);
			return;
		}

		Map<Integer, JsonNode> chunks = onePassGlobalStateChunksByRef.get(stateRef);

		if (chunks == null) {
			chunks = new HashMap<Integer, JsonNode>();
			onePassGlobalStateChunksByRef.put(stateRef, chunks);
		}

		chunks.put(chunkId, chunk);

		System.out.println("[OnePass GLOBAL_STATE_CHUNK] received stateRef="
				+ stateRef
				+ ", chunkId=" + chunkId
				+ ", chunkCount=" + chunkCount
				+ ", key=" + node.getKey()
				+ ", received=" + chunks.size() + "/" + chunkCount);

		if (chunks.size() >= chunkCount) {
			JsonNode assembled = assembleGlobalState(stateRef, chunks, chunkCount);

			onePassGlobalStatesByRef.put(stateRef, assembled);
			onePassGlobalStateChunksByRef.remove(stateRef);

			System.out.println("[OnePass GLOBAL_STATE_READY_LOCAL] stateRef="
					+ stateRef
					+ ", key=" + node.getKey()
					+ ", entries=" + assembled.get("entries").size());

			Request pending = pendingOnePassInstallRequestsByRef.remove(stateRef);

			if (pending != null) {
				System.out.println("[OnePass INSTALL_GLOBAL_INDEX] pending request found after chunks completed. stateRef="
						+ stateRef);

				handleInstallGlobalIndexRequest(pending, synopses, collector);
			}
		}
	}

	private static String resolveOnePassPhaseOneAlias(Request request) {
		if (request == null) {
			return "";
		}

		JsonNode parameters = request.getParameters();

		if (parameters != null && !parameters.isNull()) {
			JsonNode aliasNode = parameters.get("onePassAlias");

			if (aliasNode == null || aliasNode.isNull()) {
				aliasNode = parameters.get("phaseOneAlias");
			}

			if (aliasNode != null && !aliasNode.isNull()) {
				String value = aliasNode.asText();

				if (value != null && !value.trim().isEmpty()) {
					return value.trim();
				}
			}
		}

		String[] param = request.getParam();

		/*
		 * Expected multi-alias form:
		 *   param[0] = FINISH_PHASE_1
		 *   param[1] = resultId
		 *   param[2] = activeAlias
		 */
		if (param != null && param.length > 2 && param[2] != null) {
			String value = param[2];

			if (!value.trim().isEmpty()) {
				return value.trim();
			}
		}

		return "";
	}

	private JsonNode assembleGlobalState(String stateRef, Map<Integer, JsonNode> chunks, int chunkCount) {
		ObjectNode assembled = MAPPER.createObjectNode();

		JsonNode first = chunks.get(0);

		if (first == null) {
			throw new IllegalStateException("Missing chunk 0 for stateRef=" + stateRef);
		}

		assembled.put("type", "GLOBAL_PHASE1_INDEX");
		assembled.put("stateRef", stateRef);

		copyIfPresent(first, assembled, "stateType");
		copyIfPresent(first, assembled, "uid");
		copyIfPresent(first, assembled, "synopsisID");
		copyIfPresent(first, assembled, "phase");
		copyIfPresent(first, assembled, "resultId");
		copyIfPresent(first, assembled, "queryName");
		copyIfPresent(first, assembled, "rootAlias");
		copyIfPresent(first, assembled, "baseKey");
		copyIfPresent(first, assembled, "expectedWorkers");
		copyIfPresent(first, assembled, "workerId");
		copyIfPresent(first, assembled, "workerKey");
		copyIfPresent(first, assembled, "seenTuplesByAlias");
		copyIfPresent(first, assembled, "edgeSummaries");
		copyIfPresent(first, assembled, "activeAlias");
		copyIfPresent(first, assembled, "activeEdgeId");

		ArrayNode entries = MAPPER.createArrayNode();

		for (int i = 0; i < chunkCount; i++) {
			JsonNode chunk = chunks.get(i);

			if (chunk == null) {
				throw new IllegalStateException("Missing chunk " + i + " for stateRef=" + stateRef);
			}

			JsonNode chunkEntries = chunk.get("entries");

			if (chunkEntries != null && chunkEntries.isArray()) {
				for (JsonNode entry : chunkEntries) {
					entries.add(entry);
				}
			}
		}

		assembled.set("entries", entries);

		return assembled;
	}

	private void copyIfPresent(JsonNode source, ObjectNode target, String fieldName) {
		JsonNode value = source.get(fieldName);

		if (value != null && !value.isNull()) {
			target.set(fieldName, value);
		}
	}

	private void handleInstallGlobalIndexRequest(
			Request rq,
			ArrayList<Synopsis> synopses,
			Collector<Estimation> collector) {

		String stateRef = resolveInstallStateRef(rq);

		if (stateRef == null || stateRef.trim().isEmpty()) {
			System.out.println("[OnePass INSTALL_GLOBAL_INDEX] Missing stateRef. Request=" + rq);
			return;
		}

		JsonNode state = onePassGlobalStatesByRef.get(stateRef);

		if (state == null || state.isNull()) {
			pendingOnePassInstallRequestsByRef.put(stateRef, rq);

			System.out.println("[OnePass INSTALL_GLOBAL_INDEX] State not available yet. Pending install stored. stateRef="
					+ stateRef
					+ ", key=" + rq.getKey());

			return;
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsis(rq, synopses);

		boolean installed = false;
		String error = "";

		if (onePass == null) {
			error = "No OnePassSamplerSdeSynopsis found for uid=" + rq.getUID()
					+ ", key=" + rq.getKey();
		} else {
			try {
				String activeAlias = resolveInstallActiveAlias(rq, state);
				onePass.installGlobalPhaseOneIndex(state, activeAlias);
				installed = true;
			} catch (Exception ex) {
				error = ex.getMessage();
				ex.printStackTrace();
			}
		}

		int expectedWorkers = rq.getNoOfP() > 0
				? rq.getNoOfP()
				: getRuntimeContext().getNumberOfParallelSubtasks();

		String stateCheckSum = computeGlobalStateChecksum(state);

		String ackJson = buildInstallGlobalIndexAckJson(
				rq,
				state,
				stateRef,
				pId,
				expectedWorkers,
				installed,
				error,
				stateCheckSum
		);

		String[] param = new String[] {
				"INSTALL_GLOBAL_INDEX_ACK",
				stateRef,
				"PHASE1",
				textField(state, "rootAlias", ""),
				Integer.toString(pId),
				Integer.toString(expectedWorkers)
		};

		String estimationKey = rq.getUID() + "_INSTALL_GLOBAL_INDEX_" + stateRef + "_" + pId;

		Estimation ack = new Estimation(
				rq.getUID(),
				estimationKey,
				75,
				30,
				rq.getKey(),
				ackJson,
				param,
				expectedWorkers
		);

		collector.collect(ack);

		System.out.println("[OnePass INSTALL_GLOBAL_INDEX] ACK emitted uid="
				+ rq.getUID()
				+ ", workerId=" + pId
				+ ", stateRef=" + stateRef
				+ ", installed=" + installed
				+ ", checksum=" + stateCheckSum
				+ ", key=" + rq.getKey());
	}

	private String resolveInstallActiveAlias(Request request, JsonNode state) {
		JsonNode parameters = request == null ? null : request.getParameters();

		if (parameters != null && !parameters.isNull()) {
			JsonNode aliasNode = parameters.get("onePassAlias");

			if (aliasNode == null || aliasNode.isNull()) {
				aliasNode = parameters.get("phaseOneAlias");
			}

			if (aliasNode != null && !aliasNode.isNull()) {
				String value = aliasNode.asText();

				if (value != null && !value.trim().isEmpty()) {
					return value.trim();
				}
			}
		}

		if (request != null && request.getParam() != null && request.getParam().length > 3) {
			String value = request.getParam()[3];

			if (value != null && !value.trim().isEmpty()) {
				return value.trim();
			}
		}

		return textField(state, "activeAlias", "");
	}

	private OnePassSamplerSdeSynopsis findOnePassSynopsis(Request rq, ArrayList<Synopsis> synopses) {
		if (synopses == null) {
			return null;
		}

		for (Synopsis syn : synopses) {
			if (syn instanceof OnePassSamplerSdeSynopsis
					&& rq.getUID() == syn.getSynopsisID()) {
				return (OnePassSamplerSdeSynopsis) syn;
			}
		}

		return null;
	}

	private String resolveInstallStateRef(Request request) {
		if (request == null) {
			return "";
		}

		JsonNode parameters = request.getParameters();

		if (parameters != null && !parameters.isNull()) {
			JsonNode node = parameters.get("onePassStateRef");

			if (node != null && !node.isNull()) {
				String value = node.asText();

				if (value != null && !value.trim().isEmpty()) {
					return value.trim();
				}
			}
		}

		String[] param = request.getParam();

		if (param != null && param.length > 1 && param[1] != null) {
			String value = param[1];

			if (!value.trim().isEmpty()) {
				return value.trim();
			}
		}

		return "";
	}

	private String buildInstallGlobalIndexAckJson(
			Request rq,
			JsonNode state,
			String stateRef,
			int workerId,
			int expectedWorkers,
			boolean installed,
			String error, String stateCheckSum) {

		Map<String, Object> ack = new LinkedHashMap<String, Object>();

		ack.put("type", "INSTALL_GLOBAL_INDEX_ACK");
		ack.put("uid", rq.getUID());
		ack.put("stateRef", stateRef);
		ack.put("phase", "PHASE1");
		ack.put("resultId", textField(state, "resultId", ""));
		ack.put("rootAlias", textField(state, "rootAlias", ""));
		ack.put("workerId", workerId);
		ack.put("expectedWorkers", expectedWorkers);
		ack.put("installed", installed);

		JsonNode entries = state.get("entries");
		ack.put("entryCount", entries != null && entries.isArray() ? entries.size() : 0);
		//debug
		ack.put("stateChecksum", computeGlobalStateChecksum(state));

		if (error != null && !error.trim().isEmpty()) {
			ack.put("error", error);
		}

		try {
			return MAPPER.writeValueAsString(ack);
		} catch (Exception e) {
			throw new IllegalStateException("Could not serialize INSTALL_GLOBAL_INDEX_ACK", e);
		}
	}

	private String computeGlobalStateChecksum(JsonNode state) {
		try {
			JsonNode entries = state.get("entries");

			if (entries == null || !entries.isArray()) {
				return "EMPTY";
			}

			List<String> parts = new ArrayList<String>();

			for (JsonNode entry : entries) {
				String edgeId = textField(entry, "edgeId", "");
				String joinKey = textField(entry, "joinKey", "");
				double weight = entry.has("globalWeight")
						? entry.get("globalWeight").asDouble(0.0d)
						: 0.0d;

				parts.add(edgeId + "|" + joinKey + "|" + weight);
			}

			Collections.sort(parts);

			StringBuilder sb = new StringBuilder();

			for (String part : parts) {
				sb.append(part).append("\n");
			}

			return Integer.toHexString(sb.toString().hashCode());

		} catch (Exception e) {
			return "CHECKSUM_ERROR_" + e.getClass().getSimpleName();
		}
	}

}
