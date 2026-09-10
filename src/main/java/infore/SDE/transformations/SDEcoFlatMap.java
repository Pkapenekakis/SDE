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
import infore.SDE.synopses.OnePassSampler.OnePassSamplerSynopsis;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOne;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneContribution;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassShardedPhaseTwoState;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassShardOwnership;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;
import infore.SDE.transformations.onepass.debug.OnePassPhaseOneValidatorExporter;
import infore.SDE.transformations.onepass.debug.OnePassPhaseTwoValidatorExporter;
import infore.SDE.transformations.onepass.OnePassRequestParser;
import infore.SDE.transformations.onepass.worker.OnePassTupleBufferGate;
import infore.SDE.transformations.onepass.worker.PhaseThree.OnePassPhaseThreeEnrichmentBuffer;
import infore.SDE.transformations.onepass.worker.PhaseThree.OnePassPhaseThreeEnrichmentCompletionTracker;
import infore.SDE.transformations.onepass.worker.PhaseTwo.OnePassPhaseTwoEnrichmentBuffer;
import infore.SDE.transformations.onepass.worker.PhaseTwo.OnePassPhaseTwoEnrichmentCompletionTracker;
import lib.WDFT.controlBucket;
import lib.WLSH.Bucket;
import infore.SDE.synopses.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.messages.Datapoint;
import infore.SDE.transformations.onepass.worker.PhaseOne.OnePassPhaseOneTransferBuffer;
import infore.SDE.transformations.onepass.worker.PhaseOne.OnePassPhaseOneCompletionTracker;
import infore.SDE.transformations.onepass.worker.PhaseOne.OnePassPhaseOneEnrichmentBuffer;
import infore.SDE.transformations.onepass.worker.PhaseOne.OnePassPhaseOneEnrichmentCompletionTracker;

public class SDEcoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Estimation> {

	private static final long serialVersionUID = 1L;
	private HashMap<String,ArrayList<Synopsis>> M_Synopses = new HashMap<>();
	private HashMap<String,ArrayList<ContinuousSynopsis>> MC_Synopses = new HashMap<>();
	private HashMap<String, Map<Integer, JsonNode>> onePassStateChunksByRef = new HashMap<String, Map<Integer, JsonNode>>();
	private static final int ONEPASS_MAX_BUFFERED_TUPLES_PER_UID =
			Integer.getInteger("sde.onepass.maxBufferedTuplesPerUid", 1000000);
	private final OnePassTupleBufferGate onePassTupleBufferGate =
			new OnePassTupleBufferGate(ONEPASS_MAX_BUFFERED_TUPLES_PER_UID);
	private final Map<String, Datapoint> pendingOnePassEndAliasByUidAlias = new HashMap<String, Datapoint>();

	private int pId;
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final int ONEPASS_SYNOPSIS_ID = 30;
	private static final String ONEPASS_END_ALIAS_TYPE = "END_ALIAS";
	private final Set<String> processedOnePassEndAliasMarkers = new HashSet<String>();
	private final Map<Integer, Integer> onePassExpectedWorkersByUid = new HashMap<Integer, Integer>();
	private final Map<Integer, String> onePassBaseKeyByUid = new HashMap<Integer, String>();
	private final Map<Integer, Integer> onePassPhaseOneEpochByUid = new HashMap<Integer, Integer>();

	private final OnePassPhaseOneTransferBuffer onePassPhaseOneTransferBuffer =
			new OnePassPhaseOneTransferBuffer();
	private final OnePassPhaseOneCompletionTracker onePassPhaseOneCompletionTracker =
			new OnePassPhaseOneCompletionTracker();
	private final OnePassPhaseOneEnrichmentBuffer onePassPhaseOneEnrichmentBuffer =
			new OnePassPhaseOneEnrichmentBuffer();
	private final OnePassPhaseOneEnrichmentCompletionTracker onePassPhaseOneEnrichmentCompletionTracker =
			new OnePassPhaseOneEnrichmentCompletionTracker();
	private final OnePassPhaseTwoEnrichmentBuffer onePassPhaseTwoEnrichmentBuffer =
			new OnePassPhaseTwoEnrichmentBuffer();
	private final OnePassPhaseTwoEnrichmentCompletionTracker onePassPhaseTwoEnrichmentCompletionTracker =
			new OnePassPhaseTwoEnrichmentCompletionTracker();

	/*
	 * Completed State-Topic sample installs.
	 * Needed only for idempotence against duplicate Kafka delivery.
	 * This stores tiny stateRef strings, not sample payloads.
	 */
	private final Set<String> installedOnePassPhaseTwoStateRefs = new HashSet<String>();

	private final OnePassPhaseThreeEnrichmentBuffer onePassPhaseThreeEnrichmentBuffer =
			new OnePassPhaseThreeEnrichmentBuffer();
	private final OnePassPhaseThreeEnrichmentCompletionTracker onePassPhaseThreeEnrichmentCompletionTracker =
			new OnePassPhaseThreeEnrichmentCompletionTracker();
	private final Map<Integer, List<JsonNode>> pendingOnePassPhaseThreeStateByUid =
			new HashMap<Integer, List<JsonNode>>();
	private final Set<String> emittedOnePassPhaseThreeLocalSelections =
			new HashSet<String>();
	// Tiny stateRef strings only; no selection payloads are retained here.
	private final Set<String> installedOnePassPhaseThreeStateRefs =
			new HashSet<String>();

	//State-topic messages may race ahead of START_PHASE_2 on another Flink input.
	private final Map<Integer, List<JsonNode>> pendingOnePassPhaseTwoStateByUid = new HashMap<Integer, List<JsonNode>>();
	private final Set<String> emittedOnePassPhaseTwoLocalSummaries = new HashSet<String>();

	private static final String ONEPASS_COMMAND_START_NEXT_ALIAS = "START_NEXT_ALIAS";
	private static final String ONEPASS_COMMAND_START_PHASE_2 = "START_PHASE_2";
	private static final String ONEPASS_STATE_TYPE_PHASE2_ROOT_SAMPLE = "GLOBAL_PHASE2_ROOT_SAMPLE";
	private static final String ONEPASS_STATE_TYPE_PHASE3_ALIAS_SELECTIONS = "GLOBAL_PHASE3_ALIAS_SELECTIONS";

	@Override
	public void flatMap1(Datapoint node, Collector<Estimation> collector) throws JsonProcessingException {
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(node.getKey());

		if (isOnePassPhaseThreeAliasSelectionsChunk(node)) {
			handleOnePassPhaseThreeAliasSelectionsChunk(node, Synopses, collector);
			return;
		}

		if (isOnePassPhaseTwoStateTransfer(node)) {
			handleOnePassPhaseTwoStateTransfer(node, Synopses, collector);
			return;
		}

		if (isOnePassPhaseOneStateTransfer(node)) {
			handleOnePassPhaseOneStateTransfer(node, Synopses, collector);
			return;
		}

		if (isOnePassPhaseTwoRootSampleChunk(node)) {
			handleOnePassPhaseTwoRootSampleChunk(node, Synopses, collector);
			return;
		}

		if (isOnePassEndAlias(node)) {
			handleOnePassEndAlias(node, Synopses, collector
			);

			return;
		}

		if (Synopses != null) {
			for (Synopsis ski : Synopses) {
				if (ski instanceof OnePassSamplerSdeSynopsis) {
					handleOnePassDataTuple((OnePassSamplerSdeSynopsis) ski, node.getValues(), collector);

				} else {
					//Existing synopses keep their original behavior.
					ski.add(node.getValues());
				}
			}
			M_Synopses.put(node.getKey(), Synopses);
			/*
			for (Synopsis ski : Synopses) {
				ski.add(node.getValues());
			}
		M_Synopses.put(node.getKey(),Synopses); */
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
		/*System.out.println("[SDEcoFlatMap REQUEST] requestID=" + rq.getRequestID()
						+ ", synopsisID=" + rq.getSynopsisID() + ", uid=" + rq.getUID()
						+ ", key=" + rq.getKey() + ", known keys=" + M_Synopses.keySet());
		System.out.println(rq.toString()); */
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(rq.getKey());
		ArrayList<ContinuousSynopsis>  C_Synopses =  MC_Synopses.get(rq.getKey());

		if (isOnePassPhaseOneDebugExportRequest(rq)) {
			handleOnePassPhaseOneDebugExportRequest(rq, Synopses);
			return;
		}

		if (isOnePassPhaseTwoDebugValidationRequest(rq)) {
			handleOnePassPhaseTwoDebugValidationRequest(rq, Synopses);
			return;
		}

		/*
		 * OnePass explicit cleanup.
		 * Keep this isolated from generic synopsis removal because OnePass owns
		 * additional worker-local protocol/gating state outside M_Synopses.
		 */
		if (rq.getSynopsisID() == ONEPASS_SYNOPSIS_ID && rq.getRequestID() % 10 == 2) {

			handleOnePassRemove(rq, Synopses);

			return;
		}

		if (isOnePassShardedPhaseThreeTransitionRequest(rq)) {
			handleOnePassShardedPhaseThreeTransitionRequest(rq, Synopses, collector);
			return;
		}

		if (isOnePassShardedPhaseOneTransitionRequest(rq)) {
			handleOnePassShardedPhaseOneTransitionRequest(rq, Synopses, collector);
			return;
		}

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

				registerOnePassTupleGate((OnePassSamplerSdeSynopsis) sketch);

				int expectedWorkers = rq.getNoOfP() > 0 ? rq.getNoOfP() : 1;
				String baseKey = OnePassShardOwnership.baseKeyFromWorkerKey(rq.getKey(), expectedWorkers, pId);

				onePassExpectedWorkersByUid.put(rq.getUID(), expectedWorkers);
				onePassBaseKeyByUid.put(rq.getUID(), baseKey);
				onePassPhaseOneEpochByUid.put(rq.getUID(), 1);

				System.out.println("OnePassSamplerSdeSynopsis added for uid=" + rq.getUID() + ", key=" +
						rq.getKey() + ", initialAllowedAlias=" + onePassTupleBufferGate.getAllowedAlias(rq.getUID()));

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

			for (Synopsis syn : Synopses) {
				if (rq.getUID() == syn.getSynopsisID()) {
					if (syn instanceof OnePassSamplerSdeSynopsis) {
						OnePassSamplerSdeSynopsis onePass = (OnePassSamplerSdeSynopsis) syn;

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


						if ("FINISH_PHASE_3_ALIAS".equalsIgnoreCase(command)) {
							int actualParallelism = 1;

							try {
								actualParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
							} catch (Exception ignored) {
								actualParallelism = 1;
							}

							int expectedWorkers = rq.getNoOfP() > 0 ? rq.getNoOfP() : actualParallelism;
							String alias = resolvePhaseThreeAlias(rq);
							String resultId = resolveOnePassResultId(rq, "PHASE3_ALIAS_" + alias + "_" + rq.getUID());

							Estimation localPhaseThreeAliasResult = onePass.buildLocalPhaseThreeAliasResultEstimation(
									rq, pId, expectedWorkers, actualParallelism, resultId, alias);

							collector.collect(localPhaseThreeAliasResult);

							Estimation ack = new Estimation(rq, "ACK_FINISH_PHASE_3_ALIAS", Integer.toString(rq.getUID()));
							collector.collect(ack);
							handled = true;
							break;
						}

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
									onePass.buildLocalPhaseOneResultEstimation(rq, pId, expectedWorkers,
											actualParallelism, resultId, activeAlias);

							collector.collect(localPhaseOneResult);

							System.out.println("[OnePass LOCAL_PHASE1_RESULT] emitted uid="
									+ rq.getUID()
									+ ", workerId=" + pId
									+ ", expectedWorkers=" + expectedWorkers
									+ ", resultId=" + resultId
									+ ", activeAlias=" + activeAlias);
						}

						if ("FINISH_PHASE_2".equalsIgnoreCase(command)) {
							int actualParallelism = 1;

							try {
								actualParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
							} catch (Exception ignored) {
								actualParallelism = 1;
							}

							int expectedWorkers = rq.getNoOfP() > 0 ? rq.getNoOfP() : actualParallelism;
							String resultId = resolveOnePassResultId(rq, "PHASE2_RESULT_" + rq.getUID());

							Estimation localPhaseTwoSummary =
									onePass.buildLocalPhaseTwoRootSummaryEstimation(
											rq,
											pId,
											expectedWorkers,
											actualParallelism,
											resultId
									);

							collector.collect(localPhaseTwoSummary);

							System.out.println("[OnePass LOCAL_PHASE2_ROOT_SUMMARY] emitted uid="
									+ rq.getUID()
									+ ", workerId=" + pId
									+ ", expectedWorkers=" + expectedWorkers
									+ ", resultId=" + resultId);
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

	private static String firstParam(Request request) {
		if (request == null || request.getParam() == null || request.getParam().length == 0) {
			return "";
		}

		String value = request.getParam()[0];

		if (value == null) {
			return "";
		}

		return value.trim();
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

		String stateType = textField(first, "stateType", "GLOBAL_PHASE1_INDEX");
		assembled.put("type", stateType);

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
		copyIfPresent(first, assembled, "sampleSize");
		copyIfPresent(first, assembled, "rootTuplesSeen");
		copyIfPresent(first, assembled, "positiveRootCandidatesSeen");
		copyIfPresent(first, assembled, "totalRootGroupWeight");
		copyIfPresent(first, assembled, "sampleInstanceCount");
		copyIfPresent(first, assembled, "datasetSeed");
		copyIfPresent(first, assembled, "globalReservoir");
		copyIfPresent(first, assembled, "phaseThreeAlias");
		copyIfPresent(first, assembled, "alias");
		copyIfPresent(first, assembled, "selectionCount");
		copyIfPresent(first, assembled, "totalCandidatesSeen");
		copyIfPresent(first, assembled, "totalCandidateWeight");
		copyIfPresent(first, assembled, "sampleSize");

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

	private static long longField(JsonNode node, String fieldName, long defaultValue) {
		if (node == null || node.isNull()) return defaultValue;
		JsonNode field = node.get(fieldName);
		if (field == null || field.isNull()) return defaultValue;
		return field.asLong(defaultValue);
	}

	private static double doubleField(JsonNode node, String fieldName, double defaultValue) {
		if (node == null || node.isNull()) return defaultValue;
		JsonNode field = node.get(fieldName);
		if (field == null || field.isNull()) return defaultValue;
		return field.asDouble(defaultValue);
	}

	private static String resolvePhaseThreeAlias(Request request) {
		if (request == null) return "";

		JsonNode parameters = request.getParameters();

		if (parameters != null && !parameters.isNull()) {
			JsonNode aliasNode = parameters.get("onePassAlias");
			if (aliasNode == null || aliasNode.isNull()) aliasNode = parameters.get("phaseThreeAlias");
			if (aliasNode != null && !aliasNode.isNull()) {
				String value = aliasNode.asText();
				if (value != null && !value.trim().isEmpty()) return value.trim();
			}
		}

		String[] param = request.getParam();
		if (param != null && param.length > 1 && param[1] != null && !param[1].trim().isEmpty()) {
			return param[1].trim();
		}

		return "";
	}

	private OnePassSamplerSdeSynopsis findOnePassSynopsisByUid(int uid, ArrayList<Synopsis> synopses) {

		if (synopses == null) {
			return null;
		}

		for (Synopsis synopsis : synopses) {
			if (synopsis instanceof OnePassSamplerSdeSynopsis && uid == synopsis.getSynopsisID()) {
				return (OnePassSamplerSdeSynopsis) synopsis;
			}
		}

		return null;
	}

	private void registerOnePassTupleGate(
			OnePassSamplerSdeSynopsis onePass) {

		if (onePass == null) {
			return;
		}

		int uid = onePass.getSynopsisID();

		List<String> leafToRootOrder = onePass.getPlan().getLeafToRootOrder();

		if (leafToRootOrder == null || leafToRootOrder.isEmpty()) {

			throw new IllegalStateException("Cannot initialize OnePass tuple gate because " +
					"leafToRootOrder is empty. uid=" + uid);
		}

		String firstAlias = leafToRootOrder.get(0);

		onePassTupleBufferGate.registerIfAbsent(uid, firstAlias);
	}

	private void handleOnePassDataTuple(OnePassSamplerSdeSynopsis onePass, JsonNode payload,
										Collector<Estimation> collector) {

		if (onePass == null) {
			return;
		}

		int uid = onePass.getSynopsisID();

		if (!onePassTupleBufferGate.isRegistered(uid)) {
			registerOnePassTupleGate(onePass);
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
		String alias = tuple.getTable();

		if (!onePassTupleBufferGate.isAllowed(uid, alias)) {
			onePassTupleBufferGate.buffer(uid, alias, payload);
			return;
		}

		int expectedWorkers = onePassExpectedWorkersByUid.getOrDefault(uid, 1);

		if (onePass.getLifecycle().getPhase() == OnePassSamplerSynopsis.Phase.PHASE_1 && expectedWorkers > 1) {
			processShardedPhaseOneTuple(onePass, payload, collector);
			return;
		}

		if (onePass.getLifecycle().getPhase() == OnePassSamplerSynopsis.Phase.PHASE_2 && expectedWorkers > 1 &&
				onePass.isShardedPhaseTwoActive()) {
			processShardedPhaseTwoRootTuple(onePass, payload, collector);
			return;
		}

		if (onePass.getLifecycle().getPhase() == OnePassSamplerSynopsis.Phase.PHASE_3 && expectedWorkers > 1 &&
				onePass.isShardedPhaseThreeActive()) {
			processShardedPhaseThreeTuple(onePass, payload, collector);
			return;
		}

		// Single-worker / legacy paths remain unchanged.
		onePass.add(payload);
	}

	private void processShardedPhaseOneTuple(OnePassSamplerSdeSynopsis onePass, JsonNode payload, Collector<Estimation> collector) {

		int uid = onePass.getSynopsisID();
		int expectedWorkers = onePassExpectedWorkersByUid.get(uid);
		int epoch = onePassPhaseOneEpochByUid.get(uid);

		String baseKey = onePassBaseKeyByUid.get(uid);
		OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
		String alias = tuple.getTable();

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(alias);

		/*
		 * Original tuple accounting happens exactly once here.
		 */
		double partialWeight = onePass.beginShardedPhaseOneTuple(payload);

		if (partialWeight == 0.0d) {
			return;
		}

		/*
		 * Leaf alias:
		 * The data router already sent this tuple to the final parent-index owner.
		 */
		if (childEdges.isEmpty()) {

			OnePassPhaseOneContribution contribution = onePass.buildShardedPhaseOneParentContribution(payload, partialWeight);

			emitFinalPhaseOneContribution(onePass, contribution, uid, baseKey, expectedWorkers, epoch, alias, collector);

			return;
		}

		/*
		 * Internal alias:
		 * The data router sent this original tuple to the owner of child index 0.
		 */
		partialWeight *= onePass.lookupShardedPhaseOneChildWeight(payload, 0);

		if (partialWeight == 0.0d) {
			return;
		}

		if (childEdges.size() == 1) {
			OnePassPhaseOneContribution contribution = onePass.buildShardedPhaseOneParentContribution(payload, partialWeight);
			emitFinalPhaseOneContribution(onePass, contribution, uid, baseKey, expectedWorkers, epoch, alias, collector);

			return;
		}

		/*
		 * Branching case:
		 * child 0 was consumed locally. Move the partially enriched tuple to the owner of child 1.
		 */
		routePhaseOneEnrichmentWork(onePass, payload, partialWeight, 1, uid, baseKey, expectedWorkers, epoch,
				alias, collector);
	}

	private boolean isOnePassEndAlias(Datapoint node) {

		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		JsonNode values = node.getValues();
		String type = textField(values, "type", "");

		int synopsisId = intField(values, "synopsisID", -1);

		return ONEPASS_END_ALIAS_TYPE.equals(type) && synopsisId == ONEPASS_SYNOPSIS_ID;
	}

	private void handleOnePassEndAlias(Datapoint node, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		JsonNode values = node.getValues();
		int uid = intField(values, "uid", -1);
		String phase = textField(values, "phase", "");
		String alias = textField(values, "alias", "");
		String resultId = textField(values, "resultId", "");
		String nextCommand = textField(values, "nextCommand", "");
		String nextAlias = textField(values, "nextAlias", "");
		int requestedExpectedWorkers = intField(values, "expectedWorkers", 0);


		// -------------------------------------------------------------
		// Basic END_ALIAS validation
		// -------------------------------------------------------------

		if (uid < 0) {
			throw new IllegalStateException("END_ALIAS is missing a valid uid: " + values);
		}

		boolean phaseOne = "PHASE1".equalsIgnoreCase(phase);
		boolean phaseTwo = "PHASE2".equalsIgnoreCase(phase);
		boolean phaseThree = "PHASE3".equalsIgnoreCase(phase);

		if (!phaseOne && !phaseTwo && !phaseThree) {
			throw new IllegalStateException("END_ALIAS supports PHASE1, PHASE2 and PHASE3. Received phase=" +
					phase + ", payload=" + values);
		}

		if (alias == null || alias.trim().isEmpty()) {
			throw new IllegalStateException("END_ALIAS is missing alias: " + values);
		}

		if (resultId == null || resultId.trim().isEmpty()) {
			throw new IllegalStateException("END_ALIAS is missing resultId: " + values);
		}


		/*
		 * PHASE1 still needs nextCommand / nextAlias because END_ALIAS drives
		 * the transition to the next Phase-1 alias or to Phase 2.
		 *
		 * PHASE2 does NOT need them. Finishing the root scan only closes the
		 * distributed root-enrichment protocol and emits the local Phase-2
		 * reservoir summary.
		 */
		if (phaseOne) {
			if (!ONEPASS_COMMAND_START_NEXT_ALIAS.equals(nextCommand) &&
					!ONEPASS_COMMAND_START_PHASE_2.equals(nextCommand)) {
				throw new IllegalStateException("PHASE1 END_ALIAS has invalid nextCommand=" + nextCommand +
						". Expected START_NEXT_ALIAS or START_PHASE_2." + " payload=" + values);
			}


			if (nextAlias == null || nextAlias.trim().isEmpty()) {
				throw new IllegalStateException("PHASE1 END_ALIAS is missing nextAlias: " + values);
			}
		}


		String markerKey = uid + "|" + phase + "|" + alias + "|" + resultId;

		if (processedOnePassEndAliasMarkers.contains(markerKey)) {
			System.out.println("[OnePass END_ALIAS] Duplicate ignored." +
					" markerKey=" + markerKey + ", workerId=" + pId);
			return;
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

		if (onePass == null) {
			throw new IllegalStateException("END_ALIAS reached worker without OnePass synopsis." +
					" uid=" + uid + ", key=" + node.getKey() + ", workerId=" + pId);
		}


		// -------------------------------------------------------------
		// Tuple-gate ordering
		// -------------------------------------------------------------
		String currentAlias = onePassTupleBufferGate.getAllowedAlias(uid);

		/*
		 * The END_ALIAS marker may arrive before the transition that activates
		 * this alias on this worker.
		 *
		 * Store it and retry it when processPendingOnePassEndAlias(...) is called.
		 */
		if (!alias.equals(currentAlias) || onePassTupleBufferGate.isSealed(uid, alias)) {
			String pendingKey = onePassEndAliasPendingKey(uid, alias);

			if (!pendingOnePassEndAliasByUidAlias.containsKey(pendingKey)) {

				JsonNode copy = node.getValues() == null ? null : node.getValues().deepCopy();
				pendingOnePassEndAliasByUidAlias.put(pendingKey, new Datapoint(node.getKey(), node.getStreamID(), copy));

				System.out.println("[OnePass END_ALIAS DEFERRED]" + " uid=" + uid + ", phase=" + phase +
						", alias=" + alias + ", currentAlias=" + currentAlias + ", workerId=" + pId);
			}

			return;
		}


		int actualParallelism = 1;
		try {
			actualParallelism = getRuntimeContext().getNumberOfParallelSubtasks();

		} catch (Exception ignored) {
			actualParallelism = 1;
		}

		int expectedWorkers = requestedExpectedWorkers > 0 ? requestedExpectedWorkers : actualParallelism;

		if (expectedWorkers <= 0) {
			expectedWorkers = 1;
		}


		// =============================================================
		// SHARDED PHASE 2 END_ALIAS
		// =============================================================
		if (phaseTwo) {
			if (expectedWorkers <= 1) {
				throw new IllegalStateException("PHASE2 END_ALIAS currently belongs to the sharded " +
						"Phase-2 path and requires expectedWorkers > 1." + " uid=" + uid +
						", expectedWorkers=" + expectedWorkers);
			}

			/*
			 * Usually, if rootAlias is active in the tuple gate, START_PHASE_2
			 * has already initialized the sharded Phase-2 state.
			 *
			 * Keep this check anyway because RequestTopic and DataTopic are
			 * independent inputs, and we do not want to seal the root alias before
			 * Phase 2 is actually active.
			 */
			if (!onePass.isShardedPhaseTwoActive()) {
				String pendingKey = onePassEndAliasPendingKey(uid, alias);

				if (!pendingOnePassEndAliasByUidAlias.containsKey(pendingKey)) {
					JsonNode copy = node.getValues() == null ? null : node.getValues().deepCopy();
					pendingOnePassEndAliasByUidAlias.put(pendingKey, new Datapoint(node.getKey(), node.getStreamID(), copy));
				}

				System.out.println("[OnePass PHASE2 END_ALIAS DEFERRED]" + " uid=" + uid + ", alias=" + alias +
						", lifecyclePhase=" + onePass.getLifecycle().getPhase().name() + ", workerId=" + pId);

				return;
			}


			/*
			 * IMPORTANT:
			 * Do not call completeOnePassEndAlias(...) here.
			 * That method exports the OLD replicated Phase-1 result.
			 * Phase 2 has its own distributed completion protocol:
			 *   END_ALIAS(root)
			 *       -> flush Phase-2 enrichment
			 *       -> wait for SOURCE_DONE from all workers
			 *       -> emit LOCAL_PHASE2_ROOT_SUMMARY
			 */
			handleShardedPhaseTwoEndRoot(node, onePass, uid, alias, resultId, expectedWorkers, collector);
			processedOnePassEndAliasMarkers.add(markerKey);
			pendingOnePassEndAliasByUidAlias.remove(onePassEndAliasPendingKey(uid, alias));

			System.out.println("[OnePass PHASE2 END_ALIAS]" + " uid=" + uid + ", rootAlias=" + alias +
					", resultId=" + resultId + ", workerId=" + pId + ", expectedWorkers=" + expectedWorkers);

			return;
		}

		// =============================================================
		// SHARDED PHASE 3 END_ALIAS
		// =============================================================
		if (phaseThree) {
			if (expectedWorkers <= 1) {
				throw new IllegalStateException("PHASE3 END_ALIAS belongs to the sharded Phase-3 path " +
						"and requires expectedWorkers > 1. uid=" + uid);
			}

			if (!onePass.isShardedPhaseThreeActive() || !alias.equals(onePass.getShardedPhaseThreeActiveAlias())) {

				String pendingKey = onePassEndAliasPendingKey(uid, alias);
				if (!pendingOnePassEndAliasByUidAlias.containsKey(pendingKey)) {
					JsonNode copy = node.getValues() == null ? null : node.getValues().deepCopy();
					pendingOnePassEndAliasByUidAlias.put(pendingKey, new Datapoint(node.getKey(), node.getStreamID(), copy));
				}

				System.out.println("[OnePass PHASE3 END_ALIAS DEFERRED] uid=" + uid
								+ ", alias=" + alias
								+ ", active="
								+ onePass.getShardedPhaseThreeActiveAlias()
								+ ", worker=" + pId);
				return;
			}

			String canonicalResultId = shardedPhaseThreeResultId(uid, alias);
			if (!canonicalResultId.equals(resultId)) {
				throw new IllegalStateException("PHASE3 END_ALIAS resultId mismatch. expected=" +
						canonicalResultId + ", received=" + resultId);
			}

			handleShardedPhaseThreeEndAlias(onePass, uid, alias, canonicalResultId, expectedWorkers, collector);
			processedOnePassEndAliasMarkers.add(markerKey);
			pendingOnePassEndAliasByUidAlias.remove(
					onePassEndAliasPendingKey(uid, alias));
			return;
		}


		// =============================================================
		// PHASE 1
		// =============================================================

		/*
		 * Once PHASE1 END_ALIAS has been observed, no later tuple for this
		 * alias may mutate the state that is about to be completed.
		 */
		onePassTupleBufferGate.sealAlias(uid, alias);

		if (expectedWorkers > 1) {
			handleShardedPhaseOneEndAlias(node, onePass, uid, alias, resultId, nextCommand, nextAlias, expectedWorkers, collector);
			processedOnePassEndAliasMarkers.add(markerKey);
			pendingOnePassEndAliasByUidAlias.remove(onePassEndAliasPendingKey(uid, alias));
			return;
		}

		//Existing single-worker / legacy Phase-1 path.
		Estimation localPhaseOneResult = onePass.buildLocalPhaseOneResultEstimation(node.getKey(), uid, pId,
				expectedWorkers, actualParallelism, resultId, alias, nextCommand, nextAlias);

		collector.collect(localPhaseOneResult);
		processedOnePassEndAliasMarkers.add(markerKey);
		System.out.println("[OnePass END_ALIAS] LOCAL_PHASE1_RESULT emitted." + " uid=" + uid + ", alias=" + alias + ", resultId=" + resultId + ", nextCommand=" + nextCommand + ", nextAlias=" + nextAlias + ", workerId=" + pId + ", expectedWorkers=" + expectedWorkers + ", key=" + node.getKey());
		completeOnePassEndAlias(node, synopses, collector);
	}

	private static String onePassEndAliasPendingKey(int uid, String alias) {
		return uid + "|" + (alias == null ? "" : alias.trim());
	}
	private void completeOnePassEndAlias(Datapoint node, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		JsonNode values = node.getValues();

		int uid = intField(values, "uid", -1);

		String phase = textField(values, "phase", "");
		String alias = textField(values, "alias", "");
		String resultId = textField(values, "resultId", "");
		String nextCommand = textField(values, "nextCommand", "");
		String nextAlias = textField(values, "nextAlias", "");
		int requestedExpectedWorkers = intField(values, "expectedWorkers", 0);

		String markerKey = uid + "|" + phase + "|" + alias + "|" + resultId;

		if (processedOnePassEndAliasMarkers.contains(markerKey)) {

			return;
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

		if (onePass == null) {
			throw new IllegalStateException("END_ALIAS reached worker without OnePass synopsis. " + "uid=" + uid +
					", key=" + node.getKey() + ", workerId=" + pId);
		}

		String currentAlias = onePassTupleBufferGate
				.getAllowedAlias(uid);

		if (!alias.equals(currentAlias)) {
			throw new IllegalStateException("Cannot complete END_ALIAS because alias is not active. " + "uid=" + uid +
					", alias=" + alias + ", currentAlias=" + currentAlias + ", workerId=" + pId);
		}

		onePassTupleBufferGate.sealAlias(uid, alias);

		int actualParallelism = 1;

		try {
			actualParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
		} catch (Exception ignored) {actualParallelism = 1;
		}

		int expectedWorkers = requestedExpectedWorkers > 0 ? requestedExpectedWorkers : actualParallelism;

		if (expectedWorkers <= 0) {
			expectedWorkers = 1;
		}

		Estimation localPhaseOneResult = onePass.buildLocalPhaseOneResultEstimation(node.getKey(), uid, pId,
				expectedWorkers, actualParallelism, resultId, alias, nextCommand, nextAlias);

		collector.collect(localPhaseOneResult);

		processedOnePassEndAliasMarkers.add(markerKey);
		pendingOnePassEndAliasByUidAlias.remove(onePassEndAliasPendingKey(uid, alias));

		System.out.println("[OnePass END_ALIAS COMPLETE] " + "uid=" + uid + ", alias=" + alias + ", resultId="
				+ resultId + ", nextCommand=" + nextCommand + ", nextAlias=" + nextAlias + ", workerId=" + pId);
	}

	private void processPendingOnePassEndAlias(int uid, String alias, ArrayList<Synopsis> synopses,
											   Collector<Estimation> collector) {

		if (alias == null || alias.trim().isEmpty()) {
			return;
		}

		String pendingKey = onePassEndAliasPendingKey(uid, alias);
		/*
		 * Remove first. If the marker still cannot be processed,
		 * handleOnePassEndAlias() will safely defer it again.
		 */
		Datapoint pending = pendingOnePassEndAliasByUidAlias.remove(pendingKey);

		if (pending == null) {
			return;
		}

		handleOnePassEndAlias(pending, synopses, collector);
	}

	private void handleOnePassRemove(Request request, ArrayList<Synopsis> synopses) {
		int uid = request.getUID();

		//1. Remove the actual OnePass synopsis.
		if (synopses != null) {
            synopses.removeIf(synopsis -> synopsis instanceof OnePassSamplerSdeSynopsis
					&& synopsis.getSynopsisID() == uid);

			if (synopses.isEmpty()) {
				M_Synopses.remove(request.getKey());
			} else {
				M_Synopses.put(request.getKey(), synopses);
			}
		}

		//3. Remove buffered tuples / active alias / sealed aliases.
		onePassTupleBufferGate.clear(uid);

		//4. Remove deferred END_ALIAS.
		String pendingPrefix = uid + "|";
        pendingOnePassEndAliasByUidAlias.keySet().removeIf(key -> key.startsWith(pendingPrefix));

		//5. Remove END_ALIAS deduplication state.
        processedOnePassEndAliasMarkers.removeIf(key -> key.startsWith(pendingPrefix));

		onePassPhaseOneTransferBuffer.clearUid(uid);
		onePassPhaseOneCompletionTracker.clearUid(uid);
		onePassPhaseOneEnrichmentBuffer.clearUid(uid);
		onePassPhaseOneEnrichmentCompletionTracker.clearUid(uid);
		onePassExpectedWorkersByUid.remove(uid);
		onePassBaseKeyByUid.remove(uid);
		onePassPhaseOneEpochByUid.remove(uid);
		onePassPhaseTwoEnrichmentBuffer.clearUid(uid);
		onePassPhaseTwoEnrichmentCompletionTracker.clearUid(uid);
		pendingOnePassPhaseTwoStateByUid.remove(uid);
		String phaseTwoPrefix = uid + "|";
		emittedOnePassPhaseTwoLocalSummaries.removeIf(key -> key.startsWith(phaseTwoPrefix));

		String statePrefix = uid + "_";
		onePassStateChunksByRef.keySet().removeIf(key -> key != null && key.startsWith(statePrefix));
		installedOnePassPhaseTwoStateRefs.removeIf(key -> key != null && key.startsWith(statePrefix));

		onePassPhaseThreeEnrichmentBuffer.clearUid(uid);
		onePassPhaseThreeEnrichmentCompletionTracker.clearUid(uid);
		pendingOnePassPhaseThreeStateByUid.remove(uid);
		emittedOnePassPhaseThreeLocalSelections.removeIf(key -> key.startsWith(uid + "|"));
		installedOnePassPhaseThreeStateRefs.removeIf(key -> key != null && key.startsWith(uid + "_"));

		System.out.println("[OnePass REMOVE] worker-local state cleared." + " uid=" + uid + ", workerId=" + pId +
				", key=" + request.getKey());
	}

	private boolean isOnePassPhaseOneStateTransfer(Datapoint node) {

		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		String type = textField(node.getValues(), "type", "");
		String protocol = textField(node.getValues(), "protocol", "");

		if (!OnePassPhaseOneTransferBuffer.PROTOCOL.equals(protocol)) {
			return false;
		}

		return OnePassPhaseOneTransferBuffer.TYPE_SHARD_BATCH.equals(type)
				|| OnePassPhaseOneTransferBuffer.TYPE_SOURCE_DONE.equals(type)
				|| OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_BATCH.equals(type)
				|| OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_SOURCE_DONE.equals(type);
	}

	private void handleOnePassPhaseOneStateTransfer(Datapoint node, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		JsonNode payload = node.getValues();

		int uid = intField(payload, "uid", -1);
		int epoch = intField(payload, "epoch", -1);
		String alias = textField(payload, "alias", "");
		int sourceWorker = intField(payload, "sourceWorker", -1);
		int targetWorker = intField(payload, "targetWorker", -1);
		int expectedWorkers = intField(payload, "expectedWorkers", 0);
		String type = textField(payload, "type", "");

		if (targetWorker != pId) {
			throw new IllegalStateException("Phase-1 state message reached wrong worker. target=" +
					targetWorker + ", actual=" + pId + ", payload=" + payload);
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);
		if (onePass == null) {
			throw new IllegalStateException("Phase-1 state message reached worker before OnePass synopsis exists. uid=" +
					uid + ", worker=" + pId);
		}

		/*
		 * -------------------------------------------------------------
		 * FINAL PARENT-INDEX TRANSFER
		 * -------------------------------------------------------------
		 */
		if (OnePassPhaseOneTransferBuffer.TYPE_SHARD_BATCH.equals(type)) {

			int sequence = intField(payload, "sequence", -1);
			boolean firstDelivery = onePassPhaseOneCompletionTracker.acceptBatch(uid, epoch, alias, expectedWorkers, sourceWorker, sequence);

			if (!firstDelivery) {
				return;
			}

			String edgeId = textField(payload, "edgeId", "");
			JsonNode entries = payload.get("entries");

			if (entries == null || !entries.isArray()) {
				throw new IllegalStateException("SHARD_BATCH has no entries array: " + payload);
			}

			for (JsonNode entry : entries) {
				JsonNode partsNode = entry.get("joinKeyParts");

				if (partsNode == null || !partsNode.isArray() || partsNode.size() == 0) {
					throw new IllegalStateException("Invalid SHARD_BATCH joinKeyParts: " + entry);
				}

				List<String> parts = new ArrayList<String>();
				for (JsonNode part : partsNode) {
					parts.add(part.asText());
				}

				double delta = entry.get("delta").asDouble(0.0d);
				onePass.applyPhaseOneContribution(edgeId, new JoinValue(parts), delta);
			}

			maybeEmitLocalPhaseOneShardReady(uid, epoch, alias, onePass, collector);

			return;
		}

		if (OnePassPhaseOneTransferBuffer.TYPE_SOURCE_DONE.equals(type)) {

			int lastSequence = intField(payload, "lastSequence", -1);
			onePassPhaseOneCompletionTracker.acceptSourceDone(uid, epoch, alias, expectedWorkers, sourceWorker, lastSequence);
			maybeEmitLocalPhaseOneShardReady(uid, epoch, alias, onePass, collector);
			return;
		}

		/*
		 * -------------------------------------------------------------
		 * BRANCHING ENRICHMENT TRANSFER
		 * -------------------------------------------------------------
		 */
		if (OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_BATCH.equals(type)) {

			int childIndex = intField(payload, "childIndex", -1);

			int sequence = intField(payload, "sequence", -1);

			boolean firstDelivery = onePassPhaseOneEnrichmentCompletionTracker.
					acceptBatch(uid, epoch, alias, childIndex, expectedWorkers, sourceWorker, sequence);

			if (!firstDelivery) {
				return;
			}

			JsonNode items = payload.get("items");

			if (items == null || !items.isArray()) {
				throw new IllegalStateException("ENRICH_BATCH has no items array: " + payload);
			}

			String baseKey = onePassBaseKeyByUid.get(uid);

			for (JsonNode item : items) {
				JsonNode tuplePayload = item.get("tuple");
				if (tuplePayload == null || tuplePayload.isNull()) {
					throw new IllegalStateException("ENRICH_BATCH item has no tuple: " + item);
				}

				double partialWeight = doubleField(item, "partialWeight", 0.0d);

				processShardedPhaseOneEnrichmentWork(onePass, tuplePayload, partialWeight, childIndex, uid,
						baseKey, expectedWorkers, epoch, alias, collector);
			}

			maybeAdvanceOnePassPhaseOneEnrichmentStage(onePass, uid, epoch, alias, childIndex, expectedWorkers, collector);

			return;
		}

		if (OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_SOURCE_DONE.equals(type)) {

			int childIndex = intField(payload, "childIndex", -1);
			int lastSequence = intField(payload, "lastSequence", -1);

			onePassPhaseOneEnrichmentCompletionTracker.acceptSourceDone(uid, epoch, alias, childIndex,
					expectedWorkers, sourceWorker, lastSequence);

			maybeAdvanceOnePassPhaseOneEnrichmentStage(onePass, uid, epoch, alias, childIndex, expectedWorkers, collector);
			return;
		}

		throw new IllegalStateException("Unknown sharded Phase-1 state-transfer type: " + type + ", payload=" + payload);
	}

	private void handleShardedPhaseOneEndAlias(Datapoint node, OnePassSamplerSdeSynopsis onePass, int uid, String alias,
											   String resultId, String nextCommand, String nextAlias,
											   int expectedWorkers, Collector<Estimation> collector) {

		int epoch = intField(node.getValues(), "epoch", -1);

		if (epoch <= 0) {
			throw new IllegalStateException("Sharded END_ALIAS requires epoch > 0: " + node.getValues());
		}

		Integer expectedEpoch = onePassPhaseOneEpochByUid.get(uid);

		if (expectedEpoch == null || expectedEpoch != epoch) {

			throw new IllegalStateException("END_ALIAS epoch mismatch." + " uid=" + uid +
					", expected=" + expectedEpoch + ", received=" + epoch);
		}

		onePassTupleBufferGate.sealAlias(uid, alias);
		CompiledOnePassPlan.DirectedJoinEdge parentEdge = onePass.getPlan().getParentEdge(alias);
		String activeEdgeId = parentEdge == null ? "" : parentEdge.getEdgeId();

		//END_ALIAS metadata is recorded now, but local final SOURCE_DONE is NOT.
		onePassPhaseOneCompletionTracker.acceptLocalEndAlias(uid, epoch, alias, expectedWorkers, resultId, nextCommand,
				nextAlias, onePassBaseKeyByUid.get(uid), activeEdgeId);

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(alias);

		/*
		 * Leaf or existing one-child chain:
		 * Every original tuple has already produced its final parent contribution,
		 * so final source generation can close immediately.
		 */
		if (childEdges.size() <= 1) {
			finishFinalPhaseOneSourceGeneration(onePass, uid, epoch, alias, expectedWorkers, collector);

			return;
		}

		/*
		 * Branching alias:
		 *
		 * Original tuples consumed child 0. Their first remote enrichment target is
		 * childIndex 1.
		 */
		int firstEnrichmentChildIndex = 1;

		flushEnrichmentStageAndDeclareDone(uid, onePassBaseKeyByUid.get(uid), expectedWorkers, epoch, alias,
				firstEnrichmentChildIndex, collector);

		maybeAdvanceOnePassPhaseOneEnrichmentStage(onePass, uid, epoch, alias, firstEnrichmentChildIndex,
				expectedWorkers, collector);
	}

	private void maybeEmitLocalPhaseOneShardReady(int uid, int epoch, String alias, OnePassSamplerSdeSynopsis onePass,
												  Collector<Estimation> collector) {

		OnePassPhaseOneCompletionTracker.ReadySnapshot ready =
				onePassPhaseOneCompletionTracker.readySnapshotIfComplete(uid, epoch, alias);

		if (ready == null) {
			return;
		}

		/*
		 * IMPORTANT:
		 * These statistics are intentionally read only after every final
		 * SHARD_BATCH/SOURCE_DONE dependency is satisfied.
		 * Reading them at END_ALIAS can be premature because remote final contributions may still be in flight.
		 */
		Map<String, Object> summary = onePass.getLocalPhaseOneEdgeSummary(ready.activeEdgeId);
		int localKeyCount = ((Number) summary.get("numberOfKeys")).intValue();
		double localTotalWeight = ((Number) summary.get("totalWeight")).doubleValue();
		long localSeenTuples = onePass.getLocalPhaseOneSeenTupleCount(ready.alias);

		Map<String, Object> payload = new LinkedHashMap<String, Object>();

		payload.put("type", "LOCAL_PHASE1_SHARD_READY");
		payload.put("protocol", "SHARDED_PHASE1_V1");
		payload.put("phase", "PHASE1");
		payload.put("uid", uid);
		payload.put("workerId", pId);
		payload.put("expectedWorkers", ready.expectedWorkers);
		payload.put("epoch", ready.epoch);
		payload.put("alias", ready.alias);
		payload.put("resultId", ready.resultId);
		payload.put("nextCommand", ready.nextCommand);
		payload.put("nextAlias", ready.nextAlias);
		payload.put("baseKey", ready.baseKey);
		payload.put("activeEdgeId", ready.activeEdgeId);
		payload.put("localKeyCount", localKeyCount);
		payload.put("localTotalWeight", localTotalWeight);
		payload.put("localSeenTuples", localSeenTuples);

		String json;
		try {
			json = MAPPER.writeValueAsString(payload);
		} catch (Exception e) {
			throw new IllegalStateException("Could not serialize LOCAL_PHASE1_SHARD_READY", e);
		}

		String reduceKey = uid + "_PHASE1_READY_" + ready.resultId;

		collector.collect(new Estimation(uid, reduceKey, 76, ONEPASS_SYNOPSIS_ID, reduceKey, json,
				new String[] {"LOCAL_PHASE1_SHARD_READY",
						ready.resultId,
						ready.alias,
						Integer.toString(ready.epoch),
						Integer.toString(pId),
						Integer.toString(ready.expectedWorkers)},
						ready.expectedWorkers));
	}

	private boolean isOnePassShardedPhaseOneTransitionRequest(Request request) {
		if (request == null
				|| request.getSynopsisID() != ONEPASS_SYNOPSIS_ID
				|| request.getRequestID() != 7) {
			return false;
		}

		JsonNode payload = request.getParameters();
		if (payload == null || payload.isNull()) {
			return false;
		}

		if (!"SHARDED_PHASE1_V1".equals(textField(payload, "protocol", ""))) {
			return false;
		}

		String type = textField(payload, "type", "");
		return "START_NEXT_ALIAS".equals(type) || "START_PHASE_2".equals(type);
	}

	private void handleOnePassShardedPhaseOneTransitionRequest(
			Request request,
			ArrayList<Synopsis> synopses,
			Collector<Estimation> collector) {

		JsonNode payload = request.getParameters();
		String command = textField(payload, "type", "");
		int uid = request.getUID();
		int nextEpoch = intField(payload, "epoch", -1);
		String nextAlias = textField(payload, "nextAlias", "");

		Integer currentEpoch = onePassPhaseOneEpochByUid.get(uid);
		int completedEpoch = intField(payload, "completedEpoch", -1);

		if (currentEpoch == null || currentEpoch.intValue() != completedEpoch) {
			throw new IllegalStateException(
					"Phase-1 transition epoch mismatch. uid=" + uid
							+ ", current=" + currentEpoch
							+ ", completed=" + completedEpoch);
		}

		if ("START_NEXT_ALIAS".equals(command)) {
			onePassPhaseOneEpochByUid.put(uid, nextEpoch);

			List<JsonNode> released = onePassTupleBufferGate.activateAliasAndDrain(uid, nextAlias);
			OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

			System.out.println("[OnePass SHARDED TRANSITION]" + " uid=" + uid + ", workerId=" + pId +
					", completedEpoch=" + completedEpoch + ", nextEpoch=" + nextEpoch +
					", nextAlias=" + nextAlias + ", released=" + released.size());

			for (JsonNode buffered : released) {
				//Buffered distributed Phase-1 tuples must re-enter the sharded path.
				processShardedPhaseOneTuple(onePass, buffered, collector
				);
			}

			processPendingOnePassEndAlias(uid, nextAlias, synopses, collector);
			return;
		}

		if ("START_PHASE_2".equals(command)) {
			onePassPhaseOneEpochByUid.put(uid, nextEpoch);
			OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

			if (onePass == null) {
				throw new IllegalStateException("START_PHASE_2 reached worker without OnePass synopsis." +
						" uid=" + uid + ", worker=" + pId);
			}

			String rootAlias = onePass.getPlan().getRootAlias();

			if (!rootAlias.equals(nextAlias)) {
				throw new IllegalStateException("START_PHASE_2 nextAlias mismatch." + " uid=" + uid +
						", expectedRoot=" + rootAlias + ", received=" + nextAlias);
			}

			//No replicated OnePassPhaseOneResult is constructed.
			onePass.startShardedPhaseTwo(pId);

			/*
			 * Root tuples that raced ahead of this RequestTopic transition are sitting
			 * behind the tuple gate.
			 */
			List<JsonNode> released = onePassTupleBufferGate.activateAliasAndDrain(uid, rootAlias);

			System.out.println("[OnePass SHARDED PHASE2 START]" + " uid=" + uid + ", worker=" + pId +
					", rootAlias=" + rootAlias + ", released=" + released.size() +
					", pendingStateMessages=" + pendingOnePassPhaseTwoStateCount(uid));

			//Process local root tuples first.
			for (JsonNode buffered : released) {
				processShardedPhaseTwoRootTuple(onePass, buffered, collector);
			}

			//Then process any State-Topic enrichment that reached this worker before START_PHASE_2.
			drainPendingOnePassPhaseTwoState(uid, onePass, collector);

			//END_ALIAS(root) may also have raced ahead of the transition.
			processPendingOnePassEndAlias(uid, rootAlias, synopses, collector);
		}
	}

	private boolean isOnePassPhaseOneDebugExportRequest(Request request) {

		if (request == null || request.getSynopsisID() != ONEPASS_SYNOPSIS_ID || request.getRequestID() != 79) {

			return false;
		}

		JsonNode parameters = request.getParameters();

		if (parameters == null || parameters.isNull()) {

			return false;
		}

		return "DEBUG_EXPORT_PHASE1_INDEXES".equals(textField(parameters, "onePassCommand", "")
		);
	}

	private void handleOnePassPhaseOneDebugExportRequest(Request request, ArrayList<Synopsis> synopses) throws Exception {

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsis(request, synopses);

		if (onePass == null) {
			throw new IllegalStateException("DEBUG_EXPORT_PHASE1_INDEXES could not find OnePass synopsis." + " uid=" +
					request.getUID() + ", worker=" + pId + ", key=" + request.getKey());
		}

		JsonNode parameters = request.getParameters();

		String outputDirectory = textField(parameters, "debugOutputDirectory",
				"/tmp/onepass-phase1-validator");

		int expectedWorkers = request.getNoOfP() > 0 ?
				request.getNoOfP() : getRuntimeContext().getNumberOfParallelSubtasks();

		OnePassPhaseOneValidatorExporter.exportWorkerShard(onePass, request.getUID(), pId,
				expectedWorkers, outputDirectory);
	}

	private void emitFinalPhaseOneContribution(OnePassSamplerSdeSynopsis onePass,
											   OnePassPhaseOneContribution contribution, int uid, String baseKey,
											   int expectedWorkers, int epoch, String alias, Collector<Estimation> collector) {

		if (contribution == null || contribution.getDelta() == 0.0d) {
			return;
		}

		int targetWorker = OnePassShardOwnership.ownerForEdgeKey(contribution.getEdgeId(), contribution.getJoinKey(), expectedWorkers);

		if (targetWorker == pId) {
			//Local final-index fast path.
			onePass.applyPhaseOneContribution(contribution.getEdgeId(), contribution.getJoinKey(), contribution.getDelta());
			return;
		}

		/*
		 * Remote final-index path. Existing combine/batch logic remains unchanged.
		 */
		for (Estimation stateMessage : onePassPhaseOneTransferBuffer.addRemoteContribution(uid, baseKey,
				expectedWorkers, pId, targetWorker, epoch, alias, contribution)) {
			collector.collect(stateMessage);
		}
	}

	private void routePhaseOneEnrichmentWork(OnePassSamplerSdeSynopsis onePass, JsonNode tuplePayload,
											 double partialWeight, int childIndex, int uid,
											 String baseKey, int expectedWorkers, int epoch, String alias,
											 Collector<Estimation> collector) {

		if (partialWeight == 0.0d) {
			return;
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(tuplePayload);

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(alias);

		if (childIndex <= 0 || childIndex >= childEdges.size()) {

			throw new IllegalArgumentException("Invalid enrichment childIndex=" + childIndex + " for alias=" + alias +
					", childCount=" + childEdges.size());
		}

		CompiledOnePassPlan.DirectedJoinEdge childEdge = childEdges.get(childIndex);
		JoinValue lookupKey = JoinValue.fromTuple(tuple, childEdge.getParentFields()
				);
		int targetWorker = OnePassShardOwnership.ownerForEdgeKey(childEdge.getEdgeId(), lookupKey, expectedWorkers);

		if (targetWorker == pId) {

			/*
			 * Local child-index fast path.
			 *
			 * This may recursively consume multiple consecutive child indexes if
			 * they all happen to be owned by the same worker.
			 */
			processShardedPhaseOneEnrichmentWork(onePass, tuplePayload, partialWeight, childIndex,
					uid, baseKey, expectedWorkers, epoch, alias, collector);
			return;
		}

		for (Estimation message : onePassPhaseOneEnrichmentBuffer.addRemoteWork(uid, baseKey, expectedWorkers, pId,
				targetWorker, epoch, alias, childIndex, tuplePayload, partialWeight)) {
			collector.collect(message);
		}
	}

	private void processShardedPhaseOneEnrichmentWork(OnePassSamplerSdeSynopsis onePass, JsonNode tuplePayload,
													  double partialWeight, int childIndex, int uid, String baseKey,
													  int expectedWorkers, int epoch, String alias,
													  Collector<Estimation> collector) {

		if (partialWeight == 0.0d) {
			return;
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(tuplePayload);

		if (!alias.equals(tuple.getTable())) {

			throw new IllegalStateException("Enrichment alias mismatch." + " messageAlias=" + alias + ", tupleAlias="
					+ tuple.getTable());
		}

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(alias);

		if (childIndex <= 0 || childIndex >= childEdges.size()) {
			throw new IllegalArgumentException("Invalid enrichment childIndex=" + childIndex + " for alias=" + alias +
					", childCount=" + childEdges.size());
		}

		/*
		 * This worker owns the required child edge/key by construction.
		 */
		double childWeight = onePass.lookupShardedPhaseOneChildWeight(tuplePayload, childIndex);

		double enrichedWeight = partialWeight * childWeight;

		if (enrichedWeight == 0.0d) {
			return;
		}

		int nextChildIndex = childIndex + 1;

		if (nextChildIndex < childEdges.size()) {
			routePhaseOneEnrichmentWork(onePass, tuplePayload, enrichedWeight, nextChildIndex, uid, baseKey,
					expectedWorkers, epoch, alias, collector);

			return;
		}

		/*
		 * Last child consumed: now create the normal final parent contribution.
		 */
		OnePassPhaseOneContribution contribution = onePass.buildShardedPhaseOneParentContribution(tuplePayload, enrichedWeight);
		emitFinalPhaseOneContribution(onePass, contribution, uid, baseKey, expectedWorkers, epoch, alias, collector);
	}

	private void flushEnrichmentStageAndDeclareDone(int uid, String baseKey, int expectedWorkers, int epoch,
													String alias, int childIndex, Collector<Estimation> collector) {
		/*
		 * Flush every remaining remote work item generated by this source for the
		 * destination child hop.
		 */
		for (Estimation batch : onePassPhaseOneEnrichmentBuffer.flushStage(uid, epoch, alias, childIndex)) {
			collector.collect(batch);
		}

		/*
		 * Remote destinations receive explicit sequence-aware done markers.
		 */
		for (Estimation done : onePassPhaseOneEnrichmentBuffer.
				buildStageDoneMessages(uid, baseKey, expectedWorkers, pId, epoch, alias, childIndex)) {
			collector.collect(done);
		}

		/*
		 * Local-target work used the direct fast path and is already processed.
		 */
		onePassPhaseOneEnrichmentCompletionTracker.acceptLocalSourceDone(uid, epoch, alias, childIndex, expectedWorkers, pId);
	}

	private void maybeAdvanceOnePassPhaseOneEnrichmentStage(OnePassSamplerSdeSynopsis onePass, int uid, int epoch,
															String alias, int childIndex, int expectedWorkers,
															Collector<Estimation> collector) {

		boolean complete =
				onePassPhaseOneEnrichmentCompletionTracker.markCompleteIfReady(uid, epoch, alias, childIndex);

		if (!complete) {
			return;
		}

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(alias);

		if (childIndex <= 0 || childIndex >= childEdges.size()) {
			throw new IllegalStateException("Completed invalid enrichment childIndex=" + childIndex +
					" for alias=" + alias + ", childCount=" + childEdges.size());
		}

		int nextChildIndex = childIndex + 1;
		String baseKey = onePassBaseKeyByUid.get(uid);

		if (nextChildIndex < childEdges.size()) {

			/*
			 * All childIndex input on this source worker is now known complete.
			 * Therefore, this source has generated every output item that can targetnextChildIndex.
			 */
			flushEnrichmentStageAndDeclareDone(uid, baseKey, expectedWorkers, epoch, alias, nextChildIndex, collector);

			//Remote DONE markers for nextChildIndex may already have arrived.Re-check immediately.

			maybeAdvanceOnePassPhaseOneEnrichmentStage(onePass, uid, epoch, alias, nextChildIndex, expectedWorkers, collector);
			return;
		}

		/*
		 * Last child hop is complete on this worker.
		 *
		 * Every enrichment item that can produce a final parent contribution has
		 * now been processed, so this source can finally close the existing final
		 * SHARD_BATCH/SOURCE_DONE stream.
		 */
		finishFinalPhaseOneSourceGeneration(onePass, uid, epoch, alias, expectedWorkers, collector
		);
	}

	private void finishFinalPhaseOneSourceGeneration(OnePassSamplerSdeSynopsis onePass, int uid, int epoch,
													 String alias, int expectedWorkers,
													 Collector<Estimation> collector) {

		String baseKey = onePassBaseKeyByUid.get(uid);

		/*
		 * Every remaining remote final contribution must be emitted before
		 * SOURCE_DONE.
		 */
		for (Estimation batch : onePassPhaseOneTransferBuffer.flushAlias(uid, epoch, alias)) {
			collector.collect(batch);
		}

		for (Estimation done : onePassPhaseOneTransferBuffer.
				buildSourceDoneMessages(uid, baseKey, expectedWorkers, pId, epoch, alias)) {
			collector.collect(done);
		}

		//Final local-target contributions were already applied directly.
		onePassPhaseOneCompletionTracker.acceptLocalSourceDone(uid, epoch, alias, expectedWorkers, pId);
		maybeEmitLocalPhaseOneShardReady(uid, epoch, alias, onePass, collector);
	}

	private int pendingOnePassPhaseTwoStateCount(int uid) {

		List<JsonNode> pending = pendingOnePassPhaseTwoStateByUid.get(uid);
		return pending == null ? 0 : pending.size();
	}

	private void drainPendingOnePassPhaseTwoState(int uid, OnePassSamplerSdeSynopsis onePass, Collector<Estimation> collector) {
		List<JsonNode> pending = pendingOnePassPhaseTwoStateByUid.remove(uid);
		if (pending == null || pending.isEmpty()) {
			return;
		}

		for (JsonNode payload : pending) {
			handleOnePassPhaseTwoStateTransferPayload(payload, onePass, collector);
		}
	}

	private void processShardedPhaseTwoRootTuple(OnePassSamplerSdeSynopsis onePass, JsonNode payload, Collector<Estimation> collector) {

		int uid = onePass.getSynopsisID();
		int expectedWorkers = onePassExpectedWorkersByUid.get(uid);
		String baseKey = onePassBaseKeyByUid.get(uid);

		String resultId = shardedPhaseTwoResultId(uid);

		OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

		String rootAlias = onePass.getPlan().getRootAlias();

		if (!rootAlias.equals(tuple.getTable())) {
			throw new IllegalStateException("Sharded Phase 2 received non-root tuple." + " expected=" + rootAlias +
					", actual=" + tuple.getTable());
		}

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(rootAlias);

		//Count the ORIGINAL root tuple exactly once and evaluate root own weight.
		double partialWeight = onePass.beginShardedPhaseTwoRootTuple(payload);

		if (partialWeight == 0.0d) {
			return;
		}

		//Hypothetical single-relation query.
		if (childEdges.isEmpty()) {
			onePass.acceptShardedPhaseTwoRootCandidate(payload, partialWeight);
			return;
		}

		//Data router has already routed the root tuple to child-edge-0 owner.
		double firstChildWeight = onePass.lookupShardedPhaseTwoRootChildWeight(payload, 0);

		partialWeight = OnePassShardedPhaseTwoState.
				checkedMultiply(partialWeight, firstChildWeight, "phase2RootWeight.child0");

		if (partialWeight == 0.0d) {
			return;
		}

		if (childEdges.size() == 1) {
			onePass.acceptShardedPhaseTwoRootCandidate(payload, partialWeight);
			return;
		}

		routeShardedPhaseTwoRootEnrichment(onePass, payload, partialWeight, 1, uid,
				resultId, baseKey, expectedWorkers, rootAlias, collector);
	}

	private void routeShardedPhaseTwoRootEnrichment(OnePassSamplerSdeSynopsis onePass, JsonNode tuplePayload,
													double partialWeight, int childIndex, int uid, String resultId,
													String baseKey, int expectedWorkers, String rootAlias,
													Collector<Estimation> collector) {

		if (partialWeight == 0.0d) {
			return;
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(tuplePayload);

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(rootAlias);

		if (childIndex <= 0 || childIndex >= childEdges.size()) {
			throw new IllegalArgumentException("Invalid Phase-2 enrichment childIndex=" + childIndex +
					", childCount=" + childEdges.size());
		}

		CompiledOnePassPlan.DirectedJoinEdge childEdge = childEdges.get(childIndex);

		JoinValue lookupKey = JoinValue.fromTuple(tuple, childEdge.getParentFields());

		int targetWorker = OnePassShardOwnership.ownerForEdgeKey(childEdge.getEdgeId(), lookupKey, expectedWorkers);

		if (targetWorker == pId) {
			processShardedPhaseTwoRootEnrichment(onePass, tuplePayload, partialWeight, childIndex, uid,
					resultId, baseKey, expectedWorkers, rootAlias, collector);
			return;
		}

		for (Estimation message : onePassPhaseTwoEnrichmentBuffer.addRemoteWork(uid, resultId, baseKey,
				expectedWorkers, pId, targetWorker, rootAlias, childIndex, tuplePayload, partialWeight)) {
			collector.collect(message);
		}
	}

	private void processShardedPhaseTwoRootEnrichment(OnePassSamplerSdeSynopsis onePass, JsonNode tuplePayload,
													  double partialWeight, int childIndex, int uid, String resultId,
													  String baseKey, int expectedWorkers, String rootAlias,
													  Collector<Estimation> collector) {

		if (partialWeight == 0.0d) {
			return;
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(tuplePayload);

		if (!rootAlias.equals(tuple.getTable())) {
			throw new IllegalStateException("Phase-2 enrichment root mismatch." + " messageRoot=" + rootAlias +
					", tupleAlias=" + tuple.getTable());
		}

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(rootAlias);

		double childWeight = onePass.lookupShardedPhaseTwoRootChildWeight(tuplePayload, childIndex);
		double enrichedWeight = OnePassShardedPhaseTwoState.checkedMultiply(partialWeight,
				childWeight, "phase2RootWeight.child" + childIndex);

		if (enrichedWeight == 0.0d) {
			return;
		}

		int nextChildIndex = childIndex + 1;
		if (nextChildIndex < childEdges.size()) {

			routeShardedPhaseTwoRootEnrichment(onePass, tuplePayload, enrichedWeight, nextChildIndex,
					uid, resultId, baseKey, expectedWorkers, rootAlias, collector);

			return;
		}

		//All root child continuations are now incorporated.
		onePass.acceptShardedPhaseTwoRootCandidate(tuplePayload, enrichedWeight);
	}

	private boolean isOnePassPhaseTwoStateTransfer(Datapoint node) {

		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		JsonNode payload = node.getValues();
		String protocol = textField(payload, "protocol",
				"");

		if (!OnePassPhaseTwoEnrichmentBuffer.PROTOCOL.equals(protocol)) {
			return false;
		}

		String type = textField(payload, "type", "");

		return OnePassPhaseTwoEnrichmentBuffer.TYPE_ROOT_ENRICH_BATCH.equals(type)
				|| OnePassPhaseTwoEnrichmentBuffer.TYPE_ROOT_ENRICH_SOURCE_DONE.equals(type);
	}

	private void handleOnePassPhaseTwoStateTransfer(Datapoint node, ArrayList<Synopsis> synopses,
													Collector<Estimation> collector) {

		JsonNode payload = node.getValues();

		int uid = intField(payload, "uid", -1);

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

		if (onePass == null) {
			throw new IllegalStateException("Phase-2 state message reached worker without OnePass synopsis." +
					" uid=" + uid + ", worker=" + pId);
		}

		/*
		 * State Topic and Request Topic are independent Flink inputs.
		 * A Phase-2 enrichment batch may reach the target worker before that worker
		 * has consumed START_PHASE_2.
		 */
		if (!onePass.isShardedPhaseTwoActive()) {
            List<JsonNode> pending = pendingOnePassPhaseTwoStateByUid.computeIfAbsent(uid, k -> new ArrayList<JsonNode>());
            pending.add(payload.deepCopy());
			System.out.println("[OnePass PHASE2 STATE DEFERRED]" + " uid=" + uid + ", type=" +
					textField(payload, "type", "") + ", worker=" + pId + ", pending=" + pending.size());

			return;
		}

		handleOnePassPhaseTwoStateTransferPayload(payload, onePass, collector);
	}

	private void handleOnePassPhaseTwoStateTransferPayload(JsonNode payload, OnePassSamplerSdeSynopsis onePass,
														   Collector<Estimation> collector) {

		int uid = intField(payload, "uid", -1);
		String resultId = textField(payload, "resultId", shardedPhaseTwoResultId(uid));
		String rootAlias = textField(payload, "rootAlias", onePass.getPlan().getRootAlias());
		int childIndex = intField(payload, "childIndex", -1);
		int sourceWorker = intField(payload, "sourceWorker", -1);
		int targetWorker = intField(payload, "targetWorker", -1);
		int expectedWorkers = intField(payload, "expectedWorkers", 0);
		String type = textField(payload, "type", "");

		if (targetWorker != pId) {
			throw new IllegalStateException("Phase-2 state message reached wrong worker." + " target=" + targetWorker + ", actual=" + pId);
		}

		if (OnePassPhaseTwoEnrichmentBuffer.TYPE_ROOT_ENRICH_BATCH.equals(type)) {

			int sequence = intField(payload, "sequence", -1);
			boolean firstDelivery = onePassPhaseTwoEnrichmentCompletionTracker.acceptBatch(uid, resultId, rootAlias,
					childIndex, expectedWorkers, sourceWorker, sequence);

			if (!firstDelivery) {
				return;
			}

			JsonNode items = payload.get("items");

			if (items == null || !items.isArray()) {
				throw new IllegalStateException("PHASE2_ROOT_ENRICH_BATCH has no items array: " + payload);
			}

			String baseKey = onePassBaseKeyByUid.get(uid);

			for (JsonNode item : items) {
				JsonNode tuplePayload = item.get("tuple");

				if (tuplePayload == null || tuplePayload.isNull()) {
					throw new IllegalStateException("Phase-2 enrichment item has no tuple: " + item);
				}

				double partialWeight = doubleField(item, "partialWeight", 0.0d);
				processShardedPhaseTwoRootEnrichment(onePass, tuplePayload, partialWeight, childIndex,
						uid, resultId, baseKey, expectedWorkers, rootAlias, collector);
			}

			maybeAdvanceShardedPhaseTwoEnrichmentStage(onePass, uid, resultId, rootAlias, childIndex,
					expectedWorkers, collector);

			return;
		}

		if (OnePassPhaseTwoEnrichmentBuffer.TYPE_ROOT_ENRICH_SOURCE_DONE.equals(type)) {

			int lastSequence = intField(payload, "lastSequence", -1);

			onePassPhaseTwoEnrichmentCompletionTracker.acceptSourceDone(uid, resultId, rootAlias, childIndex,
					expectedWorkers, sourceWorker, lastSequence);
			maybeAdvanceShardedPhaseTwoEnrichmentStage(onePass, uid, resultId, rootAlias, childIndex,
					expectedWorkers, collector);
			return;
		}

		throw new IllegalStateException("Unknown Phase-2 sharded state message: " + type);
	}

	private void flushShardedPhaseTwoEnrichmentStageAndDeclareDone(int uid, String resultId,
																   String baseKey, int expectedWorkers,
																   String rootAlias,
																   int childIndex,
																   Collector<Estimation> collector) {

		for (Estimation batch : onePassPhaseTwoEnrichmentBuffer.flushStage(uid, resultId, rootAlias, childIndex)) {
			collector.collect(batch);
		}

		for (Estimation done : onePassPhaseTwoEnrichmentBuffer.buildStageDoneMessages(uid, resultId, baseKey,
				expectedWorkers, pId, rootAlias, childIndex)) {
			collector.collect(done);
		}

		//Local target work was already processed synchronously.
		onePassPhaseTwoEnrichmentCompletionTracker.acceptLocalSourceDone(uid, resultId, rootAlias, childIndex,
				expectedWorkers, pId);
	}

	private void maybeAdvanceShardedPhaseTwoEnrichmentStage(OnePassSamplerSdeSynopsis onePass, int uid, String resultId,
															String rootAlias, int childIndex, int expectedWorkers,
															Collector<Estimation> collector) {

		boolean complete = onePassPhaseTwoEnrichmentCompletionTracker.
				markCompleteIfReady(uid, resultId, rootAlias, childIndex);

		if (!complete) {
			return;
		}

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(rootAlias);
		int nextChildIndex = childIndex + 1;
		String baseKey = onePassBaseKeyByUid.get(uid);

		if (nextChildIndex < childEdges.size()) {
			/*
			 * Every childIndex input on this source worker is now complete.
			 * Therefore, all work targeting nextChildIndex has been generated.
			 */
			flushShardedPhaseTwoEnrichmentStageAndDeclareDone(uid, resultId, baseKey, expectedWorkers, rootAlias, nextChildIndex, collector);

			//Remote DONE markers may already be waiting.
			maybeAdvanceShardedPhaseTwoEnrichmentStage(onePass, uid, resultId, rootAlias, nextChildIndex, expectedWorkers, collector);

			return;
		}

		/*
		 * Last child stage is complete.
		 * No more fully weighted root candidates can arrive at this worker.
		 */
		emitLocalShardedPhaseTwoRootSummary(onePass, uid, resultId, expectedWorkers, collector);
	}

	private void handleShardedPhaseTwoEndRoot(Datapoint node, OnePassSamplerSdeSynopsis onePass, int uid, String alias,
											  String resultId, int expectedWorkers, Collector<Estimation> collector) {

		String rootAlias = onePass.getPlan().getRootAlias();
		if (!rootAlias.equals(alias)) {
			throw new IllegalStateException("PHASE2 END_ALIAS must target root." + " expected=" + rootAlias + ", actual=" + alias);
		}

		onePassTupleBufferGate.sealAlias(uid, rootAlias);
		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(rootAlias);

		/*
		 * No enrichment hop exists:
		 *
		 * 0 children:
		 *   local root own weight already entered reservoir.
		 *
		 * 1 child:
		 *   root tuple was routed directly to child-0 owner and was fully processed synchronously.
		 */
		if (childEdges.size() <= 1) {
			emitLocalShardedPhaseTwoRootSummary(onePass, uid, resultId, expectedWorkers, collector);

			return;
		}

		/*
		 * Original root tuples consumed child 0.
		 * The first State-Topic enrichment destination is child 1.
		 */
		int firstRemoteChildIndex = 1;
		flushShardedPhaseTwoEnrichmentStageAndDeclareDone(uid, resultId, onePassBaseKeyByUid.get(uid),
				expectedWorkers, rootAlias, firstRemoteChildIndex, collector);

		maybeAdvanceShardedPhaseTwoEnrichmentStage(onePass, uid, resultId, rootAlias, firstRemoteChildIndex,
				expectedWorkers, collector);
	}

	private void emitLocalShardedPhaseTwoRootSummary(OnePassSamplerSdeSynopsis onePass, int uid, String resultId,
													 int expectedWorkers, Collector<Estimation> collector) {

		String dedupeKey = uid + "|" + resultId + "|worker=" + pId;
		if (!emittedOnePassPhaseTwoLocalSummaries.add(dedupeKey)) {
			return;
		}

		int actualParallelism = 1;
		try {
			actualParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
		} catch (Exception ignored) {
			actualParallelism = expectedWorkers;
		}

		Estimation localSummary = onePass.buildLocalShardedPhaseTwoRootSummaryEstimation(onePassBaseKeyByUid.get(uid),
				uid, pId, expectedWorkers, actualParallelism, resultId);

		collector.collect(localSummary);
		OnePassShardedPhaseTwoState localState = onePass.getLifecycle().getShardedPhaseTwoState();
		System.out.println("[OnePass LOCAL_PHASE2_ROOT_SUMMARY]" + " uid=" + uid + ", worker=" + pId + ", resultId=" +
				resultId + ", rootTuplesSeen=" + localState.getRootTuplesSeen() + ", positiveCandidates=" +
				localState.getPositiveRootCandidatesSeen() + ", totalWeight=" + localState.getTotalRootGroupWeight() +
				", reservoir=" + localState.getOrderedReservoir().size());
	}

	private static String shardedPhaseTwoResultId(int uid) {
		return "PHASE2_RESULT_" + uid;
	}

	private boolean isOnePassPhaseTwoDebugValidationRequest(Request request) {
		if (request == null || request.getSynopsisID() != ONEPASS_SYNOPSIS_ID || request.getRequestID() != 89) {
			return false;
		}

		JsonNode parameters = request.getParameters();
		if (parameters == null || parameters.isNull()) {
			return false;
		}

		return "DEBUG_VALIDATE_PHASE2_ROOT_SAMPLE".equals(textField(parameters, "onePassCommand", ""));
	}

	private void handleOnePassPhaseTwoDebugValidationRequest(Request request, ArrayList<Synopsis> synopses) throws Exception {

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsis(request, synopses);
		if (onePass == null) {
			throw new IllegalStateException("DEBUG_VALIDATE_PHASE2_ROOT_SAMPLE could not find OnePass synopsis." +
					" uid=" + request.getUID() + ", key=" + request.getKey() + ", workerId=" + pId);
		}

		int expectedWorkers = request.getNoOfP() > 0 ?
				request.getNoOfP() : getRuntimeContext().getNumberOfParallelSubtasks();

		String outputDirectory =  "/tmp/onepass-phase2-validator"; //OnePassPhaseTwoValidatorExporter.getOutputDirectory();
		JsonNode parameters = request.getParameters();

		if (parameters != null && !parameters.isNull()) {
			String requestedDirectory = textField(parameters, "outputDirectory", "");
			if (requestedDirectory != null && !requestedDirectory.trim().isEmpty()) {
				outputDirectory = requestedDirectory.trim();
			}
		}
		OnePassPhaseTwoValidatorExporter.exportInstalledRootSample(onePass, request.getUID(), pId, expectedWorkers, outputDirectory);
	}

	private boolean isOnePassPhaseTwoRootSampleChunk(Datapoint node) {
		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		JsonNode payload = node.getValues();

		return "GLOBAL_STATE_CHUNK".equals(textField(payload, "type", "")) &&
				ONEPASS_STATE_TYPE_PHASE2_ROOT_SAMPLE.equals(textField(payload, "stateType", ""));
	}

	private void handleOnePassPhaseTwoRootSampleChunk(Datapoint node, ArrayList<Synopsis> synopses,
													  Collector<Estimation> collector) {

		JsonNode chunk = node.getValues();
		String stateRef = textField(chunk, "stateRef", "");
		int chunkId = intField(chunk, "chunkId", -1);
		int chunkCount = intField(chunk, "chunkCount", -1);
		int workerId = intField(chunk, "workerId", -1);

		if (stateRef.isEmpty()) {
			throw new IllegalStateException("Phase-2 root-sample chunk has no stateRef: " + chunk);
		}

		if (workerId != pId) {
			throw new IllegalStateException("Phase-2 root-sample chunk reached wrong worker." +
					" target=" + workerId + ", actual=" + pId);
		}

		if (chunkId < 0 || chunkCount <= 0 || chunkId >= chunkCount) {
			throw new IllegalStateException("Invalid Phase-2 root-sample chunk metadata: " + chunk);
		}

		/*
		 * Already installed.
		 * Duplicate Kafka replay after completion is idempotent.
		 */
		if (installedOnePassPhaseTwoStateRefs.contains(stateRef)) {
			return;
		}

		Map<Integer, JsonNode> chunks = onePassStateChunksByRef.get(stateRef);

		if (chunks == null) {
			chunks = new HashMap<Integer, JsonNode>();
			onePassStateChunksByRef.put(stateRef, chunks);
		}

		//Every chunk for one stateRef must agree on chunkCount.
		for (JsonNode existing : chunks.values()) {
			int existingChunkCount = intField(existing, "chunkCount", -1);
			if (existingChunkCount != chunkCount) {
				throw new IllegalStateException("Conflicting Phase-2 chunkCount for stateRef=" + stateRef +
						": existing=" + existingChunkCount + ", received=" + chunkCount);
			}
			break;
		}

		JsonNode existing = chunks.get(chunkId);
		if (existing != null) {
			if (!existing.equals(chunk)) {
				throw new IllegalStateException("Conflicting duplicate Phase-2 chunk." + " stateRef=" + stateRef + ", chunkId=" + chunkId);
			}

			return;
		}

		chunks.put(chunkId, chunk.deepCopy());
		System.out.println("[OnePass PHASE2 STATE CHUNK]" + " uid=" +
				intField(chunk, "uid", -1) + ", stateRef=" + stateRef +
				", worker=" + pId + ", chunk=" + chunkId + "/" + chunkCount + ", received=" +
				chunks.size() + "/" + chunkCount);

		if (chunks.size() < chunkCount) {
			return;
		}

		JsonNode assembled = assembleOnePassPhaseTwoRootSample(stateRef, chunks, chunkCount);
		/*
		 * Release chunk objects BEFORE installation creates the lifecycle sample.
		 * assembled remains only as a local temporary variable.
		 */
		onePassStateChunksByRef.remove(stateRef);

		installCompletedOnePassPhaseTwoRootSample(node.getKey(), assembled, synopses, collector);
	}

	private JsonNode assembleOnePassPhaseTwoRootSample(String stateRef, Map<Integer, JsonNode> chunks, int chunkCount) {
		JsonNode first = chunks.get(0);
		if (first == null) {
			throw new IllegalStateException("Missing Phase-2 chunk 0 for stateRef=" + stateRef);
		}
		ObjectNode assembled = MAPPER.createObjectNode();
		assembled.put("type", ONEPASS_STATE_TYPE_PHASE2_ROOT_SAMPLE);
		assembled.put("stateType", ONEPASS_STATE_TYPE_PHASE2_ROOT_SAMPLE);
		assembled.put("stateRef", stateRef);


		copyIfPresent(first, assembled, "protocol");
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
		copyIfPresent(first, assembled, "sampleSize");
		copyIfPresent(first, assembled, "sampleInstanceCount");
		copyIfPresent(first, assembled, "rootTuplesSeen");
		copyIfPresent(first, assembled, "positiveRootCandidatesSeen");
		copyIfPresent(first, assembled, "totalRootGroupWeight");
		copyIfPresent(first, assembled, "datasetSeed");

		ArrayNode entries = MAPPER.createArrayNode();

		for (int chunkId = 0; chunkId < chunkCount; chunkId++) {
			JsonNode chunk = chunks.get(chunkId);
			if (chunk == null) {
				throw new IllegalStateException("Missing Phase-2 chunk " + chunkId + " for stateRef=" + stateRef);
			}
			requireSameChunkText(first, chunk, "resultId", stateRef);
			requireSameChunkText(first, chunk, "rootAlias", stateRef);
			requireSameChunkText(first, chunk, "baseKey", stateRef);
			requireSameChunkInt(first, chunk, "expectedWorkers", stateRef);
			requireSameChunkInt(first, chunk, "sampleSize", stateRef);
			requireSameChunkInt(first, chunk, "sampleInstanceCount", stateRef);
			JsonNode chunkEntries = chunk.get("entries");

			if (chunkEntries == null || !chunkEntries.isArray()) {
				throw new IllegalStateException("Phase-2 chunk has no entries array." + " stateRef=" + stateRef + ", chunkId=" + chunkId);
			}

			int declaredEntryCount = intField(chunk, "entryCount", -1);
			if (declaredEntryCount != chunkEntries.size()) {
				throw new IllegalStateException("Phase-2 chunk entryCount mismatch." + " stateRef=" + stateRef + ", chunkId=" + chunkId);
			}

			for (JsonNode entry : chunkEntries) {
				entries.add(entry);
			}
		}

		int expectedSampleInstances = intField(first, "sampleInstanceCount", -1);
		if (expectedSampleInstances >= 0 && entries.size() != expectedSampleInstances) {
			throw new IllegalStateException("Assembled Phase-2 sample count mismatch." + " stateRef=" + stateRef + ", expected=" + expectedSampleInstances + ", actual=" + entries.size());
		}

		assembled.set("entries", entries);

		return assembled;
	}

	private void requireSameChunkText(JsonNode first, JsonNode current, String field, String stateRef) {
		String expected = textField(first, field, "");
		String actual = textField(current, field, "");
		if (!expected.equals(actual)) {
			throw new IllegalStateException("Conflicting " + field + " across Phase-2 chunks." + " stateRef=" + stateRef);
		}
	}

	private void copyIfPresent(JsonNode source, ObjectNode target, String fieldName) {
		JsonNode value = source.get(fieldName);

		if (value != null && !value.isNull()) {
			target.set(fieldName, value.deepCopy()); //Do we need deepCopy TODO
		}
	}

	private void requireSameChunkInt(JsonNode first, JsonNode current, String field, String stateRef) {
		int expected = intField(first, field, Integer.MIN_VALUE);
		int actual = intField(current, field, Integer.MIN_VALUE);

		if (expected != actual) {
			throw new IllegalStateException("Conflicting " + field + " across Phase-2 chunks." + " stateRef=" + stateRef);
		}
	}

	private void installCompletedOnePassPhaseTwoRootSample(String workerKey, JsonNode state, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {
		int uid = intField(state, "uid", -1);

		if (uid < 0) {
			throw new IllegalStateException("Assembled Phase-2 sample has invalid uid: " + state);
		}

		String stateRef = textField(state, "stateRef", "");

		if (stateRef.isEmpty()) {
			throw new IllegalStateException("Assembled Phase-2 sample has no stateRef." + " uid=" + uid);
		}

		if (installedOnePassPhaseTwoStateRefs.contains(stateRef)) {
			return;
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

		if (onePass == null) {
			throw new IllegalStateException("Phase-2 sample reached worker without OnePass synopsis." + " uid=" + uid + ", worker=" + pId);
		}

		int expectedWorkers = intField(state, "expectedWorkers", getRuntimeContext().getNumberOfParallelSubtasks());

		if (expectedWorkers <= 0) {
			throw new IllegalStateException("Assembled Phase-2 sample has invalid expectedWorkers=" + expectedWorkers);
		}


		/*
		 * The lifecycle now becomes the authoritative owner of the installed
		 * sample. No assembled transport JSON is retained in an instance field.
		 */
		Map<String, Object> installSummary = onePass.installGlobalPhaseTwoRootSample(state);

		List<String> phaseThreeOrder = phaseThreeAliasOrder(onePass.getPlan());
		String firstPhaseThreeAlias = phaseThreeOrder.isEmpty() ? "" : phaseThreeOrder.get(0);

		installedOnePassPhaseTwoStateRefs.add(stateRef);
		String resultId = textField(state, "resultId", "PHASE2_RESULT_" + uid);
		String rootAlias = textField(state, "rootAlias", onePass.getPlan().getRootAlias());
		String baseKey = textField(state, "baseKey", OnePassShardOwnership.baseKeyFromWorkerKey(workerKey, expectedWorkers, pId));

		int sampleSize = intField(state, "sampleSize", 0);
		int sampleInstanceCount = intField(state, "sampleInstanceCount", 0);

		Map<String, Object> ready = new LinkedHashMap<String, Object>();

		ready.put("type", "LOCAL_PHASE2_ROOT_SAMPLE_INSTALLED");
		ready.put("protocol", "SHARDED_PHASE2_V1");
		ready.put("phase", "PHASE2");
		ready.put("uid", uid);
		ready.put("workerId", pId);
		ready.put("expectedWorkers", expectedWorkers);
		ready.put("resultId", resultId);
		ready.put("stateRef", stateRef);
		ready.put("rootAlias", rootAlias);
		ready.put("baseKey", baseKey);
		ready.put("sampleSize", sampleSize);
		ready.put("sampleInstanceCount", sampleInstanceCount);
		ready.put("rootTuplesSeen", longField(state, "rootTuplesSeen", 0L));
		ready.put("positiveRootCandidatesSeen", longField(state, "positiveRootCandidatesSeen", 0L));
		ready.put("totalRootGroupWeight", doubleField(state, "totalRootGroupWeight", 0.0d));
		ready.put("firstPhaseThreeAlias", firstPhaseThreeAlias);
		ready.put("phaseThreeAliasCount", phaseThreeOrder.size());

		String json;
		try {
			json = MAPPER.writeValueAsString(ready);
		} catch (Exception e) {
			throw new IllegalStateException("Could not serialize LOCAL_PHASE2_ROOT_SAMPLE_INSTALLED", e);
		}


		String reduceKey = uid + "_PHASE2_INSTALLED_" + resultId;
		collector.collect(new Estimation(uid, reduceKey, 85, ONEPASS_SYNOPSIS_ID, reduceKey, json,
				new String[]{"LOCAL_PHASE2_ROOT_SAMPLE_INSTALLED", stateRef, resultId, rootAlias,
						Integer.toString(pId), Integer.toString(expectedWorkers)}, expectedWorkers));

		System.out.println("[OnePass PHASE2 ROOT SAMPLE INSTALLED]" + " uid=" + uid + ", worker=" + pId +
				", stateRef=" + stateRef + ", sampleSize=" + sampleSize +
				", sampleInstanceCount=" + sampleInstanceCount + ", lifecycle=" +
				installSummary.get("nextLifecyclePhase"));
	}

	private boolean isOnePassShardedPhaseThreeTransitionRequest(Request request) {
		if (request == null || request.getSynopsisID() != ONEPASS_SYNOPSIS_ID || request.getRequestID() != 7) {
			return false;
		}

		JsonNode payload = request.getParameters();
		return payload != null && !payload.isNull() && OnePassPhaseThreeEnrichmentBuffer.PROTOCOL.
				equals(textField(payload, "protocol", "")) &&
				"START_PHASE_3_ALIAS".equals(textField(payload, "type", ""));
	}

	private void handleOnePassShardedPhaseThreeTransitionRequest(Request request, ArrayList<Synopsis> synopses,
																 Collector<Estimation> collector) {

		int uid = request.getUID();
		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

		if (onePass == null) {
			throw new IllegalStateException("START_PHASE_3_ALIAS reached worker without OnePass synopsis. uid=" + uid + ", worker=" + pId);
		}

		JsonNode payload = request.getParameters();
		String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));
		int expectedWorkers = intField(payload, "expectedWorkers", onePassExpectedWorkersByUid.getOrDefault(uid, 1));
		int aliasIndex = intField(payload, "aliasIndex", -1);

		if (alias.isEmpty() || expectedWorkers <= 1) {
			throw new IllegalStateException("Invalid sharded START_PHASE_3_ALIAS: " + payload);
		}

		List<String> order = phaseThreeAliasOrder(onePass.getPlan());
		if (aliasIndex < 0 || aliasIndex >= order.size() || !alias.equals(order.get(aliasIndex))) {
			throw new IllegalStateException(
					"Phase-3 traversal mismatch. alias=" + alias + ", aliasIndex=" + aliasIndex + ", expectedOrder=" + order);
		}

		onePass.startShardedPhaseThreeAlias(alias);
		List<JsonNode> released = onePassTupleBufferGate.activateAliasAndDrain(uid, alias);

		for (JsonNode buffered : released) {
			processShardedPhaseThreeTuple(onePass, buffered, collector);
		}

		// Process StateTopic work that raced ahead of the RequestTopic
		// transition before closing a deferred END_ALIAS marker.
		drainPendingOnePassPhaseThreeState(uid, onePass, collector);
		processPendingOnePassEndAlias(uid, alias, synopses, collector);

		System.out.println("[OnePass SHARDED PHASE3 START] uid=" + uid + ", worker=" + pId + ", alias=" + alias +
						", aliasIndex=" + aliasIndex + ", released=" + released.size());
	}

	private void processShardedPhaseThreeTuple(OnePassSamplerSdeSynopsis onePass, JsonNode payload, Collector<Estimation> collector) {

		int uid = onePass.getSynopsisID();
		int expectedWorkers = onePassExpectedWorkersByUid.get(uid);
		String baseKey = onePassBaseKeyByUid.get(uid);
		String alias = onePass.getShardedPhaseThreeActiveAlias();
		String resultId = shardedPhaseThreeResultId(uid, alias);

		OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
		if (alias == null || !alias.equals(tuple.getTable())) {
			throw new IllegalStateException("Phase-3 tuple alias mismatch. active=" + alias + ", received=" + tuple.getTable());
		}

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges =
				onePass.getPlan().getChildEdges(alias);

		double partialWeight = onePass.beginShardedPhaseThreeCandidate(payload);
		if (partialWeight == 0.0d) {
			return;
		}

		if (childEdges.isEmpty()) {
			// Router placed a leaf directly on the parent-edge selection owner.
			onePass.acceptShardedPhaseThreeCandidate(payload, partialWeight);
			return;
		}

		// Router placed an internal tuple on child-edge-0 owner.
		double childWeight = onePass.lookupShardedPhaseThreeChildWeight(payload, 0);
		double enriched = OnePassShardedPhaseTwoState.checkedMultiply(partialWeight, childWeight, "phase3Candidate.child0");

		if (enriched == 0.0d) {
			return;
		}

		// Stage 1 is either child-edge-1 or, for a one-child alias, the final
		// parent-edge selection owner.
		routeShardedPhaseThreeWork(onePass, payload, enriched, 1, uid, resultId, baseKey, expectedWorkers,
				alias, collector);
	}

	private int phaseThreeTargetWorker(OnePassSamplerSdeSynopsis onePass, OnePassTuple tuple, String alias,
									   int stageIndex, int expectedWorkers) {

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(alias);

		if (stageIndex >= 0 && stageIndex < childEdges.size()) {
			CompiledOnePassPlan.DirectedJoinEdge childEdge = childEdges.get(stageIndex);
			JoinValue key = JoinValue.fromTuple(tuple, childEdge.getParentFields());
			return OnePassShardOwnership.ownerForEdgeKey(childEdge.getEdgeId(), key, expectedWorkers);
		}

		if (stageIndex == childEdges.size()) {
			CompiledOnePassPlan.DirectedJoinEdge parentEdge = onePass.getPlan().getParentEdge(alias);
			if (parentEdge == null) {
				throw new IllegalStateException("Phase-3 alias has no parent edge: " + alias);
			}

			JoinValue key = JoinValue.fromTuple(tuple, parentEdge.getChildFields());
			return OnePassShardOwnership.ownerForEdgeKey(parentEdge.getEdgeId(), key, expectedWorkers);
		}

		throw new IllegalArgumentException(
				"Invalid Phase-3 stageIndex=" + stageIndex + " for alias=" + alias + ", childCount=" + childEdges.size());
	}

	private void routeShardedPhaseThreeWork(OnePassSamplerSdeSynopsis onePass, JsonNode tuplePayload,
											double partialWeight, int stageIndex, int uid, String resultId,
											String baseKey, int expectedWorkers, String alias,
											Collector<Estimation> collector) {

		if (partialWeight == 0.0d) {
			return;
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(tuplePayload);
		int targetWorker = phaseThreeTargetWorker(onePass, tuple, alias, stageIndex, expectedWorkers);

		if (targetWorker == pId) {
			processShardedPhaseThreeWork(onePass, tuplePayload, partialWeight, stageIndex, uid, resultId, baseKey,
					expectedWorkers, alias, collector);
			return;
		}

		for (Estimation message : onePassPhaseThreeEnrichmentBuffer.addRemoteWork(uid, resultId, baseKey,
				expectedWorkers, pId, targetWorker, alias, stageIndex, tuplePayload, partialWeight)) {
			collector.collect(message);
		}
	}

	private void processShardedPhaseThreeWork(OnePassSamplerSdeSynopsis onePass, JsonNode tuplePayload,
											  double partialWeight, int stageIndex, int uid, String resultId,
											  String baseKey, int expectedWorkers, String alias,
											  Collector<Estimation> collector) {

		if (partialWeight == 0.0d) {
			return;
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(tuplePayload);
		if (!alias.equals(tuple.getTable())) {
			throw new IllegalStateException("Phase-3 work alias mismatch. messageAlias=" + alias +
					", tupleAlias=" + tuple.getTable());
		}

		List<CompiledOnePassPlan.DirectedJoinEdge> childEdges = onePass.getPlan().getChildEdges(alias);

		if (stageIndex <= 0 || stageIndex > childEdges.size()) {
			throw new IllegalArgumentException("Invalid transported Phase-3 stageIndex=" + stageIndex +
					", alias=" + alias + ", childCount=" + childEdges.size());
		}

		if (stageIndex == childEdges.size()) {
			// Final parent-edge selection owner.
			onePass.acceptShardedPhaseThreeCandidate(tuplePayload, partialWeight);
			return;
		}

		double childWeight = onePass.lookupShardedPhaseThreeChildWeight(tuplePayload, stageIndex);
		double enriched = OnePassShardedPhaseTwoState.checkedMultiply(partialWeight, childWeight,
				"phase3Candidate.child" + stageIndex);

		if (enriched == 0.0d) {
			return;
		}

		routeShardedPhaseThreeWork(onePass, tuplePayload, enriched, stageIndex + 1, uid, resultId, baseKey,
				expectedWorkers, alias, collector);
	}

	private boolean isOnePassPhaseThreeStateTransfer(Datapoint node) {
		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		JsonNode payload = node.getValues();
		if (!OnePassPhaseThreeEnrichmentBuffer.PROTOCOL.equals(textField(payload, "protocol", ""))) {
			return false;
		}

		String type = textField(payload, "type", "");
		return OnePassPhaseThreeEnrichmentBuffer.TYPE_ENRICH_BATCH.equals(type) ||
				OnePassPhaseThreeEnrichmentBuffer.TYPE_ENRICH_SOURCE_DONE.equals(type);
	}

	private void handleOnePassPhaseThreeStateTransfer(Datapoint node, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		JsonNode payload = node.getValues();
		int uid = intField(payload, "uid", -1);
		String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));
		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);
		if (onePass == null) {
			throw new IllegalStateException("Phase-3 state message reached worker without OnePass synopsis. " +
					"uid=" + uid + ", worker=" + pId);
		}

		if (!onePass.isShardedPhaseThreeActive() || !alias.equals(onePass.getShardedPhaseThreeActiveAlias())) {

            List<JsonNode> pending = pendingOnePassPhaseThreeStateByUid.computeIfAbsent(uid, k -> new ArrayList<JsonNode>());
            pending.add(payload.deepCopy());

			System.out.println("[OnePass PHASE3 STATE DEFERRED] uid=" + uid
							+ ", alias=" + alias
							+ ", type=" + textField(payload, "type", "")
							+ ", worker=" + pId
							+ ", pending=" + pending.size());
			return;
		}

		handleOnePassPhaseThreeStateTransferPayload(payload, onePass, collector);
	}

	private void drainPendingOnePassPhaseThreeState(int uid, OnePassSamplerSdeSynopsis onePass, Collector<Estimation> collector) {

		List<JsonNode> pending = pendingOnePassPhaseThreeStateByUid.get(uid);
		if (pending == null || pending.isEmpty()) {
			return;
		}

		String activeAlias = onePass.getShardedPhaseThreeActiveAlias();
		List<JsonNode> remaining = new ArrayList<JsonNode>();

		for (JsonNode payload : pending) {
			String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));

			if (activeAlias != null && activeAlias.equals(alias)) {
				handleOnePassPhaseThreeStateTransferPayload(payload, onePass, collector);
			} else {
				remaining.add(payload);
			}
		}

		if (remaining.isEmpty()) {
			pendingOnePassPhaseThreeStateByUid.remove(uid);
		} else {
			pendingOnePassPhaseThreeStateByUid.put(uid, remaining);
		}
	}

	private void handleOnePassPhaseThreeStateTransferPayload(JsonNode payload, OnePassSamplerSdeSynopsis onePass, Collector<Estimation> collector) {

		int uid = intField(payload, "uid", -1);
		String alias = textField(payload, "phaseThreeAlias", textField(payload, "alias", ""));
		String resultId = textField(payload, "resultId", shardedPhaseThreeResultId(uid, alias));
		int stageIndex = intField(payload, "stageIndex", -1);
		int sourceWorker = intField(payload, "sourceWorker", -1);
		int targetWorker = intField(payload, "targetWorker", -1);
		int expectedWorkers = intField(payload, "expectedWorkers", 0);
		String type = textField(payload, "type", "");

		if (!shardedPhaseThreeResultId(uid, alias).equals(resultId)) {
			throw new IllegalStateException("Phase-3 state resultId mismatch. alias=" + alias + ", resultId=" + resultId);
		}
		if (targetWorker != pId) {
			throw new IllegalStateException("Phase-3 state message reached wrong worker. target=" + targetWorker + ", actual=" + pId);
		}
		if (!alias.equals(onePass.getShardedPhaseThreeActiveAlias())) {
			throw new IllegalStateException("Phase-3 state alias is not active. active=" +
					onePass.getShardedPhaseThreeActiveAlias() + ", received=" + alias);
		}

		if (OnePassPhaseThreeEnrichmentBuffer.TYPE_ENRICH_BATCH.equals(type)) {
			int sequence = intField(payload, "sequence", -1);
			boolean firstDelivery = onePassPhaseThreeEnrichmentCompletionTracker.acceptBatch(uid, resultId, alias,
					stageIndex, expectedWorkers, sourceWorker, sequence);

			if (!firstDelivery) {
				return;
			}

			JsonNode items = payload.get("items");
			if (items == null || !items.isArray()) {
				throw new IllegalStateException("PHASE3_ALIAS_ENRICH_BATCH has no items array: " + payload);
			}

			String baseKey = onePassBaseKeyByUid.get(uid);
			for (JsonNode item : items) {
				JsonNode tuplePayload = item.get("tuple");
				if (tuplePayload == null || tuplePayload.isNull()) {
					throw new IllegalStateException("Phase-3 enrichment item has no tuple: " + item);
				}

				double partialWeight = doubleField(item, "partialWeight", 0.0d);

				processShardedPhaseThreeWork(onePass, tuplePayload, partialWeight, stageIndex, uid, resultId,
						baseKey, expectedWorkers, alias, collector);
			}

			maybeAdvanceShardedPhaseThreeStage(onePass, uid, resultId, alias, stageIndex, expectedWorkers, collector);
			return;
		}

		if (OnePassPhaseThreeEnrichmentBuffer.TYPE_ENRICH_SOURCE_DONE.equals(type)) {
			int lastSequence = intField(payload, "lastSequence", -1);
			onePassPhaseThreeEnrichmentCompletionTracker.acceptSourceDone(uid, resultId, alias, stageIndex,
					expectedWorkers, sourceWorker, lastSequence);

			maybeAdvanceShardedPhaseThreeStage(onePass, uid, resultId, alias, stageIndex, expectedWorkers, collector);
			return;
		}

		throw new IllegalStateException("Unknown sharded Phase-3 StateTopic type: " + type);
	}

	private void handleShardedPhaseThreeEndAlias(OnePassSamplerSdeSynopsis onePass, int uid, String alias,
												 String resultId, int expectedWorkers, Collector<Estimation> collector) {

		if (!onePass.isShardedPhaseThreeActive() || !alias.equals(onePass.getShardedPhaseThreeActiveAlias())) {
			throw new IllegalStateException("Cannot finish inactive sharded Phase-3 alias " + alias);
		}

		onePassTupleBufferGate.sealAlias(uid, alias);

		int childCount = onePass.getPlan().getChildEdges(alias).size();
		int firstClosableStage = childCount == 0 ? 0 : 1;

		flushShardedPhaseThreeStageAndDeclareDone(uid, resultId, onePassBaseKeyByUid.get(uid), expectedWorkers, alias,
				firstClosableStage, collector);

		maybeAdvanceShardedPhaseThreeStage(onePass, uid, resultId, alias, firstClosableStage, expectedWorkers, collector);
	}

	private void flushShardedPhaseThreeStageAndDeclareDone(int uid, String resultId, String baseKey, int expectedWorkers,
														   String alias, int stageIndex, Collector<Estimation> collector) {

		for (Estimation batch : onePassPhaseThreeEnrichmentBuffer.flushStage(uid, resultId, alias, stageIndex)) {
			collector.collect(batch);
		}

		for (Estimation done : onePassPhaseThreeEnrichmentBuffer.buildStageDoneMessages(uid, resultId, baseKey,
				expectedWorkers, pId, alias, stageIndex)) {
			collector.collect(done);
		}

		onePassPhaseThreeEnrichmentCompletionTracker.acceptLocalSourceDone(uid, resultId, alias, stageIndex, expectedWorkers, pId);
	}

	private void maybeAdvanceShardedPhaseThreeStage(OnePassSamplerSdeSynopsis onePass, int uid, String resultId,
													String alias, int stageIndex, int expectedWorkers,
													Collector<Estimation> collector) {

		boolean complete = onePassPhaseThreeEnrichmentCompletionTracker.markCompleteIfReady(uid, resultId, alias, stageIndex);
		if (!complete) {
			return;
		}

		int childCount = onePass.getPlan().getChildEdges(alias).size();
		if (stageIndex < 0 || stageIndex > childCount) {
			throw new IllegalStateException("Completed invalid Phase-3 stageIndex=" + stageIndex + ", alias=" +
					alias + ", childCount=" + childCount);
		}

		if (stageIndex < childCount) {
			int nextStage = stageIndex + 1;

			// Every input to this worker at stageIndex is now known complete,
			// therefore every output this worker can generate for nextStage
			// has also been generated.
			flushShardedPhaseThreeStageAndDeclareDone(uid, resultId, onePassBaseKeyByUid.get(uid), expectedWorkers,
					alias, nextStage, collector);
			maybeAdvanceShardedPhaseThreeStage(onePass, uid, resultId, alias, nextStage, expectedWorkers, collector);
			return;
		}

		// stageIndex == childCount is the final parent-edge selection stage.
		emitLocalShardedPhaseThreeSelections(onePass, uid, resultId, alias, expectedWorkers, collector);
	}

	private void emitLocalShardedPhaseThreeSelections(OnePassSamplerSdeSynopsis onePass, int uid, String resultId,
													  String alias, int expectedWorkers, Collector<Estimation> collector) {

		String dedupeKey = uid + "|" + resultId + "|" + alias + "|worker=" + pId;
		if (!emittedOnePassPhaseThreeLocalSelections.add(dedupeKey)) {
			return;
		}

		List<String> order = phaseThreeAliasOrder(onePass.getPlan());
		int aliasIndex = order.indexOf(alias);
		if (aliasIndex < 0) {
			throw new IllegalStateException("Active Phase-3 alias is absent from root-to-leaf order: " + alias);
		}

		boolean isLastAlias = aliasIndex == order.size() - 1;
		String nextAlias = isLastAlias ? "" : order.get(aliasIndex + 1);

		int actualParallelism;
		try {
			actualParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
		} catch (Exception ignored) {
			actualParallelism = expectedWorkers;
		}

		Estimation local = onePass.buildLocalShardedPhaseThreeAliasSelectionsEstimation(onePassBaseKeyByUid.get(uid),
				uid, pId, expectedWorkers, actualParallelism, resultId, alias, aliasIndex, isLastAlias, nextAlias);

		collector.collect(local);

		System.out.println("[OnePass LOCAL_PHASE3_ALIAS_SELECTIONS] uid=" + uid + ", worker=" + pId + ", alias=" + alias +
						", resultId=" + resultId + ", aliasIndex=" + aliasIndex + ", isLastAlias=" + isLastAlias);
	}

	private boolean isOnePassPhaseThreeAliasSelectionsChunk(Datapoint node) {
		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		JsonNode payload = node.getValues();
		return "GLOBAL_STATE_CHUNK".equals(textField(payload, "type", "")) &&
				ONEPASS_STATE_TYPE_PHASE3_ALIAS_SELECTIONS.equals(textField(payload, "stateType", ""));
	}

	private void handleOnePassPhaseThreeAliasSelectionsChunk(Datapoint node, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		JsonNode chunk = node.getValues();
		String stateRef = textField(chunk, "stateRef", "");
		int chunkId = intField(chunk, "chunkId", -1);
		int chunkCount = intField(chunk, "chunkCount", -1);
		int workerId = intField(chunk, "workerId", -1);

		if (stateRef.isEmpty()) {
			throw new IllegalStateException("Phase-3 selection chunk has no stateRef: " + chunk);
		}
		if (workerId != pId) {
			throw new IllegalStateException("Phase-3 selection chunk reached wrong worker. target=" +
					workerId + ", actual=" + pId);
		}
		if (chunkId < 0 || chunkCount <= 0 || chunkId >= chunkCount) {
			throw new IllegalStateException("Invalid Phase-3 selection chunk metadata: " + chunk);
		}
		if (installedOnePassPhaseThreeStateRefs.contains(stateRef)) {
			return;
		}

        Map<Integer, JsonNode> chunks = onePassStateChunksByRef.
				computeIfAbsent(stateRef, k -> new HashMap<Integer, JsonNode>());

        for (JsonNode existingChunk : chunks.values()) {
			if (intField(existingChunk, "chunkCount", -1) != chunkCount) {
				throw new IllegalStateException("Conflicting Phase-3 chunkCount for stateRef=" + stateRef);
			}
			break;
		}

		JsonNode existing = chunks.get(chunkId);
		if (existing != null) {
			if (!existing.equals(chunk)) {
				throw new IllegalStateException("Conflicting duplicate Phase-3 chunk. stateRef=" +
						stateRef + ", chunkId=" + chunkId);
			}
			return;
		}

		chunks.put(chunkId, chunk.deepCopy());
		if (chunks.size() < chunkCount) {
			return;
		}

		JsonNode assembled = assembleOnePassPhaseThreeAliasSelections(stateRef, chunks, chunkCount);

		// Drop transport chunks before the lifecycle installs the bounded state.
		onePassStateChunksByRef.remove(stateRef);
		installCompletedOnePassPhaseThreeAliasSelections(node.getKey(), assembled, synopses, collector);
	}

	private JsonNode assembleOnePassPhaseThreeAliasSelections(String stateRef, Map<Integer, JsonNode> chunks, int chunkCount) {

		JsonNode first = chunks.get(0);
		if (first == null) {
			throw new IllegalStateException("Missing Phase-3 chunk 0 for stateRef=" + stateRef);
		}

		ObjectNode assembled = MAPPER.createObjectNode();
		assembled.put("type", ONEPASS_STATE_TYPE_PHASE3_ALIAS_SELECTIONS);
		assembled.put("stateType", ONEPASS_STATE_TYPE_PHASE3_ALIAS_SELECTIONS);
		assembled.put("stateRef", stateRef);

		copyIfPresent(first, assembled, "protocol");
		copyIfPresent(first, assembled, "uid");
		copyIfPresent(first, assembled, "synopsisID");
		copyIfPresent(first, assembled, "phase");
		copyIfPresent(first, assembled, "resultId");
		copyIfPresent(first, assembled, "queryName");
		copyIfPresent(first, assembled, "rootAlias");
		copyIfPresent(first, assembled, "baseKey");
		copyIfPresent(first, assembled, "workerId");
		copyIfPresent(first, assembled, "workerKey");
		copyIfPresent(first, assembled, "expectedWorkers");
		copyIfPresent(first, assembled, "alias");
		copyIfPresent(first, assembled, "phaseThreeAlias");
		copyIfPresent(first, assembled, "sampleSize");
		copyIfPresent(first, assembled, "selectionCount");
		copyIfPresent(first, assembled, "aliasIndex");
		copyIfPresent(first, assembled, "isLastAlias");
		copyIfPresent(first, assembled, "nextAlias");

		ArrayNode entries = MAPPER.createArrayNode();

		for (int id = 0; id < chunkCount; id++) {
			JsonNode chunk = chunks.get(id);
			if (chunk == null) {
				throw new IllegalStateException("Missing Phase-3 chunk " + id + " for stateRef=" + stateRef);
			}

			requireSameChunkText(first, chunk, "resultId", stateRef);
			requireSameChunkText(first, chunk, "alias", stateRef);
			requireSameChunkText(first, chunk, "baseKey", stateRef);
			requireSameChunkInt(first, chunk, "expectedWorkers", stateRef);
			requireSameChunkInt(first, chunk, "sampleSize", stateRef);
			requireSameChunkInt(first, chunk, "selectionCount", stateRef);
			requireSameChunkInt(first, chunk, "aliasIndex", stateRef);
			requireSameChunkText(first, chunk, "nextAlias", stateRef);
			requireSameChunkBoolean(first, chunk, "isLastAlias", stateRef);

			JsonNode chunkEntries = chunk.get("entries");
			if (chunkEntries == null || !chunkEntries.isArray()) {
				throw new IllegalStateException("Phase-3 chunk has no entries array. stateRef=" + stateRef + ", chunkId=" + id);
			}

			int declaredEntryCount = intField(chunk, "entryCount", -1);
			if (declaredEntryCount != chunkEntries.size()) {
				throw new IllegalStateException("Phase-3 chunk entryCount mismatch. stateRef=" + stateRef + ", chunkId=" + id);
			}

			for (JsonNode entry : chunkEntries) {
				entries.add(entry);
			}
		}

		int selectionCount = intField(first, "selectionCount", -1);
		int sampleSize = intField(first, "sampleSize", -1);
		if (selectionCount != sampleSize || entries.size() != selectionCount) {
			throw new IllegalStateException("Assembled Phase-3 selection count mismatch. stateRef=" + stateRef +
					", sampleSize=" + sampleSize + ", selectionCount=" + selectionCount + ", actual=" + entries.size());
		}

		assembled.set("entries", entries);
		return assembled;
	}

	private void requireSameChunkBoolean(JsonNode first, JsonNode current, String field, String stateRef) {

		boolean expected = first.has(field) && first.get(field).asBoolean(false);
		boolean actual = current.has(field) && current.get(field).asBoolean(false);

		if (expected != actual) {
			throw new IllegalStateException("Conflicting " + field + " across Phase-3 chunks. stateRef=" + stateRef);
		}
	}

	private void installCompletedOnePassPhaseThreeAliasSelections(String workerKey, JsonNode state,
																  ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		int uid = intField(state, "uid", -1);
		String stateRef = textField(state, "stateRef", "");
		String alias = textField(state, "phaseThreeAlias", textField(state, "alias", ""));

		if (uid < 0 || stateRef.isEmpty() || alias.isEmpty()) {
			throw new IllegalStateException("Invalid assembled Phase-3 state: " + state);
		}
		if (installedOnePassPhaseThreeStateRefs.contains(stateRef)) {
			return;
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);
		if (onePass == null) {
			throw new IllegalStateException("Phase-3 selections reached worker without OnePass synopsis. " +
					"uid=" + uid + ", worker=" + pId);
		}

		if (!onePass.isShardedPhaseThreeActive() || !alias.equals(onePass.getShardedPhaseThreeActiveAlias())) {
			throw new IllegalStateException(
					"Cannot install Phase-3 selections for inactive alias. active=" +
							onePass.getShardedPhaseThreeActiveAlias() + ", received=" + alias);
		}

		int expectedWorkers = intField(state, "expectedWorkers", getRuntimeContext().getNumberOfParallelSubtasks());
		int sampleSize = intField(state, "sampleSize", -1);
		int selectionCount = intField(state, "selectionCount", -1);
		int aliasIndex = intField(state, "aliasIndex", -1);
		boolean isLastAlias = state.has("isLastAlias") && state.get("isLastAlias").asBoolean(false);
		String nextAlias = textField(state, "nextAlias", "");
		String resultId = textField(state, "resultId", shardedPhaseThreeResultId(uid, alias));
		String baseKey = textField(state, "baseKey", OnePassShardOwnership.baseKeyFromWorkerKey(workerKey, expectedWorkers, pId));

		if (sampleSize <= 0 || selectionCount != sampleSize) {
			throw new IllegalStateException("Invalid Phase-3 global selection metadata. sampleSize="
							+ sampleSize + ", selectionCount=" + selectionCount);
		}

		List<String> order = phaseThreeAliasOrder(onePass.getPlan());
		if (aliasIndex < 0 || aliasIndex >= order.size() || !alias.equals(order.get(aliasIndex))) {
			throw new IllegalStateException("Installed Phase-3 traversal metadata mismatch. alias=" + alias +
					", aliasIndex=" + aliasIndex + ", order=" + order);
		}

		boolean expectedLast = aliasIndex == order.size() - 1;
		String expectedNext = expectedLast ? "" : order.get(aliasIndex + 1);
		if (isLastAlias != expectedLast || !expectedNext.equals(nextAlias)) {
			throw new IllegalStateException("Installed Phase-3 next-alias metadata mismatch. alias=" + alias +
					", isLastAlias=" + isLastAlias + ", nextAlias=" + nextAlias);
		}

		Map<String, Object> installSummary = onePass.installGlobalShardedPhaseThreeAliasSelections(state);

		if (isLastAlias && !onePass.isShardedPhaseThreeComplete()) {
			throw new IllegalStateException("Final Phase-3 alias installed but lifecycle is not complete");
		}
		if (!isLastAlias && onePass.isShardedPhaseThreeComplete()) {
			throw new IllegalStateException("Non-final Phase-3 alias unexpectedly completed lifecycle");
		}

		installedOnePassPhaseThreeStateRefs.add(stateRef);

		Map<String, Object> ready = new LinkedHashMap<String, Object>();
		ready.put("type", "LOCAL_PHASE3_ALIAS_SELECTIONS_INSTALLED");
		ready.put("protocol", "SHARDED_PHASE3_V1");
		ready.put("phase", "PHASE3");
		ready.put("uid", uid);
		ready.put("workerId", pId);
		ready.put("expectedWorkers", expectedWorkers);
		ready.put("resultId", resultId);
		ready.put("stateRef", stateRef);
		ready.put("baseKey", baseKey);
		ready.put("alias", alias);
		ready.put("phaseThreeAlias", alias);
		ready.put("sampleSize", sampleSize);
		ready.put("selectionCount", selectionCount);
		ready.put("aliasIndex", aliasIndex);
		ready.put("isLastAlias", isLastAlias);
		ready.put("nextAlias", nextAlias);
		ready.put("phaseThreeAliasCount", order.size());
		ready.put("phaseThreeComplete", isLastAlias);

		String json;
		try {
			json = MAPPER.writeValueAsString(ready);
		} catch (Exception e) {
			throw new IllegalStateException("Could not serialize LOCAL_PHASE3_ALIAS_SELECTIONS_INSTALLED", e);
		}

		String reduceKey = uid + "_PHASE3_INSTALLED_" + resultId;
		collector.collect(new Estimation(uid, reduceKey, 90, ONEPASS_SYNOPSIS_ID, reduceKey, json,
				new String[] {
						"LOCAL_PHASE3_ALIAS_SELECTIONS_INSTALLED",
						stateRef,
						resultId,
						alias,
						Integer.toString(pId),
						Integer.toString(expectedWorkers)
				},
				expectedWorkers));

		System.out.println("[OnePass PHASE3 ALIAS SELECTIONS INSTALLED] uid=" + uid
						+ ", worker=" + pId
						+ ", alias=" + alias
						+ ", stateRef=" + stateRef
						+ ", selectionCount=" + selectionCount
						+ ", phaseThreeComplete=" + isLastAlias
						+ ", lifecycle=" + installSummary.get("phase"));
	}

	private static List<String> phaseThreeAliasOrder(CompiledOnePassPlan plan) {
		List<String> out = new ArrayList<String>();
		for (String alias : plan.getRootToLeafOrder()) {
			if (!plan.isRoot(alias)) {
				out.add(alias);
			}
		}
		return out;
	}

	private static String shardedPhaseThreeResultId(int uid, String alias) {
		return "PHASE3_" + alias + "_" + uid;
	}
}
