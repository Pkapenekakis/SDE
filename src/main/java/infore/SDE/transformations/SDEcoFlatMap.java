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
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassShardOwnership;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;
import infore.SDE.transformations.onepass.debug.OnePassPhaseOneValidatorExporter;
import infore.SDE.transformations.onepass.worker.OnePassPhaseOneWorkerProtocol;
import infore.SDE.transformations.onepass.OnePassRequestParser;
import infore.SDE.transformations.onepass.worker.OnePassTupleBufferGate;
import lib.WDFT.controlBucket;
import lib.WLSH.Bucket;
import infore.SDE.synopses.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.messages.Datapoint;
import infore.SDE.transformations.onepass.worker.OnePassPhaseOneTransferBuffer;
import infore.SDE.transformations.onepass.worker.OnePassPhaseOneCompletionTracker;

public class SDEcoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Estimation> {

	private static final long serialVersionUID = 1L;
	private HashMap<String,ArrayList<Synopsis>> M_Synopses = new HashMap<>();
	private HashMap<String,ArrayList<ContinuousSynopsis>> MC_Synopses = new HashMap<>();

	private HashMap<String, Map<Integer, JsonNode>> onePassGlobalStateChunksByRef = new HashMap<String, Map<Integer, JsonNode>>();
	private HashMap<String, JsonNode> onePassGlobalStatesByRef = new HashMap<String, JsonNode>();
	private HashMap<String, Request> pendingOnePassInstallRequestsByRef = new HashMap<String, Request>();

	private final OnePassPhaseOneWorkerProtocol onePassPhaseOneWorkerProtocol = new OnePassPhaseOneWorkerProtocol();
	private static final int ONEPASS_MAX_BUFFERED_TUPLES_PER_UID =
			Integer.getInteger("sde.onepass.maxBufferedTuplesPerUid", 1000000);
	private final OnePassTupleBufferGate onePassTupleBufferGate =
			new OnePassTupleBufferGate(ONEPASS_MAX_BUFFERED_TUPLES_PER_UID);

	private final Map<String, Datapoint> pendingOnePassEndAliasByUidAlias = new HashMap<String, Datapoint>();

	private int pId;
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final String ONEPASS_DATA_BARRIER_FIELD = "__onePassDataBarrier";
	private static final int ONEPASS_DATA_BARRIER_REQUEST_ID = 70;
	private static final int ONEPASS_SYNOPSIS_ID = 30;
	private static final String ONEPASS_END_ALIAS_TYPE = "END_ALIAS";
	private final Set<String> processedOnePassEndAliasMarkers = new HashSet<String>();

	private final OnePassPhaseOneTransferBuffer onePassPhaseOneTransferBuffer =
			new OnePassPhaseOneTransferBuffer();

	private final OnePassPhaseOneCompletionTracker onePassPhaseOneCompletionTracker =
			new OnePassPhaseOneCompletionTracker();

	private final Map<Integer, Integer> onePassExpectedWorkersByUid =
			new HashMap<Integer, Integer>();

	private final Map<Integer, String> onePassBaseKeyByUid =
			new HashMap<Integer, String>();

	private final Map<Integer, Integer> onePassPhaseOneEpochByUid =
			new HashMap<Integer, Integer>();

	@Override
	public void flatMap1(Datapoint node, Collector<Estimation> collector) throws JsonProcessingException {
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(node.getKey());


		if (isOnePassPhaseOneStateTransfer(node)) {
			handleOnePassPhaseOneStateTransfer(node, Synopses, collector);
			return;
		}

		if (isOnePassGlobalStateChunk(node)) {
			handleOnePassGlobalStateChunk(node, Synopses, collector);
			return;
		}

		if (isOnePassEndAlias(node)) {

			handleOnePassEndAlias(node, Synopses, collector
			);

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


		/*
		 * OnePass explicit cleanup.
		 * Keep this isolated from generic synopsis removal because OnePass owns
		 * additional worker-local protocol/gating state outside M_Synopses.
		 */
		if (rq.getSynopsisID() == ONEPASS_SYNOPSIS_ID && rq.getRequestID() % 10 == 2) {

			handleOnePassRemove(rq, Synopses);

			return;
		}

		if (isOnePassShardedPhaseOneTransitionRequest(rq)) {
			handleOnePassShardedPhaseOneTransitionRequest(rq, Synopses, collector);
			return;
		}

		/*
		 * OnePass RequestTopic feedback protocol.
		 * This branch is strongly isolated from every other synopsis.
		 */
		if (isOnePassPhaseOneFeedbackRequest(rq)) {
			handleOnePassPhaseOneFeedbackRequest(rq, Synopses, collector);
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

				tryInstallReadyPhaseOneState(rq, Synopses, collector);

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

			if ("INSTALL_GLOBAL_INDEX".equalsIgnoreCase(command)) {
				handleInstallGlobalIndexRequest(rq, Synopses, collector);
				M_Synopses.put(rq.getKey(), Synopses);
				return;
			}

			if ("INSTALL_ROOT_SAMPLE".equalsIgnoreCase(command)) {
				handleInstallRootSampleRequest(rq, Synopses, collector);
				M_Synopses.put(rq.getKey(), Synopses);
				return;
			}

			if ("INSTALL_PHASE3_ALIAS_SELECTIONS".equalsIgnoreCase(command)) {
				handleInstallPhaseThreeAliasSelectionsRequest(rq, Synopses, collector);
				M_Synopses.put(rq.getKey(), Synopses);
				return;
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

	private void handleOnePassGlobalStateChunk(Datapoint node, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

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

		System.out.println("[OnePass GLOBAL_STATE_CHUNK] received stateRef=" + stateRef
				+ ", chunkId=" + chunkId + ", chunkCount=" + chunkCount + ", key=" + node.getKey()
				+ ", received=" + chunks.size() + "/" + chunkCount);

		if (chunks.size() >= chunkCount) {
			JsonNode assembled = assembleGlobalState(stateRef, chunks, chunkCount);

			onePassGlobalStatesByRef.put(stateRef, assembled);
			onePassGlobalStateChunksByRef.remove(stateRef);

			System.out.println("[OnePass GLOBAL_STATE_READY_LOCAL] stateRef=" + stateRef + ", key=" + node.getKey()
					+ ", entries=" + assembled.get("entries").size());

			Request pending = pendingOnePassInstallRequestsByRef.remove(stateRef);

			if (pending != null) {
				System.out.println("[OnePass INSTALL] pending request found after chunks completed. stateRef="
						+ stateRef);

				handlePendingOnePassInstallRequest(pending, synopses, collector);
			}
		}
	}

	private void handlePendingOnePassInstallRequest(Request pending, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		String command = firstParam(pending);

		if ("INSTALL_GLOBAL_INDEX".equalsIgnoreCase(command)) {
			handleInstallGlobalIndexRequest(pending, synopses, collector);
			return;
		}

		if ("INSTALL_ROOT_SAMPLE".equalsIgnoreCase(command)) {
			handleInstallRootSampleRequest(pending, synopses, collector);
			return;
		}

		if ("INSTALL_PHASE3_ALIAS_SELECTIONS".equalsIgnoreCase(command)) {
			handleInstallPhaseThreeAliasSelectionsRequest(pending, synopses, collector);
			return;
		}

		System.out.println("[OnePass INSTALL] Unknown pending install command=" + command + ", request=" + pending);
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

	private void handleInstallRootSampleRequest(
			Request rq,
			ArrayList<Synopsis> synopses,
			Collector<Estimation> collector) {

		String stateRef = resolveInstallStateRef(rq);

		if (stateRef == null || stateRef.trim().isEmpty()) {
			System.out.println("[OnePass INSTALL_ROOT_SAMPLE] Missing stateRef. Request=" + rq);
			return;
		}

		JsonNode state = onePassGlobalStatesByRef.get(stateRef);

		if (state == null || state.isNull()) {
			pendingOnePassInstallRequestsByRef.put(stateRef, rq);

			System.out.println("[OnePass INSTALL_ROOT_SAMPLE] State not available yet. Pending install stored. stateRef="
					+ stateRef + ", key=" + rq.getKey());
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
				onePass.installGlobalPhaseTwoRootSample(state);
				installed = true;
			} catch (Exception ex) {
				error = ex.getMessage();
				ex.printStackTrace();
			}
		}

		int expectedWorkers = rq.getNoOfP() > 0
				? rq.getNoOfP()
				: getRuntimeContext().getNumberOfParallelSubtasks();

		String ackJson = buildInstallRootSampleAckJson(
				rq,
				state,
				stateRef,
				pId,
				expectedWorkers,
				installed,
				error
		);

		String[] param = new String[] {
				"INSTALL_ROOT_SAMPLE_ACK",
				stateRef,
				"PHASE2",
				textField(state, "rootAlias", ""),
				Integer.toString(pId),
				Integer.toString(expectedWorkers)
		};

		String estimationKey = rq.getUID() + "_INSTALL_ROOT_SAMPLE_" + stateRef + "_" + pId;

		Estimation ack = new Estimation(
				rq.getUID(),
				estimationKey,
				85,
				30,
				rq.getKey(),
				ackJson,
				param,
				expectedWorkers
		);

		collector.collect(ack);

		System.out.println("[OnePass INSTALL_ROOT_SAMPLE] ACK emitted uid="
				+ rq.getUID()
				+ ", workerId=" + pId
				+ ", stateRef=" + stateRef
				+ ", installed=" + installed
				+ ", key=" + rq.getKey());
	}

	private String buildInstallRootSampleAckJson(
			Request rq,
			JsonNode state,
			String stateRef,
			int workerId,
			int expectedWorkers,
			boolean installed,
			String error) {

		Map<String, Object> ack = new LinkedHashMap<String, Object>();

		ack.put("type", "INSTALL_ROOT_SAMPLE_ACK");
		ack.put("uid", rq.getUID());
		ack.put("stateRef", stateRef);
		ack.put("phase", "PHASE2");
		ack.put("resultId", textField(state, "resultId", ""));
		ack.put("rootAlias", textField(state, "rootAlias", ""));
		ack.put("workerId", workerId);
		ack.put("expectedWorkers", expectedWorkers);
		ack.put("installed", installed);
		ack.put("sampleSize", intField(state, "sampleSize", 0));
		ack.put("sampleInstanceCount", intField(state, "sampleInstanceCount", 0));
		ack.put("rootTuplesSeen", longField(state, "rootTuplesSeen", 0L));
		ack.put("positiveRootCandidatesSeen", longField(state, "positiveRootCandidatesSeen", 0L));
		ack.put("totalRootGroupWeight", doubleField(state, "totalRootGroupWeight", 0.0d));

		if (error != null && !error.trim().isEmpty()) {
			ack.put("error", error);
		}

		try {
			return MAPPER.writeValueAsString(ack);
		} catch (Exception e) {
			throw new IllegalStateException("Could not serialize INSTALL_ROOT_SAMPLE_ACK", e);
		}
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

	private void handleInstallPhaseThreeAliasSelectionsRequest(
			Request rq,
			ArrayList<Synopsis> synopses,
			Collector<Estimation> collector) {

		String stateRef = resolveInstallStateRef(rq);

		if (stateRef == null || stateRef.trim().isEmpty()) {
			System.out.println("[OnePass INSTALL_PHASE3_ALIAS_SELECTIONS] Missing stateRef. Request=" + rq);
			return;
		}

		JsonNode state = onePassGlobalStatesByRef.get(stateRef);

		if (state == null || state.isNull()) {
			pendingOnePassInstallRequestsByRef.put(stateRef, rq);

			System.out.println("[OnePass INSTALL_PHASE3_ALIAS_SELECTIONS] State not available yet. Pending install stored. stateRef="
					+ stateRef
					+ ", key=" + rq.getKey());

			return;
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsis(rq, synopses);

		boolean installed = false;
		String error = "";
		Map<String, Object> installSummary = new LinkedHashMap<String, Object>();

		if (onePass == null) {
			error = "No OnePassSamplerSdeSynopsis found for uid=" + rq.getUID()
					+ ", key=" + rq.getKey();
		} else {
			try {
				installSummary = onePass.installGlobalPhaseThreeAliasSelections(state);
				installed = true;
			} catch (Exception ex) {
				error = ex.getMessage();
				ex.printStackTrace();
			}
		}

		int expectedWorkers = rq.getNoOfP() > 0
				? rq.getNoOfP()
				: getRuntimeContext().getNumberOfParallelSubtasks();

		String phaseThreeAlias = textField(
				state,
				"phaseThreeAlias",
				textField(state, "alias", "")
		);

		String stateChecksum = computeGenericStateChecksum(state);

		String ackJson = buildInstallPhaseThreeAliasSelectionsAckJson(
				rq,
				state,
				stateRef,
				phaseThreeAlias,
				pId,
				expectedWorkers,
				installed,
				error,
				stateChecksum,
				installSummary
		);

		String[] param = new String[] {
				"INSTALL_PHASE3_ALIAS_SELECTIONS_ACK",
				stateRef,
				"PHASE3",
				phaseThreeAlias,
				Integer.toString(pId),
				Integer.toString(expectedWorkers)
		};

		String estimationKey = rq.getUID()
				+ "_INSTALL_PHASE3_ALIAS_SELECTIONS_"
				+ stateRef
				+ "_"
				+ pId;

		Estimation ack = new Estimation(
				rq.getUID(),
				estimationKey,
				95,
				30,
				rq.getKey(),
				ackJson,
				param,
				expectedWorkers
		);

		collector.collect(ack);

		System.out.println("[OnePass INSTALL_PHASE3_ALIAS_SELECTIONS] ACK emitted uid="
				+ rq.getUID()
				+ ", workerId=" + pId
				+ ", stateRef=" + stateRef
				+ ", alias=" + phaseThreeAlias
				+ ", installed=" + installed
				+ ", checksum=" + stateChecksum
				+ ", key=" + rq.getKey());
	}

	private String buildInstallPhaseThreeAliasSelectionsAckJson(
			Request rq,
			JsonNode state,
			String stateRef,
			String phaseThreeAlias,
			int workerId,
			int expectedWorkers,
			boolean installed,
			String error,
			String stateChecksum,
			Map<String, Object> installSummary) {

		Map<String, Object> ack = new LinkedHashMap<String, Object>();

		ack.put("type", "INSTALL_PHASE3_ALIAS_SELECTIONS_ACK");
		ack.put("uid", rq.getUID());
		ack.put("stateRef", stateRef);
		ack.put("phase", "PHASE3");
		ack.put("resultId", textField(state, "resultId", ""));
		ack.put("rootAlias", textField(state, "rootAlias", ""));
		ack.put("phaseThreeAlias", phaseThreeAlias == null ? "" : phaseThreeAlias);
		ack.put("alias", phaseThreeAlias == null ? "" : phaseThreeAlias);
		ack.put("workerId", workerId);
		ack.put("expectedWorkers", expectedWorkers);
		ack.put("installed", installed);

		JsonNode entries = state.get("entries");
		ack.put("entryCount", entries != null && entries.isArray() ? entries.size() : 0);
		ack.put("stateChecksum", stateChecksum == null ? "" : stateChecksum);

		if (installSummary != null && !installSummary.isEmpty()) {
			ack.put("installSummary", installSummary);
		}

		if (error != null && !error.trim().isEmpty()) {
			ack.put("error", error);
		}

		try {
			return MAPPER.writeValueAsString(ack);
		} catch (Exception e) {
			throw new IllegalStateException(
					"Could not serialize INSTALL_PHASE3_ALIAS_SELECTIONS_ACK",
					e
			);
		}
	}

	private String computeGenericStateChecksum(JsonNode state) {
		try {
			JsonNode entries = state.get("entries");

			if (entries == null || !entries.isArray()) {
				return "EMPTY";
			}

			List<String> parts = new ArrayList<String>();

			for (JsonNode entry : entries) {
				parts.add(MAPPER.writeValueAsString(entry));
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

	private boolean isOnePassPhaseOneFeedbackRequest(Request request) {

		if (request == null) {
			return false;
		}

		if (request.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
			return false;
		}

		if (request.getRequestID() != 7) {
			return false;
		}

		JsonNode payload = request.getParameters();

		if (payload == null || payload.isNull() || !payload.isObject()) {
			return false;
		}

		String type = textField(payload, "type", "");

		return OnePassPhaseOneWorkerProtocol.TYPE_GLOBAL_STATE_BEGIN.equals(type) ||
				OnePassPhaseOneWorkerProtocol.TYPE_GLOBAL_STATE_CHUNK.equals(type) ||
				OnePassPhaseOneWorkerProtocol.TYPE_GLOBAL_STATE_COMMIT.equals(type) ||
				OnePassPhaseOneWorkerProtocol.COMMAND_START_NEXT_ALIAS.equals(type) ||
				OnePassPhaseOneWorkerProtocol.COMMAND_START_PHASE_2.equals(type);
	}

	private void handleOnePassPhaseOneFeedbackRequest(Request request, ArrayList<Synopsis> synopses,
													  Collector<Estimation> collector) {

		JsonNode payload = request.getParameters();
		String type = textField(payload, "type", "");

		if (OnePassPhaseOneWorkerProtocol.TYPE_GLOBAL_STATE_BEGIN.equals(type) ||
				OnePassPhaseOneWorkerProtocol.TYPE_GLOBAL_STATE_CHUNK.equals(type) ||
				OnePassPhaseOneWorkerProtocol.TYPE_GLOBAL_STATE_COMMIT.equals(type)) {

			JsonNode completeState = onePassPhaseOneWorkerProtocol.acceptStateMessage(payload);

			if (completeState != null) {
				installReadyPhaseOneState(request.getUID(), completeState, synopses, collector);
			}

			return;
		}

		if (OnePassPhaseOneWorkerProtocol.COMMAND_START_NEXT_ALIAS.equals(type) ||
				OnePassPhaseOneWorkerProtocol.COMMAND_START_PHASE_2.equals(type)) {

			boolean activated = onePassPhaseOneWorkerProtocol.acceptTransition(payload);

			if (activated) {
				applyActiveOnePassTransitionIfAvailable(request.getUID(), synopses, collector);
			}

			System.out.println("[OnePass TRANSITION] uid=" + request.getUID() + ", command=" + type
							+ ", nextAlias=" + textField(payload, "nextAlias", "")
							+ ", requiredStateRef=" + textField(payload, "requiredStateRef", "")
							+ ", activated=" + activated + ", workerId=" + pId);
		}
	}

	private void tryInstallReadyPhaseOneState(Request request, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		if (request == null) {
			return;
		}

		JsonNode readyState = onePassPhaseOneWorkerProtocol.getReadyStateForUid(request.getUID());

		if (readyState == null) {
			return;
		}

		installReadyPhaseOneState(request.getUID(), readyState, synopses, collector);
	}

	private void installReadyPhaseOneState(int uid, JsonNode state, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		if (state == null || state.isNull()) {
			return;
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

		if (onePass == null) {
			/*
			 * The protocol object retains the ready state.
			 * It will be retried when the ADD request creates the synopsis.
			 */
			System.out.println("[OnePass GLOBAL_STATE_READY] " + "No local synopsis yet. "
					+ "State retained for uid=" + uid + ", stateRef=" +
					textField(state, "stateRef", "") + ", workerId=" + pId);

			return;
		}

		String stateRef = textField(state, "stateRef", "");

		String activeAlias = textField(state, "activeAlias", "");

		if (stateRef == null || stateRef.trim().isEmpty()) {

			throw new IllegalStateException("Assembled Phase 1 state has no stateRef. uid=" + uid);
		}

		if (onePassPhaseOneWorkerProtocol.isStateInstalled(uid, stateRef)) {
			return;
		}

		onePass.installGlobalPhaseOneIndex(state, activeAlias);

		onePassPhaseOneWorkerProtocol.markInstalled(uid, stateRef);
		applyActiveOnePassTransitionIfAvailable(uid, synopses, collector);

		OnePassPhaseOneWorkerProtocol.Transition activeTransition = onePassPhaseOneWorkerProtocol.getActiveTransition(uid);

		System.out.println("[OnePass GLOBAL_PHASE1_INDEX_INSTALLED_LOCAL] " + "uid=" + uid
						+ ", stateRef=" + stateRef + ", activeAlias=" + activeAlias + ", workerId=" + pId
						+ ", activatedTransition=" + activeTransition
		);

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

	private void handleOnePassDataTuple(
			OnePassSamplerSdeSynopsis onePass,
			JsonNode payload,
			Collector<Estimation> collector) {

		if (onePass == null) {
			return;
		}

		int uid = onePass.getSynopsisID();

		// Keep current Phase 3 behavior until Phase 3 migration.
		if (onePass.getLifecycle().getPhase() == OnePassSamplerSynopsis.Phase.PHASE_3) {
			onePass.add(payload);
			return;
		}

		if (!onePassTupleBufferGate.isRegistered(uid)) {
			registerOnePassTupleGate(onePass);
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(payload);
		String alias = tuple.getTable();

		if (!onePassTupleBufferGate.isAllowed(uid, alias)) {
			onePassTupleBufferGate.buffer(uid, alias, payload);
			return;
		}

		int expectedWorkers = onePassExpectedWorkersByUid.containsKey(uid)
				? onePassExpectedWorkersByUid.get(uid)
				: 1;

		if (onePass.getLifecycle().getPhase() == OnePassSamplerSynopsis.Phase.PHASE_1
				&& expectedWorkers > 1) {

			processShardedPhaseOneTuple(onePass, payload, collector);
			return;
		}

		// Single-worker / not-yet-migrated phases preserve the existing path.
		onePass.add(payload);
	}

	private void processShardedPhaseOneTuple(OnePassSamplerSdeSynopsis onePass, JsonNode payload, Collector<Estimation> collector) {

		int uid = onePass.getSynopsisID();
		int expectedWorkers = onePassExpectedWorkersByUid.get(uid);
		int epoch = onePassPhaseOneEpochByUid.get(uid);
		String baseKey = onePassBaseKeyByUid.get(uid);

		OnePassPhaseOneContribution contribution = onePass.computePhaseOneContribution(payload);

		if (contribution == null || contribution.getDelta() == 0.0d) {
			return;
		}

		int targetWorker = OnePassShardOwnership.ownerForEdgeKey(
				contribution.getEdgeId(),
				contribution.getJoinKey(),
				expectedWorkers
		);

		if (targetWorker == pId) {
			// Local fast path: no Kafka round-trip.
			onePass.applyPhaseOneContribution(
					contribution.getEdgeId(),
					contribution.getJoinKey(),
					contribution.getDelta()
			);
			return;
		}

		OnePassTuple tuple = OnePassTupleExtractor.extract(payload);

		for (Estimation stateMessage : onePassPhaseOneTransferBuffer.addRemoteContribution(uid, baseKey,
				expectedWorkers, pId, targetWorker, epoch, tuple.getTable(), contribution)) {
			collector.collect(stateMessage);
		}
	}

	private void applyActiveOnePassTransitionIfAvailable(int uid, ArrayList<Synopsis> synopses,
														 Collector<Estimation> collector) {

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

		if (onePass == null) {
			return;
		}

		OnePassPhaseOneWorkerProtocol.Transition transition =onePassPhaseOneWorkerProtocol.consumeActiveTransition(uid);

		if (transition == null) {
			return;
		}

		String nextAlias = transition.getNextAlias();

		List<JsonNode> released = onePassTupleBufferGate.activateAliasAndDrain(uid, nextAlias);

		System.out.println("[OnePass GATE ACTIVATED] uid=" + uid
						+ ", command=" + transition.getCommand() + ", nextAlias=" + nextAlias
						+ ", requiredStateRef=" + transition.getRequiredStateRef() + ", released=" + released.size()
						+ ", bufferedRemaining=" + onePassTupleBufferGate.getBufferedCount(uid)
						+ ", lifecyclePhase=" + onePass.getLifecycle().getPhase().name()
						+ ", workerId=" + pId);

		for (JsonNode bufferedPayload : released) {

			OnePassTuple tuple = OnePassTupleExtractor.extract(bufferedPayload);

			if (!nextAlias.equals(tuple.getTable())) {

				throw new IllegalStateException("OnePass tuple gate released wrong alias. " + "uid=" +
						uid + ", expected=" + nextAlias + ", actual=" + tuple.getTable());
			}

			onePass.add(bufferedPayload);
		}
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

		if (uid < 0) {
			throw new IllegalStateException("END_ALIAS is missing a valid uid: " + values);
		}

		if (!"PHASE1".equalsIgnoreCase(phase)) {
			throw new IllegalStateException("Step-6 END_ALIAS currently supports PHASE1 only. " + "Received phase=" +
					phase + ", payload=" + values);
		}

		if (alias == null || alias.trim().isEmpty()) {
			throw new IllegalStateException("END_ALIAS is missing alias: " + values);
		}

		if (resultId == null || resultId.trim().isEmpty()) {
			throw new IllegalStateException("END_ALIAS is missing resultId: " + values);
		}

		if (!OnePassPhaseOneWorkerProtocol.COMMAND_START_NEXT_ALIAS.equals(nextCommand)
				&& !OnePassPhaseOneWorkerProtocol.COMMAND_START_PHASE_2.equals(nextCommand)) {

			throw new IllegalStateException("END_ALIAS has invalid nextCommand="
							+ nextCommand + ". Expected START_NEXT_ALIAS or START_PHASE_2. " + "payload=" + values);
		}

		if (nextAlias == null || nextAlias.trim().isEmpty()) {
			throw new IllegalStateException("END_ALIAS is missing nextAlias: " + values);
		}

		String markerKey = uid + "|" + phase + "|" + alias + "|" + resultId;

		/*
		 * Idempotence for duplicate/retried END_ALIAS delivery.
		 */
		if (processedOnePassEndAliasMarkers.contains(markerKey)) {

			System.out.println("[OnePass END_ALIAS] Duplicate ignored. markerKey=" + markerKey + ", workerId=" + pId);
			return;
		}

		OnePassSamplerSdeSynopsis onePass = findOnePassSynopsisByUid(uid, synopses);

		if (onePass == null) {
			throw new IllegalStateException("END_ALIAS reached worker without OnePass synopsis. " + "uid=" +
					uid + ", key=" + node.getKey() + ", workerId=" + pId);
		}

		String currentAlias = onePassTupleBufferGate.getAllowedAlias(uid);

		if (!alias.equals(currentAlias) || onePassTupleBufferGate.isSealed(uid, alias)) {

			String pendingKey = onePassEndAliasPendingKey(uid, alias);

			if (!pendingOnePassEndAliasByUidAlias.containsKey(pendingKey)) {
				JsonNode copy = node.getValues() == null ? null : node.getValues().deepCopy();
				pendingOnePassEndAliasByUidAlias.put(pendingKey, new Datapoint(node.getKey(), node.getStreamID(), copy));
				System.out.println("[OnePass END_ALIAS DEFERRED] uid=" + uid + ", alias=" + alias +
						", currentAlias=" + currentAlias + ", workerId=" + pId);
			}
			return;
		}

		/*
		 * Once END_ALIAS has been observed, no later tuple for this alias may
		 * mutate the state that is about to be exported.
		 */
		onePassTupleBufferGate.sealAlias(uid, alias);

		int actualParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
		int expectedWorkers = requestedExpectedWorkers > 0 ? requestedExpectedWorkers : actualParallelism;

		if (expectedWorkers > 1) {
			handleShardedPhaseOneEndAlias(node, onePass, uid, alias, resultId, nextCommand, nextAlias,
					expectedWorkers, collector);
			processedOnePassEndAliasMarkers.add(markerKey);
			return;
		}

		Estimation localPhaseOneResult = onePass.buildLocalPhaseOneResultEstimation(node.getKey(), uid, pId,
				expectedWorkers, actualParallelism, resultId, alias, nextCommand, nextAlias);

		collector.collect(localPhaseOneResult);
		processedOnePassEndAliasMarkers.add(markerKey);

		System.out.println("[OnePass END_ALIAS] LOCAL_PHASE1_RESULT emitted. " + "uid=" + uid + ", alias="
				+ alias + ", resultId=" + resultId + ", nextCommand=" + nextCommand + ", nextAlias="
				+ nextAlias + ", workerId=" + pId + ", expectedWorkers=" + expectedWorkers + ", key=" + node.getKey());

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

	private void processPendingOnePassEndAlias(int uid, String alias, ArrayList<Synopsis> synopses, Collector<Estimation> collector) {

		if (alias == null || alias.trim().isEmpty()) {
			return;
		}

		Datapoint pending = pendingOnePassEndAliasByUidAlias.get(onePassEndAliasPendingKey(uid, alias));

		if (pending == null) {
			return;
		}

		completeOnePassEndAlias(pending, synopses, collector);
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


		//2. Remove new asynchronous Phase 1 protocol state.
		onePassPhaseOneWorkerProtocol.clear(uid);

		//3. Remove buffered tuples / active alias / sealed aliases.
		onePassTupleBufferGate.clear(uid);

		//4. Remove deferred END_ALIAS.
		String pendingPrefix = uid + "|";
        pendingOnePassEndAliasByUidAlias.keySet().removeIf(key -> key.startsWith(pendingPrefix));

		//5. Remove END_ALIAS deduplication state.
        processedOnePassEndAliasMarkers.removeIf(key -> key.startsWith(pendingPrefix));

		onePassPhaseOneTransferBuffer.clearUid(uid);
		onePassPhaseOneCompletionTracker.clearUid(uid);
		onePassExpectedWorkersByUid.remove(uid);
		onePassBaseKeyByUid.remove(uid);
		onePassPhaseOneEpochByUid.remove(uid);

		System.out.println("[OnePass REMOVE] worker-local state cleared." + " uid=" + uid + ", workerId=" + pId +
				", key=" + request.getKey());
	}

	private boolean isOnePassPhaseOneStateTransfer(Datapoint node) {
		if (node == null || node.getValues() == null || node.getValues().isNull()) {
			return false;
		}

		String type = textField(node.getValues(), "type", "");
		String protocol = textField(node.getValues(), "protocol", "");

		return OnePassPhaseOneTransferBuffer.PROTOCOL.equals(protocol)
				&& (OnePassPhaseOneTransferBuffer.TYPE_SHARD_BATCH.equals(type)
				|| OnePassPhaseOneTransferBuffer.TYPE_SOURCE_DONE.equals(type));
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

		if (OnePassPhaseOneTransferBuffer.TYPE_SHARD_BATCH.equals(type)) {
			int sequence = intField(payload, "sequence", -1);

			boolean firstDelivery = onePassPhaseOneCompletionTracker.acceptBatch(
					uid, epoch, alias, expectedWorkers, sourceWorker, sequence
			);

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

				onePass.applyPhaseOneContribution(
						edgeId,
						new JoinValue(parts),
						delta
				);
			}
		}

		else if (OnePassPhaseOneTransferBuffer.TYPE_SOURCE_DONE.equals(type)) {
			int lastSequence = intField(payload, "lastSequence", -1);

			onePassPhaseOneCompletionTracker.acceptSourceDone(
					uid, epoch, alias, expectedWorkers, sourceWorker, lastSequence
			);
		}

		maybeEmitLocalPhaseOneShardReady(uid, epoch, alias, onePass, collector);
	}

	private void handleShardedPhaseOneEndAlias(
			Datapoint node,
			OnePassSamplerSdeSynopsis onePass,
			int uid,
			String alias,
			String resultId,
			String nextCommand,
			String nextAlias,
			int expectedWorkers,
			Collector<Estimation> collector) {

		int epoch = intField(node.getValues(), "epoch", -1);
		if (epoch <= 0) {
			throw new IllegalStateException("Sharded END_ALIAS requires epoch > 0: " + node.getValues());
		}

		Integer expectedEpoch = onePassPhaseOneEpochByUid.get(uid);
		if (expectedEpoch == null || expectedEpoch.intValue() != epoch) {
			throw new IllegalStateException("END_ALIAS epoch mismatch. uid=" + uid + ", expected=" + expectedEpoch +
					", received=" + epoch);
		}

		onePassTupleBufferGate.sealAlias(uid, alias);

		// 1) Every remaining remote contribution must be emitted before SOURCE_DONE.
		for (Estimation batch : onePassPhaseOneTransferBuffer.flushAlias(uid, epoch, alias)) {
			collector.collect(batch);
		}

		// 2) Tiny per-destination completion markers.
		for (Estimation done : onePassPhaseOneTransferBuffer.buildSourceDoneMessages(uid, onePassBaseKeyByUid.get(uid),
				expectedWorkers, pId, epoch, alias)) {
			collector.collect(done);
		}

		CompiledOnePassPlan.DirectedJoinEdge parentEdge =
				onePass.getPlan().getParentEdge(alias);
		String activeEdgeId = parentEdge == null ? "" : parentEdge.getEdgeId();

		Map<String, Object> summary = onePass.getLocalPhaseOneEdgeSummary(activeEdgeId);
		int localKeyCount = ((Number) summary.get("numberOfKeys")).intValue();
		double localTotalWeight = ((Number) summary.get("totalWeight")).doubleValue();
		long localSeenTuples = onePass.getLocalPhaseOneSeenTupleCount(alias);

		// 3) Local source is done without Kafka.
		onePassPhaseOneCompletionTracker.acceptLocalEndAlias(
				uid,
				epoch,
				alias,
				expectedWorkers,
				pId,
				resultId,
				nextCommand,
				nextAlias,
				onePassBaseKeyByUid.get(uid),
				activeEdgeId,
				localKeyCount,
				localTotalWeight,
				localSeenTuples
		);

		maybeEmitLocalPhaseOneShardReady(uid, epoch, alias, onePass, collector);
	}

	private void maybeEmitLocalPhaseOneShardReady(int uid, int epoch, String alias, OnePassSamplerSdeSynopsis onePass,
												  Collector<Estimation> collector) {

		OnePassPhaseOneCompletionTracker.ReadySnapshot ready =
				onePassPhaseOneCompletionTracker.readySnapshotIfComplete(uid, epoch, alias);

		if (ready == null) {
			return;
		}

		/*
		 * DEBUG ONLY.
		 *
		 * Dump only after the FINAL Phase-1 alias is complete.
		 *
		 * readySnapshotIfComplete() guarantees that this destination has received
		 * every declared SHARD_BATCH from every source before we inspect the state.

		if (OnePassPhaseOneValidatorExporter.isEnabled() && OnePassPhaseOneWorkerProtocol.COMMAND_START_PHASE_2
				.equals(ready.nextCommand)) {
			try {
				OnePassPhaseOneValidatorExporter.exportWorkerShard(onePass, uid, pId, ready.expectedWorkers);
			} catch (Exception exception) {
				throw new IllegalStateException("Could not write Phase-1 validator shard." + " uid=" + uid +
						", worker=" + pId, exception);
			}
		}
		*
		* */

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
		payload.put("localKeyCount", ready.localKeyCount);
		payload.put("localTotalWeight", ready.localTotalWeight);
		payload.put("localSeenTuples", ready.localSeenTuples);

		String json;
		try {
			json = MAPPER.writeValueAsString(payload);
		} catch (Exception e) {
			throw new IllegalStateException("Could not serialize LOCAL_PHASE1_SHARD_READY", e);
		}

		String reduceKey = uid + "_PHASE1_READY_" + ready.resultId;

		collector.collect(new Estimation(uid, reduceKey, 76, 30, reduceKey, json,
				new String[] {
						"LOCAL_PHASE1_SHARD_READY",
						ready.resultId,
						ready.alias,
						Integer.toString(ready.epoch),
						Integer.toString(pId),
						Integer.toString(ready.expectedWorkers)
				},
				ready.expectedWorkers
		));
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

			for (JsonNode buffered : released) {
				processShardedPhaseOneTuple(onePass, buffered, collector);
			}

			processPendingOnePassEndAlias(uid, nextAlias, synopses, collector);
			return;
		}

		if ("START_PHASE_2".equals(command)) {
			/*
			 * IMPORTANT: Phase 2 is intentionally not activated in this Phase-1
			 * patch. The current Phase 2 assumes every worker holds a complete
			 * replicated Phase-1 result, which is no longer true.
			 *
			 * Keep the previous alias sealed, record the epoch, and stop the
			 * Phase-1 benchmark here. Phase 2 will get its own sharded read path.
			 */
			onePassPhaseOneEpochByUid.put(uid, nextEpoch);
			System.out.println(
					"[OnePass SHARDED PHASE1 COMPLETE] uid=" + uid
							+ ", worker=" + pId
							+ ", nextAlias=" + nextAlias
							+ ". Phase 2 activation deferred until Phase-2 migration."
			);
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
}
