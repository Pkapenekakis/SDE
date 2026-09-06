package infore.SDE;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.kafkaStringConsumer;

import infore.SDE.sources.kafkaStringProducer;
import infore.SDE.transformations.*;
import infore.SDE.transformations.onepass.OnePassGlobalStateSplitter;
import infore.SDE.transformations.onepass.OnePassDataRouterCoFlatMap;
import infore.SDE.transformations.onepass.coordinator.OnePassCoordinatorOperator;
import infore.SDE.transformations.onepass.coordinator.OnePassWorkerPartitioner;
import infore.SDE.transformations.onepass.worker.OnePassPhaseOneEnrichmentBuffer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.transformations.onepass.OnePassStateTransferToJson;
import infore.SDE.transformations.onepass.OnePassPhaseOneTransitionMapper;

/**
 * <br>
 * Implementation code for SDE for INFORE-PROJECT" <br> *
 * ATHENA Research and Innovation Center <br> *
 * Author: Antonis_Kontaxakis <br> *
 * email: adokontax15@gmail.com *
 *
 * OnePass* adds:
 *   - OnePass-aware data routing
 *   - OnePass worker partitioning
 *   - OnePass coordinator control messages
 *
 * Heavy merge still happens in ReduceFlatMap.
 * Coordinator only handles readiness / control messages.
 */


public class Run {

	private static String kafkaDataInputTopic;
	private static String kafkaRequestInputTopic;
	private static String kafkaBrokersList;
	private static int parallelism;
	private static String kafkaOutputTopic;
	private static String kafkaOnePassStateTopic;
	private static OnePassDataRouterCoFlatMap.RoutingMode onePassRoutingMode =
			OnePassDataRouterCoFlatMap.RoutingMode.ROUND_ROBIN;

	/**
	 * @param args Program arguments. You have to provide 4 arguments otherwise
	 *             DEFAULT values will be used.<br>
	 *             <ol>
	 *             <li>args[0]={@link #kafkaDataInputTopic} DEFAULT: "Forex")
	 *             <li>args[1]={@link #kafkaRequestInputTopic} DEFAULT: "Requests")
	 *             <li>args[2]={@link #kafkaBrokersList} (DEFAULT: "localhost:9092")
	 *             <li>args[3]={@link #parallelism} Job parallelism (DEFAULT: "4")
	 *             <li>args[4]={@link #kafkaOutputTopic} DEFAULT: "OUT")
	 *             "O10")
	 *             </ol>
	 *
	 */

	public static void main(String[] args) throws Exception {
		// Initialize Input Parameters
		initializeParameters(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		//kafkaStringConsumer kc = new kafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic);
		kafkaStringConsumer kc = new kafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic, true);
		kafkaStringConsumer requests = new kafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic);
		kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);
		kafkaStringProducer onePassGlobalStateKp = new kafkaStringProducer(kafkaBrokersList, kafkaOnePassStateTopic);

		kafkaStringConsumer globalStateConsumer = new kafkaStringConsumer(kafkaBrokersList, kafkaOnePassStateTopic);
		/*
		 * Used only for coordinator feedback commands.
		 * requestID == 7 will be serialized as a Request by kafkaProducerEstimation.
		 */
		kafkaProducerEstimation pRequest = new kafkaProducerEstimation(kafkaBrokersList, kafkaRequestInputTopic);

		DataStream<String> datastream = env.addSource(kc.getFc());
		DataStream<String> RQ_stream = env.addSource(requests.getFc());
		DataStream<String> globalStateStream = env.addSource(globalStateConsumer.getFc());

		//map kafka data input to tuple2<int,double>
		DataStream<Datapoint> dataStream = datastream
				.map(new MapFunction<String, Datapoint>() {
					@Override
					public Datapoint map(String node) throws IOException {
						// TODO Auto-generated method stub
						ObjectMapper objectMapper = new ObjectMapper();
						Datapoint dp = objectMapper.readValue(node, Datapoint.class);
						return dp;
					}
				}).name("DATA_SOURCE").keyBy((KeySelector<Datapoint, String>)Datapoint::getKey);

		//DataStream<Tuple2<String, String>> dataStream = datastream.flatMap(new IngestionMultiplierFlatMap(multi)).setParallelism(parallelism2).keyBy(0);
		DataStream<Request> RQ_Stream = RQ_stream
				.map(new MapFunction<String, Request>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Request map(String node) throws IOException {
						// TODO Auto-generated method stub
						//String[] valueTokens = node.replace("\"", "").split(",");
						//if(valueTokens.length > 6) {
						ObjectMapper objectMapper = new ObjectMapper();

						// byte[] jsonData = json.toString().getBytes();
						Request request = objectMapper.readValue(node, Request.class);
						return  request;
					}
				}).name("REQUEST_SOURCE").keyBy((KeySelector<Request, String>) Request::getKey);

		DataStream<Request> SynopsisRequests = RQ_Stream
				.flatMap(new RqRouterFlatMap()).name("REQUEST_ROUTER");

		DataStream<Datapoint> onePassGlobalStateDataStream = globalStateStream
				.map(new MapFunction<String, Datapoint>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Datapoint map(String node) throws IOException {
						ObjectMapper objectMapper = new ObjectMapper();
						com.fasterxml.jackson.databind.JsonNode chunk = objectMapper.readTree(node);

						String workerKey = "";

						if (chunk.has("workerKey") && !chunk.get("workerKey").isNull()) {
							workerKey = chunk.get("workerKey").asText();
						}

						if (workerKey == null || workerKey.trim().isEmpty()) {
							throw new IOException("GLOBAL_STATE_CHUNK missing workerKey: " + node);
						}

						return new Datapoint(workerKey, "onepass-global-state", chunk);
					}
				}).name("ONEPASS_GLOBAL_STATE_SOURCE");

		/*
		DataStream<Datapoint> DataStream = dataStream.connect(RQ_Stream)
				                                .flatMap(new dataRouterCoFlatMap()).name("DATA_ROUTER")
												.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

		DataStream<Estimation> estimationStream = DataStream.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey)
				.connect(SynopsisRequests.keyBy((KeySelector<Request, String>) Request::getKey))
				.flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE");

		*/

		//Replace generic dataRouter with round-robin for One-pass*
		DataStream<Datapoint> DataStream = dataStream.connect(RQ_Stream)
				.flatMap(new OnePassDataRouterCoFlatMap(onePassRoutingMode)).name("ONEPASS_AWARE_DATA_ROUTER");

		/*
		 * Global-state chunks are already keyed by workerKey.
		 * They should enter the same physical worker path as normal OnePass data.
		 */
		DataStream<Datapoint> DataStreamWithGlobalState = DataStream.union(onePassGlobalStateDataStream);


		/*
		 * Force routed OnePass keys to the intended Flink worker.
		 *
		 * Important:
		 * Do not keyBy again after partitionCustom, because keyBy may re-hash
		 * _KEYED_0, _KEYED_1, ... into different subtasks.
		 */

		DataStream<Datapoint> partitionedDataStream = DataStreamWithGlobalState
				.partitionCustom(new OnePassWorkerPartitioner(),
						(KeySelector<Datapoint, String>) Datapoint::getKey);

		DataStream<Request> partitionedSynopsisRequests = SynopsisRequests
				.partitionCustom(new OnePassWorkerPartitioner(),
						(KeySelector<Request, String>) Request::getKey);

		DataStream<Estimation> estimationStream = partitionedDataStream
				.connect(partitionedSynopsisRequests)
				.flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE");

		DataStream<Estimation> onePassStateTransferStream = estimationStream
				.filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						if (value == null || value.getSynopsisID() != 30) {
							return false;
						}

						String type = firstParam(value);
						return value.getRequestID() == 78 &&
								("SHARD_BATCH".equals(type) ||
								"SOURCE_DONE".equals(type)) ||
								OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_SOURCE_DONE.equals(type);
					}
				})
				.name("ONEPASS_STATE_TRANSFER_BRANCH");

		onePassStateTransferStream
				.map(new OnePassStateTransferToJson())
				.name("ONEPASS_STATE_TRANSFER_SERIALIZER")
				.addSink(onePassGlobalStateKp.getProducer())
				.name("ONEPASS_STATE_TOPIC_OUTPUT");

		/*
		 * Pre-reduce coordinator input.
		 *
		 * These messages are pure synchronization/control messages.
		 * They must not enter ReduceFlatMap.
		 *
		 * Example:
		 *   DATA_BARRIER_ACK -> OnePassCoordinatorOperator -> GLOBAL_BARRIER_READY
		 */
		DataStream<Estimation> onePassPreReduceCoordinatorInput = estimationStream
				.filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						return isOnePassPreReduceCoordinatorMessage(value);
					}
				}).name("ONEPASS_PRE_REDUCE_COORDINATOR_INPUT");


		/*
		 * Normal estimation stream.
		 *
		 * Important:
		 * LOCAL_PHASE1_RESULT must stay here, because it must be merged by
		 * ReduceFlatMap into GLOBAL_PHASE1_RESULT.
		 */
		DataStream<Estimation> normalEstimationStream = estimationStream
				.filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						return !isOnePassPreReduceCoordinatorMessage(value)
								&& !isOnePassControlAck(value)
								&& !isOnePassStateTransferMessage(value);
					}
				}).name("NORMAL_ESTIMATION_STREAM");

		//SplitStream<Estimation> split = estimationStream.split(new OutputSelector<Estimation>()
		SplitStream<Estimation> split = normalEstimationStream.split(new OutputSelector<Estimation>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> select(Estimation value) {
				// TODO Auto-generated method stub
				List<String> output = new ArrayList<>();
				if (value.getNoOfP() == 1) {
					output.add("single");
				}
				else {
					output.add("multy");
				}
				return output;
			}
		});

		DataStream<Estimation> single = split.select("single");
		DataStream<Estimation> multy = split.select("multy").keyBy((KeySelector<Estimation, String>) Estimation::getKey);
		single.addSink(kp.getProducer());
		DataStream<Estimation> partialOutputStream = multy.flatMap(new ReduceFlatMap()).name("REDUCE");

		DataStream<Estimation> finalStream = partialOutputStream.flatMap(new GReduceFlatMap()).setParallelism(1);

		/*
		DataStream<Estimation> onePassPhaseOneGlobalResults = finalStream.filter(new FilterFunction<Estimation>() {
			private static final long serialVersionUID = 1L;
			@Override
			public boolean filter(Estimation value) {
				return isOnePassPhaseOneGlobalResult(value);
			}
		}).name("ONEPASS_PHASE1_GLOBAL_RESULTS");

		DataStream<Estimation> onePassPhaseOneStateMessages = onePassPhaseOneGlobalResults
				.flatMap(new OnePassPhaseOneRequestSplitter())
				.name("ONEPASS_PHASE1_REQUEST_SPLITTER")
				.setParallelism(1);

		DataStream<Estimation> onePassPhaseOneFeedback = onePassPhaseOneStateMessages
				.flatMap(new OnePassCoordinatorFilter())
				.name("ONEPASS_PHASE1_FEEDBACK_COORDINATOR")
				.setParallelism(1);

		*/

		SplitStream<Estimation> split_2 = finalStream.split(new OutputSelector<Estimation>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> select(Estimation value) {
				// TODO Auto-generated method stub
				List<String> output = new ArrayList<>();
				if (value.getRequestID() == 7) {
					output.add("UR");
				}
				else {
					output.add("E");
				}
				return output;
			}
		});

		DataStream<Estimation> onePassPhaseOneAliasReady = finalStream
				.filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						return value != null
								&& value.getSynopsisID() == 30
								&& value.getRequestID() == 77
								&& "GLOBAL_PHASE1_ALIAS_READY".equals(firstParam(value));
					}
				})
				.name("ONEPASS_PHASE1_ALIAS_READY");

		DataStream<Estimation> onePassPhaseOneTransitions = onePassPhaseOneAliasReady
				.flatMap(new OnePassPhaseOneTransitionMapper())
				.name("ONEPASS_PHASE1_TRANSITION_MAPPER")
				.setParallelism(1);

		DataStream<Estimation> UR = split_2.select("UR");
		DataStream<Estimation> E = split_2.select("E");
		//E.addSink(kp.getProducer());
		//UR.addSink(pRequest.getProducer());


		/*
		 * Post-reduce coordinator input.
		 *
		 * These are already merged OnePass global results.
		 * The coordinator must not merge them again.
		 *
		 * Phase 1 is excluded from this old coordinator path.
		 * Phase 2/3 remain here until they are migrated.
		 */
		DataStream<Estimation> onePassPostReduceCoordinatorInput = finalStream.
				filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						return isOnePassPostReduceCoordinatorMessage(value);
					}
				}).name("ONEPASS_POST_REDUCE_COORDINATOR_INPUT");


		/*
		 * Global state payload path.
		 *
		 * This is now the legacy Phase 2/3 global-state path only.
		 * Phase 1 feedback is carried through RequestTopic above.
		 */
		DataStream<String> onePassGlobalStateChunks = onePassPostReduceCoordinatorInput
				.flatMap(new OnePassGlobalStateSplitter())
				.name("ONEPASS_GLOBAL_STATE_SPLITTER");

		onePassGlobalStateChunks
				.addSink(onePassGlobalStateKp.getProducer())
				.name("ONEPASS_GLOBAL_STATE_TOPIC_OUTPUT");

		DataStream<Estimation> finalStreamExternalOutput = finalStream
				.filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						return !isOnePassLargeGlobalStateResult(value);
					}
				}).name("FINAL_STREAM_EXTERNAL_OUTPUT_FILTER");

		finalStreamExternalOutput
				.addSink(kp.getProducer())
				.name("FINAL_STREAM_EXTERNAL_OUTPUT");

		/*
		 * Single OnePass coordinator stage.
		 *
		 * Receives:
		 *   - pre-reduce ACKs
		 *   - post-reduce global results
		 *
		 * Emits:
		 *   - GLOBAL_BARRIER_READY
		 *   - GLOBAL_PHASE1_RESULT_READY
		 */
		DataStream<Estimation> onePassCoordinatorOutput = onePassPreReduceCoordinatorInput
				.union(onePassPostReduceCoordinatorInput)
				.flatMap(new OnePassCoordinatorOperator())
				.name("ONEPASS_COORDINATOR")
				.setParallelism(1);

		DataStream<Estimation> onePassCoordinatorRequestOutput = onePassCoordinatorOutput
				.filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						return isOnePassCoordinatorRequestCommand(value);
					}
				}).name("ONEPASS_COORDINATOR_REQUEST_OUTPUT");

		DataStream<Estimation> onePassCoordinatorEstimationOutput = onePassCoordinatorOutput
				.filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						return !isOnePassCoordinatorRequestCommand(value);
					}
				}).name("ONEPASS_COORDINATOR_ESTIMATION_OUTPUT");

		/*
		 * requestID == 7 is serialized as Request by kafkaProducerEstimation.
		 *
		 * Phase 1 uses the new BEGIN/CHUNK/COMMIT + transition path.
		 * Phase 2/3 still use requests emitted by OnePassCoordinatorOperator.
		 */
		DataStream<Estimation> onePassRequestFeedback = onePassPhaseOneTransitions
				.union(onePassCoordinatorRequestOutput);

		onePassRequestFeedback
				.addSink(pRequest.getProducer())
				.name("ONEPASS_REQUEST_TOPIC_FEEDBACK")
				.setParallelism(1);

		/*
		 * Readiness/status events stay in estimationTopic.
		 */
		onePassCoordinatorEstimationOutput.addSink(kp.getProducer()).name("ONEPASS_COORDINATOR_OUTPUT");

		env.execute("Streaming SDE");

	}

	private static boolean isOnePassPreReduceCoordinatorMessage(Estimation value) {
		if (value == null) {
			return false;
		}

		if (value.getSynopsisID() != 30) {
			return false;
		}

		String type = firstParam(value);

		if (value.getRequestID() == 70 && "DATA_BARRIER_ACK".equals(type)) {
			return true;
		}

		if (value.getRequestID() == 75 && "INSTALL_GLOBAL_INDEX_ACK".equals(type)) {
			return true;
		}

		if (value.getRequestID() == 85 && "INSTALL_ROOT_SAMPLE_ACK".equals(type)) {
			return true;
		}
		if (value.getRequestID() == 95 && "INSTALL_PHASE3_ALIAS_SELECTIONS_ACK".equals(type)) {
			return true;
		}

		return false;
	}

	private static boolean isOnePassCoordinatorRequestCommand(Estimation value) {
		if (value == null) {
			return false;
		}

		if (value.getSynopsisID() != 30) {
			return false;
		}

		String type = firstParam(value);
		return value.getRequestID() == 7
				&& ("INSTALL_GLOBAL_INDEX".equals(type)
				|| "INSTALL_ROOT_SAMPLE".equals(type)
				|| "INSTALL_PHASE3_ALIAS_SELECTIONS".equals(type));
	}

	private static boolean isOnePassPostReduceCoordinatorMessage(Estimation value) {
		if (value == null) {
			return false;
		}

		if (value.getSynopsisID() != 30) {
			return false;
		}

		/*
		 * Phase 1 now uses OnePassPhaseOneRequestSplitter +
		 * OnePassCoordinatorFilter. Keep the old coordinator/globalStateTopic
		 * path only for the still-unmigrated Phase 2 and Phase 3.
		 */
		String type = firstParam(value);

		if (value.getRequestID() == 83 && "GLOBAL_PHASE2_ROOT_SAMPLE".equals(type)) {
			return true;
		}

		if (value.getRequestID() == 93 && "GLOBAL_PHASE3_ALIAS_RESULT".equals(type)) {
			return true;
		}

		return false;
	}

	private static String firstParam(Estimation value) {
		if (value == null) {
			return "";
		}

		String[] param = value.getParam();

		if (param == null || param.length == 0 || param[0] == null) {
			return "";
		}

		return param[0].trim();
	}

	private static boolean isOnePassControlAck(Estimation value) {
		if (value == null) {
			return false;
		}

		if (value.getSynopsisID() != 30) {
			return false;
		}

		/*
		 * SDEcoFlatMap currently emits lightweight ACKs for RequestID=7 commands,
		 * for example FINISH_PHASE_1. These ACKs are useful for debugging/tests,
		 * but they are not mergeable algorithmic results.
		 *
		 * They should not enter ReduceFlatMap.
		 */
		if (value.getRequestID() != 7) {
			return false;
		}

		String type = firstParam(value);

		return "FINISH_PHASE_1".equals(type) || "FINISH_PHASE_2".equals(type) || "START_PHASE_3_ALIAS".equals(type)
				|| "FINISH_PHASE_3_ALIAS".equals(type) || "FINISH_PHASE_3".equals(type) || "STATUS".equals(type);
	}

	private static boolean isOnePassLargeGlobalStateResult(Estimation value) {
		if (value == null) {
			return false;
		}

		if (value.getSynopsisID() != 30) {
			return false;
		}

		String type = firstParam(value);

		if (value.getRequestID() == 73 && "GLOBAL_PHASE1_RESULT".equals(type)) {
			return true;
		}

		if (value.getRequestID() == 83 && "GLOBAL_PHASE2_ROOT_SAMPLE".equals(type)) {
			return true;
		}

		if (value.getRequestID() == 93 && "GLOBAL_PHASE3_ALIAS_RESULT".equals(type)) {
			return true;
		}

		return false;
	}

	private static boolean isOnePassPhaseOneGlobalResult(
			Estimation value) {

		if (value == null) {
			return false;
		}

		if (value.getSynopsisID() != 30) {
			return false;
		}

		return value.getRequestID() == 73 && "GLOBAL_PHASE1_RESULT".equals(firstParam(value));
	}

	private static boolean isOnePassStateTransferMessage(Estimation value) {
		if (value == null || value.getSynopsisID() != 30 || value.getRequestID() != 78) {
			return false;
		}

		String type = firstParam(value);
		return "SHARD_BATCH".equals(type) ||
				"SOURCE_DONE".equals(type) ||
				OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_BATCH.equals(type) ||
				OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_SOURCE_DONE.equals(type);
	}

	private static void initializeParameters(String[] args) {

		if (args.length > 4) {

			System.out.println("[INFO] User Defined program arguments");
			//User defined program arguments
			kafkaDataInputTopic = args[0];
			kafkaRequestInputTopic = args[1];
			kafkaOutputTopic = args[2];
			kafkaBrokersList = args[3];
			//kafkaBrokersList = "localhost:9092";
			parallelism = Integer.parseInt(args[4]);
			if (args.length > 5) {
				kafkaOnePassStateTopic = args[5];
			} else {
				kafkaOnePassStateTopic = "onepassStateTopic";
			}
			if (args.length > 6) {
				onePassRoutingMode = OnePassDataRouterCoFlatMap.RoutingMode.fromString(args[6]
				);
			} else {
				onePassRoutingMode = OnePassDataRouterCoFlatMap.RoutingMode.ROUND_ROBIN;
			}
			//parallelism2 = Integer.parseInt(args[5]);
			//multi = Integer.parseInt(args[5]);

		}else{

			System.out.println("[INFO] Default values");
			//Default values
			//kafkaDataInputTopic = "FAN";
			kafkaDataInputTopic = "dataTopic";
			kafkaRequestInputTopic = "requestTopic";
			//kafkaRequestInputTopic = "Rq_FAN";
			parallelism = 4;
			//parallelism2 = 4;
			//kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
			//kafkaBrokersList = "45.10.26.123:19092";
			kafkaBrokersList = "localhost:9092";
			//kafkaBrokersList = "159.69.32.166:9092";
			kafkaOutputTopic = "estimationTopic";
			kafkaOnePassStateTopic = "onepassStateTopic";
			onePassRoutingMode = OnePassDataRouterCoFlatMap.RoutingMode.JOIN_KEY_HASH;
		}
	}
}
