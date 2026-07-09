package infore.SDE;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.kafkaStringConsumer;

import infore.SDE.sources.kafkaStringConsumer_Earliest;
import infore.SDE.transformations.*;
import infore.SDE.transformations.onepass.RoundRobinDataRouterCoFlatMap;
import infore.SDE.transformations.onepass.coordinator.OnePassCoordinatorOperator;
import infore.SDE.transformations.onepass.coordinator.OnePassWorkerPartitioner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;


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
		kafkaStringConsumer kc = new kafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic);
		kafkaStringConsumer requests = new kafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic);
		kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);


		DataStream<String> datastream = env.addSource(kc.getFc());
		DataStream<String> RQ_stream = env.addSource(requests.getFc());

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
				.flatMap(new RoundRobinDataRouterCoFlatMap()).name("ONEPASS_AWARE_DATA_ROUTER");


		/*
		 * Force routed OnePass keys to the intended Flink worker.
		 *
		 * Important:
		 * Do not keyBy again after partitionCustom, because keyBy may re-hash
		 * _KEYED_0, _KEYED_1, ... into different subtasks.
		 */

		DataStream<Datapoint> partitionedDataStream = DataStream
				.partitionCustom(new OnePassWorkerPartitioner(),
						(KeySelector<Datapoint, String>) Datapoint::getKey);

		DataStream<Request> partitionedSynopsisRequests = SynopsisRequests
				.partitionCustom(new OnePassWorkerPartitioner(),
						(KeySelector<Request, String>) Request::getKey);

		DataStream<Estimation> estimationStream = partitionedDataStream
				.connect(partitionedSynopsisRequests)
				.flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE");

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
						return !isOnePassPreReduceCoordinatorMessage(value) && !isOnePassControlAck(value);
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
		 * Example:
		 *   GLOBAL_PHASE1_RESULT -> OnePassCoordinatorOperator -> GLOBAL_PHASE1_RESULT_READY
		 */
		DataStream<Estimation> onePassPostReduceCoordinatorInput = finalStream.
				filter(new FilterFunction<Estimation>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Estimation value) {
						return isOnePassPostReduceCoordinatorMessage(value);
					}
				}).name("ONEPASS_POST_REDUCE_COORDINATOR_INPUT");

		finalStream.addSink(kp.getProducer());

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

		onePassCoordinatorOutput.addSink(kp.getProducer()).name("ONEPASS_COORDINATOR_OUTPUT");

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
		return value.getRequestID() == 70 && "DATA_BARRIER_ACK".equals(type);
	}

	private static boolean isOnePassPostReduceCoordinatorMessage(Estimation value) {
		if (value == null) {
			return false;
		}

		if (value.getSynopsisID() != 30) {
			return false;
		}

		String type = firstParam(value);
		return value.getRequestID() == 73 && "GLOBAL_PHASE1_RESULT".equals(type);
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
		}
	}
}
