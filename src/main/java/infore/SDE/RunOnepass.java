package infore.SDE;


import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.kafkaStringConsumer;

import infore.SDE.transformations.*;
import infore.SDE.transformations.onepass.OnePassDataRouterCoFlatMap;
import infore.SDE.transformations.onepass.coordinator.OnePassWorkerPartitioner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.transformations.onepass.coordinator.OnePassCoordinatorOperator;

/**
 * <br>
 * Implementation code for SDE for INFORE-PROJECT" <br> *
 * ATHENA Research and Innovation Center <br> *
 * Author: Antonis_Kontaxakis <br> *
 * email: adokontax15@gmail.com *
 */


public class RunOnepass {

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

        DataStream<Request> SynopsisRequests = RQ_Stream.flatMap(new RqRouterFlatMap()).name("REQUEST_ROUTER");

        /*
         * OnePass logical data router:
         *   baseKey -> baseKey_2_KEYED_0, baseKey_2_KEYED_1, ...
         */
        DataStream<Datapoint> routedDataStream = dataStream.connect(RQ_Stream)
                .flatMap(new OnePassDataRouterCoFlatMap()).name("ONEPASS_ROUND_ROBIN_DATA_ROUTER");

        /*
         * Force routed data to the intended physical worker.
         */
        DataStream<Datapoint> partitionedDataStream = routedDataStream.partitionCustom(new OnePassWorkerPartitioner(),
                                (KeySelector<Datapoint, String>) Datapoint::getKey);

        /*
         * Force routed requests to the same intended physical worker.
         */
        DataStream<Request> partitionedRequestStream = SynopsisRequests.partitionCustom(new OnePassWorkerPartitioner(),
                                (KeySelector<Request, String>) Request::getKey);

        /*
         * Important:
         * Do NOT keyBy again here.
         * keyBy would re-hash and could send _KEYED_0 and _KEYED_1 to the same subtask.
         */
        DataStream<Estimation> estimationStream = partitionedDataStream.connect(partitionedRequestStream)
                        .flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE");

        DataStream<Datapoint> DataStream = dataStream.connect(RQ_Stream)
                .flatMap(new OnePassDataRouterCoFlatMap()).name("ONEPASS_ROUND_ROBIN_DATA_ROUTER")
                .keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

        /*
         * Route One-pass* coordinator messages away from the old generic SDE
         * multi-parallel reduce path.
         */
        DataStream<Estimation> onePassCoordinatorInput = estimationStream.filter(new FilterFunction<Estimation>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public boolean filter(Estimation value) {
                                return isOnePassCoordinatorMessage(value);
                            }
                        }).name("ONEPASS_COORDINATOR_INPUT");

        DataStream<Estimation> normalEstimationStream = estimationStream.filter(new FilterFunction<Estimation>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public boolean filter(Estimation value) {
                                return !isOnePassCoordinatorMessage(value);
                            }
                        }).name("NORMAL_ESTIMATION_STREAM");


        /*
         * First real Flink coordinator stage.
         *
         * V0 behavior:
         *   workers -> DATA_BARRIER_ACK
         *   coordinator -> GLOBAL_BARRIER_READY
         */
        DataStream<Estimation> onePassCoordinatorOutput =
                onePassCoordinatorInput.flatMap(new OnePassCoordinatorOperator())
                        .name("ONEPASS_COORDINATOR")
                        .setParallelism(1);

        /*
         * For now we write coordinator output to the same estimation output topic.
         */
        onePassCoordinatorOutput.addSink(kp.getProducer()).name("ONEPASS_COORDINATOR_OUTPUT");

        DataStream<Estimation> single = normalEstimationStream.filter(new FilterFunction<Estimation>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public boolean filter(Estimation value) {
                                return value.getNoOfP() == 1;
                            }
                        }).name("SINGLE_OUTPUT");

        DataStream<Estimation> multy = normalEstimationStream.filter(new FilterFunction<Estimation>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public boolean filter(Estimation value) {
                                return value.getNoOfP() != 1;
                            }
                        }).name("MULTY_OUTPUT")
                        .keyBy((KeySelector<Estimation, String>) Estimation::getKey);

        single.addSink(kp.getProducer()).name("SINGLE_KAFKA_OUTPUT");

        DataStream<Estimation> partialOutputStream = multy.flatMap(new ReduceFlatMap()).name("REDUCE");

        DataStream<Estimation> finalStream = partialOutputStream.flatMap(new GReduceFlatMap()).name("GLOBAL_REDUCE")
                        .setParallelism(1);

        finalStream.addSink(kp.getProducer()).name("FINAL_KAFKA_OUTPUT");
        env.execute("Streaming SDE");

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

    private static boolean isOnePassCoordinatorMessage(Estimation value) {
        if (value == null) {
            return false;
        }

        if (value.getSynopsisID() != 30) {
            return false;
        }

        String[] param = value.getParam();

        if (param == null || param.length == 0 || param[0] == null) {
            return false;
        }

        String type = param[0].trim();

        if (value.getRequestID() == 70 && "DATA_BARRIER_ACK".equals(type)) {
            return true;
        }

        if (value.getRequestID() == 72 && "LOCAL_PHASE1_RESULT".equals(type)) {
            return true;
        }

        return false;
    }
}
