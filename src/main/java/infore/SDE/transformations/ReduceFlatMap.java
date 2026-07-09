package infore.SDE.transformations;

import infore.SDE.reduceFunctions.*;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.WLSH_Reduce;
import infore.SDE.reduceFunctions.onepass.OnePassReduceFunctionFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class ReduceFlatMap extends RichFlatMapFunction<Estimation, Estimation> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private HashMap<String, ReduceFunction> rf = new HashMap<>();


    @Override
    public void flatMap(Estimation value, Collector<Estimation> out){

        ReduceFunction t_rf = rf.get("" + value.getEstimationkey());
        int id = value.getSynopsisID();
        String key  = value.getEstimationkey();

            if (t_rf == null){

                t_rf = initReduceFunction(value, id);

                if (t_rf == null) {
                    System.out.println("[ReduceFlatMap] No reducer for synopsisID=" + id + ", requestID=" +
                            value.getRequestID() + ", estimationkey=" + value.getEstimationkey());
                    return;
                }

                rf.put("" + key, t_rf);

            }else{

                if (t_rf.add(value)) {
                    Object output = t_rf.reduce();
                    if (output != null) {
                        value.setEstimation(output);
                        rf.remove("" + key);
                        if(id == 28)
                            value.setEstimationkey(value.getUID()+"");
                        if(id == 30){
                            decorateOnePassReducedEstimation(value);
                        }
                        out.collect(value);
                    }

                }
            }
        }

    private ReduceFunction initReduceFunction(Estimation value, int id) {
        ReduceFunction t_rf = null;
        //RadiusCount
        if (id == 100) {
            t_rf = new RadiusReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }

        //MAX
        if (id == 11) {
            t_rf = new SimpleMaxFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //AVG
        else if (id == 15) {
            t_rf = new SimpleAvgFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //SUM
        else if (id == 1 || id == 3 || id == 8 || id == 9 || id == 7) {

            if (id == 1 && value.getRequestID() % 10 == 6) {
                t_rf = new JoinEstimationFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            } else {
                t_rf = new SimpleSumFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            }
            t_rf.add(value);
        }
        //OR
        else if (id == 2) {
            t_rf = new SimpleORFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //DFT CORRELATION
        else if (id == 4){
            t_rf = new CorrelationDFTReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //KMEANS CORESETS
        else if( id == 6) {
            //System.out.println("START");
            t_rf = new KmeansReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //KMEANS CORESETS
        else if( id == 13) {
            //System.out.println("START");
            t_rf = new TopKReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //WINDOW LSH SYNOPSIS
        else if (id == 28) {
            t_rf = new WLSH_Reduce(value.getNoOfP(), 0,value.getEstimationkey(), Double.parseDouble(value.getParam()[0]),Integer.parseInt(value.getParam()[1]));
            t_rf.add(value);
        }
        else if (id == 29) {
            t_rf = new WDFT_Reduce(value.getNoOfP(), Double.parseDouble(value.getParam()[0]),Integer.parseInt(value.getParam()[0]),Integer.parseInt(value.getParam()[0]),stringToStringArray(value.getParam()[0]));
            //int workers, double th, int k, int t, String[] stock
            t_rf.add(value);
        }
        //One-pass* SYNOPSIS
        else if (id == 30) {
            t_rf = OnePassReduceFunctionFactory.create(value);

            if (t_rf != null) {
                t_rf.add(value);
            }
        }
        return t_rf;
    }


    private  String[] stringToStringArray(String param)
    {
        return param.split(";");
    }

    private void decorateOnePassReducedEstimation(Estimation value) {
        String[] param = value.getParam();

        if (value.getRequestID() == 72 && param != null && param.length > 0 && "LOCAL_PHASE1_RESULT".equals(param[0])) {

            String resultId = "PHASE1_RESULT_" + value.getUID();

            if (param.length > 1 && param[1] != null && !param[1].trim().isEmpty()) {
                resultId = param[1].trim();
            }

            String globalKey = value.getUID() + "_PHASE1_" + resultId + "_GLOBAL";

            value.setRequestID(73);
            value.setEstimationkey(globalKey);
            value.setKey(globalKey);

            value.setParam(new String[] {"GLOBAL_PHASE1_RESULT", resultId, "PHASE1", "", "",
                    Integer.toString(value.getNoOfP())});
        }

        if (value.getRequestID() == 82 && param != null && param.length > 0 && "LOCAL_PHASE2_ROOT_SUMMARY".equals(param[0])) {

            String resultId = "PHASE2_RESULT_" + value.getUID();

            if (param.length > 1 && param[1] != null && !param[1].trim().isEmpty()) {
                resultId = param[1].trim();
            }

            String globalKey = value.getUID() + "_PHASE2_" + resultId + "_GLOBAL";

            value.setRequestID(83);
            value.setEstimationkey(globalKey);
            value.setKey(globalKey);

            value.setParam(new String[] {"GLOBAL_PHASE2_ROOT_SAMPLE", resultId, "PHASE2", "", "",
                    Integer.toString(value.getNoOfP())});
        }

        if (value.getRequestID() == 92 && param != null && param.length > 0 && "LOCAL_PHASE3_ALIAS_RESULT".equals(param[0])) {

            String resultId = "PHASE3_ALIAS_RESULT_" + value.getUID();
            String alias = "";

            if (param.length > 1 && param[1] != null && !param[1].trim().isEmpty()) {
                resultId = param[1].trim();
            }

            if (param.length > 2 && param[2] != null && !param[2].trim().isEmpty()) {
                alias = param[2].trim();
            }

            String globalKey = value.getUID() + "_PHASE3_ALIAS_" + resultId + "_GLOBAL";

            value.setRequestID(93);
            value.setEstimationkey(globalKey);
            value.setKey(globalKey);

            value.setParam(new String[] {"GLOBAL_PHASE3_ALIAS_RESULT", resultId, alias, "PHASE3",
                    Integer.toString(value.getNoOfP())});
        }
    }

}

