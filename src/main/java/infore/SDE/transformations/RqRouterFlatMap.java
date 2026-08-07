package infore.SDE.transformations;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import infore.SDE.messages.Request;

public class RqRouterFlatMap extends RichFlatMapFunction<Request, Request> implements Serializable{

	private static final long serialVersionUID = 1L;

   //SourceID + Uid (1-1000),Keys(1-1000),        
	@Override
	public void flatMap(Request rq, Collector<Request> out) throws Exception {

		//OnePass router
		if (rq.getSynopsisID() == 30 && rq.getNoOfP() > 1) {
			String baseKey = rq.getKey();

			for (int i = 0; i < rq.getNoOfP(); i++) {
				Request routed = copyRequest(rq);
				routed.setDataSetkey(baseKey + "_" + rq.getNoOfP() + "_KEYED_" + i);

				out.collect(routed);
			}

			return;
		}

			 //ADD-REMOVE-ESTIMATE SKETCH FOR A STREAM
			  if( rq.getRequestID()%10 < 8) {
				  if(rq.getNoOfP() == 1)
				  out.collect(rq);
				  else {
					  String tmpkey = rq.getKey();
					  if(rq.getRequestID()%10 == 6) {
							int n = Integer.parseInt(rq.getParam()[0]);
							String[] dataSets = rq.getDataSetkey().split(",");
							String[] tpr = rq.getParam();
							String[] pr = rq.getParam();
							pr[1]=""+rq.getUID();

							for(int i=0;i< n;i++){

								tmpkey = dataSets[i];
								rq.setUID(Integer.parseInt(tpr[i+1]));

								for(int j = 0; j < rq.getNoOfP(); j ++) {

									rq.setKey(tmpkey + "_" + rq.getNoOfP() + "_KEYED_" + j);
									out.collect(rq);

								}

							}
					  }else{
						  if(rq.getSynopsisID() == 100){
							  for(int i=0;i<Integer.parseInt(rq.getParam()[2]);i++){
								  rq.setDataSetkey(tmpkey + "_" + i);

								  out.collect(rq);

							  }
							  return;
						  }
						  for(int i = 0; i < rq.getNoOfP(); i ++) {

						  	if(rq.getRequestID()%10 == 4){
								rq.setKey(tmpkey +"_"+rq.getNoOfP()+"_RANDOM_" + i);
								out.collect(rq);
							}else{
						  		//System.out.println("here ->" + i );
								rq.setDataSetkey(tmpkey + "_" + rq.getNoOfP() + "_KEYED_" + i);

								out.collect(rq);
								//rq.setKey(tmpkey +"_"+rq.getNoOfP()+"_RANDOM_" + i);
								//out.collect(rq);
							}
						  }
					  }
				  }
			  }	  
	}

	private static Request copyRequest(Request rq) {
		Request copy = new Request();

		copy.setDataSetkey(rq.getDataSetkey());
		copy.setRequestID(rq.getRequestID());
		copy.setSynopsisID(rq.getSynopsisID());
		copy.setUID(rq.getUID());
		copy.setStreamID(rq.getStreamID());
		copy.setNoOfP(rq.getNoOfP());
		copy.setParameters(rq.getParameters() == null ? null : rq.getParameters().deepCopy());

		String[] param = rq.getParam();

		if (param != null) {
			String[] paramCopy = new String[param.length];
			System.arraycopy(param, 0, paramCopy, 0, param.length);
			copy.setParam(paramCopy);
		}

		return copy;
	}
}
