//import CountMinSketchState;
package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;
import backtype.storm.tuple.Values;


/**
 * This BaseQueryFunction is sued to get top K words from the count min
 *@author: bkakran
 */


public class CountTopKQuery extends BaseQueryFunction<CountMinSketchState, String> {
   //Get the top k words from the count min and return it to the storm
	public List<String> batchRetrieve(CountMinSketchState state, List<TridentTuple> inputs) {
        List<String> ret = new ArrayList();
        ret.add(state.getTopKAsString());
        return ret;
    }

	//returns the topk value string
    public void execute(TridentTuple tuple, String topK, TridentCollector collector) {
        collector.emit(new Values(topK));
    }    
}
