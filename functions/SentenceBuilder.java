package storm.starter.trident.project.functions;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import storm.starter.trident.project.functions.Tweet;
import com.google.common.collect.Lists;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;
import org.elasticsearch.index.query.QueryBuilders;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;

/**
 * 
 * Create the sentence by just extracting the twitter text
 */
public class SentenceBuilder extends BaseFunction {
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// "text", "tweetId", "user"
		String text = tuple.getString(0).toLowerCase();
		long tweetId = tuple.getLong(1);
		String user = tuple.getString(2);
		

		collector.emit(new Values(text));
	}
}
