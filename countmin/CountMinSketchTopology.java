package storm.starter.trident.project.countmin; 

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;


import storm.trident.operation.builtin.Count;

import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
import storm.starter.trident.project.filters.PrintFilter;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.tutorial.functions.SplitFunction;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 */


public class CountMinSketchTopology {

	private static String consumerKey;
	private static String consumerSecret;
	private static String accessToken;
	private static String accessTokenSecret;
	private static String[] keyWords;
	
	 public static StormTopology buildTopology( LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int width = 100;
	int depth = 15;
	int seed = 10;

    	FixedBatchSpout spoutFixedBatch = new FixedBatchSpout(new Fields("sentence"), 3,
			new Values("the cow jumped over the moon"),
			new Values("the man went to the store and bought some candy"),
			new Values("four score and seven years ago"),
			new Values("how many apples can you eat"),
			new Values("to be or not to be the person"))
			;
		spoutFixedBatch.setCycle(true);

		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret, keyWords);
		
		TridentState countMinDBMS = topology.newStream("tweets", spoutTweets)
			.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
			.each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
			.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
			.each(new Fields("sentence"), new PrintFilter("PARSED sentence:"))
			.each(new Fields("sentence"), new Split(), new Fields("words"))
			.partitionPersist( new CountMinSketchStateFactory(depth,width,seed,5), new Fields("words"), new CountMinSketchUpdater())
			;


		topology.newDRPCStream("get_count", drpc)
			.each( new Fields("args"), new Split(), new Fields("query"))
			.stateQuery(countMinDBMS, new Fields("query"), new CountMinQuery(), new Fields("count"))
			.project(new Fields("query", "count"))
			;

		return topology.build();

	}


	public static void main(String[] args) throws Exception {
		Config conf = new Config();
        	conf.setDebug( false );
        	conf.setMaxSpoutPending( 10 );

        	consumerKey = args[0];
    		consumerSecret=args[1];
    		accessToken=args[2];
    		accessTokenSecret=args[3];
			if (args.length > 4) {
				int keywordlen = args.length - 4;
				keyWords = new String[keywordlen];
				for (int i = 0; i < keyWords.length; i++) {
					keyWords[i] = args[4 + i];
				}
			}
        	LocalCluster cluster = new LocalCluster();
        	LocalDRPC drpc = new LocalDRPC();
        	cluster.submitTopology("get_count",conf,buildTopology(drpc));
        	Thread.sleep( 2000 );
        	for (int i = 0; i < 10; i++) {

            		System.out.println("DRPC RESULT:"+ drpc.execute("get_count","to the apple"));
            		Thread.sleep( 5000 );
        	}

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        	//drpc.shutdown();
	}
}
