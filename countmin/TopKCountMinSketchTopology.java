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
import storm.starter.trident.project.countmin.state.CountTopKQuery;
import storm.starter.trident.project.filters.PrintFilter;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.tutorial.functions.SplitFunction;

/**
 *This class implements the topK words calculation on storm using countmin and bloom filter. 
 *@author: Preetham MS (pmahish@ncsu.edu)
 *@author bkakran
 */


public class TopKCountMinSketchTopology {
	//Variables representing the twitter related constants and filter keywords
	private static String consumerKey;
	private static String consumerSecret;
	private static String accessToken;
	private static String accessTokenSecret;
	private static String[] keyWords;
	
	//Build Storm topology
	 public static StormTopology buildTopology( LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();
      
        //Configuration for count min
	    int width = 1000;
		int depth = 15;
		int seed = 10;
		
		//Spout to read streams of tweets filtered by key words
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret, keyWords);
		
		//Create a trident count min state to extract sentemce from tweet and split it and add it to the count min
		TridentState countMinDBMS = topology.newStream("tweets", spoutTweets)
			//parse each tweet
			.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
			//.each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
			//get the sentence out of tweet by extracting tweet text
			.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
			//.each(new Fields("sentence"), new PrintFilter("PARSED sentence:"))
			//Split the sentence to words
			.each(new Fields("sentence"), new Split(), new Fields("words"))
			//Store each of the words in the count min data structure
			.partitionPersist( new CountMinSketchStateFactory(depth,width,seed,5), new Fields("words"), new CountMinSketchUpdater())
			;

		//create event get_topquery to get top 5 elements 
		topology.newDRPCStream("get_topquery", drpc)
			.stateQuery(countMinDBMS, new Fields("args"), new CountTopKQuery(), new Fields("top-k"))
			.project(new Fields("top-k"))
			;

		return topology.build();

	}


	public static void main(String[] args) throws Exception {
		Config conf = new Config();
        	conf.setDebug( false );
        	conf.setMaxSpoutPending( 10 );
        	//Twitter related configurations
        	consumerKey = args[0];
    		consumerSecret=args[1];
    		accessToken=args[2];
    		accessTokenSecret=args[3];
    		//Check for filter keywords
    		if (args.length > 4) {
				int keywordlen = args.length - 4;
				keyWords = new String[keywordlen];
				for (int i = 0; i < keyWords.length; i++) {
					keyWords[i] = args[4 + i];
				}
			}
    		//create a local cluster
        	LocalCluster cluster = new LocalCluster();
        	LocalDRPC drpc = new LocalDRPC();
        	cluster.submitTopology("get_topquery",conf,buildTopology(drpc));
        	Thread.sleep( 2000 );
        	//Query every 5 seconds 100 times for top-5 words
        	for (int i = 0; i < 100; i++) {
            		System.out.println("DRPC RESULT:"+ drpc.execute("get_topquery",""));
            		Thread.sleep( 5000 );
        	}

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        	//drpc.shutdown();
	}
}
