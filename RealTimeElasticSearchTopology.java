package storm.starter.trident.project;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteIndex;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import storm.starter.trident.project.functions.CreateJson;
import storm.starter.trident.project.functions.DocumentBuilder;
import storm.starter.trident.project.functions.ExtractDocumentInfo;
import storm.starter.trident.project.functions.ExtractSearchArgs;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.functions.Tweet;
import storm.starter.trident.project.functions.TweetBuilder;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.ClientFactory.NodeClient;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.QuerySearchIndexQuery;
import com.github.tlrx.elasticsearch.test.EsSetup;
// Local spouts, functions and filters


/**
* This topology shows how to build ElasticSearch engine for a stream made of
* tweets and how to query it, using DRPC calls. 
* This example should be intended as
* an example of {@link TridentState} custom implementation.
* Modified by bkakran from original author:
* @author Nagiza Samatova (samatova@csc.ncsu.edu)
* @author Preetham Srinath (pmahish@ncsu.edu)
*/
public class RealTimeElasticSearchTopology {
	
    //Variables representing the twiiter realted constants and filter keywords
	private static String consumerKey;
	private static String consumerSecret;
	private static String accessToken;
	private static String accessTokenSecret;
	private static String[] keyWords;

	//Method to build storm topology
	public static StormTopology buildTopology(String[] args, Settings settings, LocalDRPC drpc)
            throws IOException {

	TridentTopology topology = new TridentTopology();

	//This class implements Trident State on top of ElasticSearch
	ESIndexMapState.Factory<Tweet> stateFactory = ESIndexMapState
	      .nonTransactional(new ClientFactory.NodeClient(settings.getAsMap()), Tweet.class);
        
	TridentState staticState = topology
              .newStaticState(new ESIndexState.Factory<Tweet>(
              new NodeClient(settings.getAsMap()), Tweet.class));
	
		//Spout to read streams of tweets filtered by key words
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret, keyWords);
		
        /**
         * Read a stream of tweets, parse and extract
         * only the text, and its id. Store the stream
         * using the {@link ElasticSearchState} implementation for Trident.
		 * NOTE: Commented lines are for debugging only
		 * TO DO FOR PROJECT 2: PART B:
		 *    1. Write a spout that ingests real text data stream:
		 * 		 You are allowed to utilize TweeterSampleSpout provided (DEAFULT), or
		 * 		 you may consider writing a spout that ingests Wikipedia updates
		 *		 or any other text stream (e.g., using Google APIs to bring a synopsis of a URL web-page
		 *		 for a stream of URL web-pages).
		 *	  2. Write proper support/utility functions to prepare your text data
		 * 		 for passing to the ES stateFactory: e.g., WikiUpdatesDocumentBuilder(), 
		 *		 GoogleWebURLSynopsisBuilder().
         */
		/**
		topology.newStream("tweets", spoutFixedBatch)
			//.each(new Fields("sentence"), new Print("sentence field"))
			.parallelismHint(1)
			.each(new Fields("sentence"), new DocumentBuilder(), new Fields("document"))
			.each(new Fields("document"), new ExtractDocumentInfo(), new Fields("id", "index", "type"))
			//.each(new Fields("id"), new Print("id field"))
			.groupBy(new Fields("index", "type", "id"))
			.persistentAggregate(stateFactory,  new Fields("document"), new TweetBuilder(), new Fields("tweet"))
			//.each(new Fields("tweet"), new Print("tweet field"))
			.parallelismHint(1) 
            ;
          **/
		//Read the tweets from the spout and parse the tweet to extract tweet text and then build the sentence out of it
		//and index it to the Elastic Search 
		topology.newStream("tweets", spoutTweets)
		.parallelismHint(1)
        //ParseTweet is used to parse the tweet to get the userName, tweet text and tweetId
		.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
        //.each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
		//SentenceBuilder is used to extract the text from the tweet
		.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
		//Form ES document using the sentence field 
		.each(new Fields("sentence"), new DocumentBuilder(), new Fields("document"))
		//Extract the id, index and type of the ES doc from the document tuple 
		.each(new Fields("document"), new ExtractDocumentInfo(), new Fields("id", "index", "type"))
		//.each(new Fields("id"), new Print("id field"))
		//group it
		.groupBy(new Fields("index", "type", "id"))
		//Get the count of each type of tweet text
		.persistentAggregate(stateFactory,  new Fields("document"), new TweetBuilder(), new Fields("tweet"))
		.parallelismHint(1);
	
				
        /**
         * Now use a DRPC stream to query the state where the tweets are stored.
	 * CRITICAL: DO NOT CHANGE "query", "indicies", "types"
	 * WHY: QuerySearchIndexQuery() has hard-coded these tags in its code:
	 *     https://github.com/fhussonnois/storm-trident-elasticsearch/blob/13bd8203503a81754dc2a421accff216b665a11d/src/main/java/com/github/fhuss/storm/elasticsearch/state/QuerySearchIndexQuery.java 
         */
		 //Query the ES served for all documents stored
	    topology
            .newDRPCStream("search_event", drpc)
            .each(new Fields("args"), new ExtractSearchArgs(), new Fields("query", "indices", "types"))
            .groupBy(new Fields("query", "indices", "types"))
            .stateQuery(staticState, new Fields("query", "indices", "types"), new QuerySearchIndexQuery(), new Fields("tweet"))
            .each(new Fields("tweet"), new FilterNull())
            .each(new Fields("tweet"), new CreateJson(), new Fields("json"))
            .project(new Fields("json"))
	        ; 


        return topology.build();
    }

    public static void main(String[] args) throws Exception {

		// Specify the name and the type of ES index 
		String index_name = new String();
		String index_type = new String();
		index_name = "my_index";
		index_type = "my_type";
		//$TWITTER_CONSUMER_KEY $TWITTER_CONSUMER_SECRET $TWITTER_ACCESS_TOKEN $TWITTER_ACCESS_SECRET man
		consumerKey = args[0];
		consumerSecret=args[1];
		accessToken=args[2];
		accessTokenSecret=args[3];
		//Check for filter keywords
		if(args.length > 4) {
			int keywordlen = args.length - 4;
			keyWords = new String[keywordlen];
			//Add keywords to the keryword array
			for (int i = 0; i < keyWords.length; i++) {
				keyWords[i] = args[4+i];
			}
		}
		/**
		* Configure local cluster and local DRPC 
		***/
        Config conf = new Config();
		conf.setMaxSpoutPending(10);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();

		/**
		* Configure ElasticSearch related information
		* Make sure that elasticsearch (ES) server is up and running
		* Check README.md file on how to install and run ES
		* Note that you can check if index exists and/or was created
		* with curl 'localhost:9200/_cat/indices?v'
		* and type: curl ‘http://127.0.0.1:9200/my_index/_mapping?pretty=1’
		* IMPORTANT: DO NOT CHANGE PORT TO 9200. IT MUST BE 9300 in code below.
		**/
		Settings settings = ImmutableSettings.settingsBuilder()
			.put("storm.elasticsearch.cluster.name", "elasticsearch")
			.put("storm.elasticsearch.hosts", "127.0.0.1:9300")
			.build();

		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    
		/** If you need more options, you can check test code:
		* https://github.com/tlrx/elasticsearch-test/blob/master/src/test/java/com/github/tlrx/elasticsearch/test/EsSetupTest.java
		* Remove else{} stmt if you do not want index to be deleted but rather updated
		* NOTE: Index files are stored by default under $HOME/elasticsearch-1.4.2/data/elasticsearch/nodes/0/indices
		**/
		EsSetup esSetup = new EsSetup(client);
		if (! esSetup.exists(index_name))
		{
			esSetup.execute(createIndex(index_name));
		}
		else
		{
			esSetup.execute(deleteIndex(index_name));
			esSetup.execute(createIndex(index_name));
		}
		//submit topology
        cluster.submitTopology("state_drpc", conf, buildTopology(args, settings, drpc));

		System.out.println("STARTING DRPC QUERY PROCESSING");

	   /***
	   * TO DO FOR PROJECT 2: PART B:
	   *   3. Investigate Java APIs for ElasticSearch for various types of queries:
	   *		http://www.elasticsearch.com/guide/en/elasticsearch/client/java-api/current/index.html
	   *   4. Create at least 3 different query types besides termQuery() below
	   *		and test them with the DRPC execute().
	   *	    Example queries: matchQuery(), multiMatchQuery(), fuzzyQuery(), etc.
	   *   5. Can you thing of the type of indexing technologies ES might be using
	   *		to support efficient query processing for such query types: 
	   *		e.g., fuzzyQuery() vs. boolQuery().
	   ***/
		//Termquery on 'man' keyword
		String query1 = QueryBuilders.termQuery("text", "man").buildAsBytes().toUtf8();
		//Termquery on 'apple' keyword
		String query2 = QueryBuilders.termQuery("text", "apple").buildAsBytes().toUtf8();
		//MultiMatchQuery on 'apple' keyword
		String query3 = QueryBuilders.multiMatchQuery(
			    "apple",     // Text you are looking for
			    "text"           // Fields you query on
			    ).buildAsBytes().toUtf8();
		//BoolQuery on must for 'apple' keyword and mustNot for 'iphone'
		String query4= QueryBuilders
               .boolQuery()
               .must(QueryBuilders.termQuery("text", "apple"))
               .mustNot(QueryBuilders.termQuery("text", "iphone")).buildAsBytes().toUtf8();
		//BoolQuery on must for 'apple' keyword and must for 'iphone'
		String query5= QueryBuilders
               .boolQuery()
               .must(QueryBuilders.termQuery("text", "apple"))
               .must(QueryBuilders.termQuery("text", "iphone"))
               .buildAsBytes().toUtf8();
		//FuzzyQuery on keyword 'applic' with prefix length of 1 and boost of 1 so that apple keyword matches
		String query6= QueryBuilders
			   .fuzzyQuery("text", "applic").prefixLength(0).boost(1)
               .buildAsBytes().toUtf8();
	    //MatchAllQuery to retrieve all documents
		String query7 = QueryBuilders.matchAllQuery().buildAsBytes().toUtf8();
		//PrefixQuery for keyword 'app' keyword with boost of 2
		String query8 =  QueryBuilders.prefixQuery("text", "app").boost(2).buildAsBytes().toUtf8();
		//WildcardQuery for regular expression 'a??l*' to match apple
		String query9 =  QueryBuilders.wildcardQuery("text", "a??l*").buildAsBytes().toUtf8();
	   //BoostingQuery for postive:'apple' and negative:'iphone' with ngative boost as 0.2
		String query10 =  QueryBuilders.boostingQuery()
       .positive(QueryBuilders.termQuery("text","apple"))
       .negative(QueryBuilders.termQuery("text","iphone"))
       .negativeBoost(0.2f).buildAsBytes().toUtf8();
	   
		String drpcResult;
       //Query the storm toplogy
		for (int i = 0; i < 5; i++) {
          drpcResult = drpc.execute("search_event", query1+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for termquery on 'man' keyword: " + drpcResult);
          drpcResult = drpc.execute("search_event", query2+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for termquery on 'apple' keyword: " + drpcResult);
          drpcResult = drpc.execute("search_event", query3+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for multiMatchQuery on 'apple' keyword: " + drpcResult);
          drpcResult = drpc.execute("search_event", query4+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for boolQuery on must for 'apple' keyword and mustNot for 'iphone' keyword: " + drpcResult);
          drpcResult = drpc.execute("search_event", query5+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for boolQuery on must for 'apple' keyword and must for 'iphone' keyword: " + drpcResult);
          drpcResult = drpc.execute("search_event", query6+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for fuzzyQuery on keyword 'applic' with prefix length of 1 and boost of 1: " + drpcResult);
          drpcResult = drpc.execute("search_event", query7+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for matchAllQuery: " + drpcResult);
          drpcResult = drpc.execute("search_event", query8+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for prefixQuery for keyword 'app' keyword with boost of 2: " + drpcResult);
          drpcResult = drpc.execute("search_event", query9+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for wildcardQuery for regular expression 'a??l*': " + drpcResult);
          drpcResult = drpc.execute("search_event", query10+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT for boostingQuery for postive:'apple' and negative 'iphone' with ngative boost as 0.2" + drpcResult);
          Thread.sleep(4000);
       }
        System.out.println("STATUS: OK");
        //cluster.shutdown();
        //drpc.shutdown();
		//esSetup.terminate(); 
    }
}
