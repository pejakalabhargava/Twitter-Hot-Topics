/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.starter.trident.project.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
//import backtype.storm.spout.SpoutOutputCollector;

@SuppressWarnings("serial")
public class TwitterSampleSpout implements IBatchSpout {

	// Trident batch size
    private static final int RECORDS_PER_BATCH = 10;

	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	
	public TwitterSampleSpout(String consumerKey, 
				  String consumerSecret,
				  String accessToken, 
				  String accessTokenSecret, 
				  String[] keyWords) 
	{
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
	}

	public TwitterSampleSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Map conf, TopologyContext context) {
		//Queue to hold the tweets that are streamed from twitter.Size of queue us 2000
		queue = new LinkedBlockingQueue<Status>(2000);

		//Add tweets to the queue when it is received through callback
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		//Create twiter stream
		TwitterStream twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		//If filter is not present , get all tweets
		if (keyWords == null) {
			twitterStream.sample();
		}
		//apply filter for US english and keywords supplied
		else {
			String[] lang = {"en"};
			FilterQuery query = new FilterQuery().language(lang).track(keyWords);
			twitterStream.filter(query);
		}

	}


	@Override
    /**
     * Used to emit the tweets batch to trident
     */
	public void emitBatch( long batchId, TridentCollector collector ) {
    	int emitted=0;
    	while(emitted<RECORDS_PER_BATCH) {
    		//if no contents on top of queue sleep
    		Status ret = queue.poll();
			if (ret == null) {
				Utils.sleep(50);
			} else {
				//return the value of the tweet stored
				collector.emit(new Values(ret));
				emitted++;
			}
    	}


    }


	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(long id) {
		//System.out.println("Twitter spout ack = "+id.toString());
	}

	//@Override
	public void fail(Object id) {
		//System.out.println("Twitter spout fail = "+id.toString());
	}

	@Override
	public Fields getOutputFields() {
		return (new Fields("tweet"));
	}
	

}