/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.starter.trident.project.countmin.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import storm.starter.trident.project.functions.Tweet;
import storm.trident.state.State;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.io.Files;

/**
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 * Modified by Preetham MS. Originally by https://github.com/addthis/stream-lib/
 * Modified by Bhargava kakannaya
 * @author: Preetham MS (pmahish@ncsu.edu)
 */
public class CountMinSketchState implements State {

    public static final long PRIME_MODULUS = (1L << 31) - 1;

    int depth;
    int width;
    long[][] table;
    long[] hashA;
    long size;
    double eps;
    double confidence;
	//This holds the stop words that needs to be ignored before storing into the count min
    BloomFilter<String> stopWordsBloomFilter;
    //This will fetch top 5 words with respect to its frequency
    int k=5;
    //This queue is used to implment the Max prioirty queue to store the top 5 words in the queue 
	MinMaxPriorityQueue<Tweet> topkQueue;
	
	
	CountMinSketchState() {
    }

    public CountMinSketchState(int depth, int width, int seed) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        initTablesWith(depth, width, seed);
        initialize();
    }

    public CountMinSketchState(int depth, int width, int seed,int k) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        initTablesWith(depth, width, seed);
        this.k = k;
        initialize();
     }
    /**
     * Stores all the stop words related membership information in the BloomFilter
     */
	private void initializeBloomFilter() {
		//Create a String funnel to put string into the funnel
		Funnel<String> stringFunnel = new Funnel<String>() {

			@Override
			public void funnel(String stopwordString, PrimitiveSink into) {
				into.putString(stopwordString, Charsets.UTF_8);
			}
		};
		//create a BloomFilter of 500 elements with false positive rate of 0.01
		stopWordsBloomFilter = BloomFilter.create(stringFunnel, 500, 0.01);
		ImmutableList<String> lines = null;
		//Read contents from stop words
		try {
			lines = Files.asCharSource(new File("data/stop-words_english.txt"),
					Charsets.UTF_8).readLines();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Put the stop words into the bloom filter 
		for (String line : lines) {
			stopWordsBloomFilter.put(line);
			// System.out.println("BloomFilter:" + line);
		}
	}
	
	/**
	 * This method creates a priority queue and initilizes the bloom filter
	 */
	private void initialize() {
		//Create queue of size maximum size 5
		topkQueue =MinMaxPriorityQueue.maximumSize(k).create();
		initializeBloomFilter();
	}


	public CountMinSketchState(double epsOfTotalCount, double confidence, int seed) {
        // 2/w = eps ; w = 2/eps
        // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(2 / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        initTablesWith(depth, width, seed);
    }

    public CountMinSketchState(int depth, int width, int size, long[] hashA, long[][] table) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.hashA = hashA;
        this.table = table;
        this.size = size;
    }

    private void initTablesWith(int depth, int width, int seed) {
        this.table = new long[depth][width];
        this.hashA = new long[depth];
        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

    public double getRelativeError() {
        return eps;
    }

    public double getConfidence() {
        return confidence;
    }

    int hash(long item, int i) {
        long hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
        return ((int) hash) % width;
    }

    
    public void add(long item, long count) {
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        for (int i = 0; i < depth; ++i) {
            table[i][hash(item, i)] += count;
        }
        size += count;
    }

    /**
     * Method used to add an item to the count min.While adding an item it has to 
     * make sure that the item being added is not one of the stop words. Also
     * it has to modify the priority queue such that it holds the top 5 words
     * with respect to the frequency count
     * @param item
     * @param count
     */
    public void add(String item, long count) {
    	
    	//Check if bloomFilter contains the stop word.if it does
    	//do not add the item to the queue and count min
    	if(stopWordsBloomFilter.mightContain(item.toLowerCase())){
        	return;
        }
    	
    	
    	if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
       //get the count of word from the count min
    	int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            table[i][buckets[i]] += count;
        }
        size += count;
        //Create a tweet object representing the count and the word
        Tweet tweet = new Tweet(item,(int)estimateCount(item));
        //Access the top queue in a synchronous way and of the tweet is already present in it
        //then update it.This addition will take care that only top 5 elements stay on the priority 
        //queue. Make sure Tweet object is comparable
        synchronized (topkQueue) {
        	 if(topkQueue.contains(tweet)){
             	topkQueue.remove(tweet);
             }
             topkQueue.add(tweet);
		}
    }

    
    public long size() {
        return size;
    }

    /**
     * The estimate is correct within 'epsilon' * (total item count),
     * with probability 'confidence'.
     */
    
    public long estimateCount(long item) {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    
    public long estimateCount(String item) {
    	//If the item being queried is a stop word ,then return 0 so that
    	//stop words are not returned with a count> 0 since false positives are
    	//possible
    	if(stopWordsBloomFilter.mightContain(item.toLowerCase())){
        	return 0;
        }
    	
    	long res = Long.MAX_VALUE;
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }

    /**
     * Merges count min sketches to produce a count min sketch for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws CMSMergeException if estimators are not mergeable (same depth, width and seed)
     */
    public static CountMinSketchState merge(CountMinSketchState... estimators) throws CMSMergeException {
        CountMinSketchState merged = null;
        if (estimators != null && estimators.length > 0) {
            int depth = estimators[0].depth;
            int width = estimators[0].width;
            long[] hashA = Arrays.copyOf(estimators[0].hashA, estimators[0].hashA.length);

            long[][] table = new long[depth][width];
            int size = 0;

            for (CountMinSketchState estimator : estimators) {
                if (estimator.depth != depth) {
                    throw new CMSMergeException("Cannot merge estimators of different depth");
                }
                if (estimator.width != width) {
                    throw new CMSMergeException("Cannot merge estimators of different width");
                }
                if (!Arrays.equals(estimator.hashA, hashA)) {
                    throw new CMSMergeException("Cannot merge estimators of different seed");
                }

                for (int i = 0; i < table.length; i++) {
                    for (int j = 0; j < table[i].length; j++) {
                        table[i][j] += estimator.table[i][j];
                    }
                }
                size += estimator.size;
            }

            merged = new CountMinSketchState(depth, width, size, hashA, table);
        }

        return merged;
    }

    public static byte[] serialize(CountMinSketchState sketch) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream s = new DataOutputStream(bos);
        try {
            s.writeLong(sketch.size);
            s.writeInt(sketch.depth);
            s.writeInt(sketch.width);
            for (int i = 0; i < sketch.depth; ++i) {
                s.writeLong(sketch.hashA[i]);
                for (int j = 0; j < sketch.width; ++j) {
                    s.writeLong(sketch.table[i][j]);
                }
            }
            return bos.toByteArray();
        } catch (IOException e) {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }

    public static CountMinSketchState deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream s = new DataInputStream(bis);
        try {
            CountMinSketchState sketch = new CountMinSketchState();
            sketch.size = s.readLong();
            sketch.depth = s.readInt();
            sketch.width = s.readInt();
            sketch.eps = 2.0 / sketch.width;
            sketch.confidence = 1 - 1 / Math.pow(2, sketch.depth);
            sketch.hashA = new long[sketch.depth];
            sketch.table = new long[sketch.depth][sketch.width];
            for (int i = 0; i < sketch.depth; ++i) {
                sketch.hashA[i] = s.readLong();
                for (int j = 0; j < sketch.width; ++j) {
                    sketch.table[i][j] = s.readLong();
                }
            }
            return sketch;
        } catch (IOException e) {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginCommit(Long txid) {
        return;
    }

    @Override
    public void commit(Long txid) {
        return;
    }

    @SuppressWarnings("serial")
    protected static class CMSMergeException extends RuntimeException {
   // protected static class CMSMergeException extends FrequencyMergeException {

        public CMSMergeException(String message) {
            super(message);
        }
    }

    /**
     * Returns top k words as string from the priority queue
     * @return top k words as string
     */
	public String getTopKAsString() {
		StringBuilder sb = new StringBuilder();
		//Access the queue and form a string of top 5 words
		if(topkQueue.isEmpty()) {
			return "";
		}
		//Make it synchronized to make sure there is no ConcurrentModification exception
		synchronized (topkQueue) {
			Iterator<Tweet> iter = topkQueue.iterator();
			while(iter.hasNext()) {
				Tweet t = iter.next();
				sb.append(t.getText());
				sb.append(":");
				sb.append(t.getCount());
				sb.append("|");
			}
		}
		return sb.toString();
	}
}
