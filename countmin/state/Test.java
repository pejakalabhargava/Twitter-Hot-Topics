package storm.starter.trident.project.countmin.state;

import java.util.Comparator;
import java.util.PriorityQueue;

import storm.starter.trident.project.functions.Tweet;

public class Test {
public static void main(String[] args){
	PriorityQueue<Tweet> topkQueue = new PriorityQueue<Tweet>(10, new Comparator<Tweet>(){
        public int compare(Tweet o1, Tweet o2){
            return o2.getCount() - o1.getCount();
        }
    });
	topkQueue.add(new Tweet("a",10));
	topkQueue.add(new Tweet("b",30));
	topkQueue.add(new Tweet("c",20));
	topkQueue.add(new Tweet("d",60));
	topkQueue.add(new Tweet("e",80));
	topkQueue.add(new Tweet("f",150));
	topkQueue.add(new Tweet("g",5));
	
	while(!topkQueue.isEmpty()){
		System.out.println(topkQueue.poll());
	}
	
}
}
