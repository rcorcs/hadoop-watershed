package sample.wordcount;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.AbstractMap.SimpleEntry;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import hws.core.ChannelEncoder;
import hws.util.Logger;

public class Combiner extends ChannelEncoder{
	private Map<String, Integer> counts;
        private final Lock lock = new ReentrantLock();

	public void start(){
		super.start();
		Logger.info("Starting Combiner");
		counts = new ConcurrentHashMap<String, Integer>();
	}

	public void finish(){
		super.finish();
                Logger.info("Finishing Combiner");
		for(String key: counts.keySet()){
			channelSender().send(new SimpleEntry<String,String>(key, counts.get(key).toString()));
		}
	}

	public void encode(Object obj){
		SimpleEntry<String,String> data = (SimpleEntry<String,String>)obj;
		if(counts!=null){
  		  String word = data.getKey().toLowerCase();
 		  int val = Integer.parseInt(data.getValue());
                  try{ lock.lock();

		    if(counts.containsKey(word)){
			counts.put(word, new Integer(val+counts.get(word).intValue()));
		    }else {
			counts.put(word, new Integer(val));
		    }
                  }finally{lock.unlock();}
		}
	}
}
