package sample.knn;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;

import hws.core.Filter;
//import hws.util.Json;

public class MergeFilter extends Filter{
	private int k;
	Map<Integer, List<Entry<Integer, Double>>> candidates;
        private final Lock lock = new ReentrantLock();

	public void start(){
		super.start();
		this.k = Integer.parseInt(attribute("k"));
		candidates = new ConcurrentHashMap<Integer, List<Entry<Integer, Double>>>();
	}

	public void finish(){
		super.finish();
		
		//Logger.info("FINISHING: ");
		//Logger.info("Candidates: "+candidates.keySet().size());
		for(Integer itemId: candidates.keySet()){
			List<Entry<Integer, Double>> neighbors = candidates.get(itemId);

			//Logger.info("neighbors: "+neighbors.size());
			Collections.sort(neighbors, new Comparator() {
				  public int compare(Object o1, Object o2) {

					   Double x1 = ((Entry<Integer, Double>)o1).getValue();
					   Double x2 = ((Entry<Integer, Double>)o2).getValue();

					   return x1.compareTo(x2);
				  }
			});

			//Logger.info("neighbors: "+neighbors.size()+" k: "+this.k);

			Map<Integer, Integer> categories = new ConcurrentHashMap<Integer, Integer>();
			int ik = 0;
			for(Entry<Integer, Double> pair: neighbors){
				if(ik>=this.k) break;
				ik++;
				//Logger.info("ik: "+ik+" pair: "+Json.dumps(pair));

				Integer nKey = pair.getKey();
				if(!categories.containsKey(nKey)){
					categories.put(nKey, new Integer(1));
				}else{
					categories.put(nKey, new Integer(categories.get(nKey).intValue() + 1));
				}
			}
			//Logger.info("categories: "+Json.dumps(categories));
		
			int maxCat = -1;
			int maxFreq = -1;
			for(Integer key: categories.keySet()){
				if(categories.get(key).intValue()>maxFreq){
					maxFreq = categories.get(key).intValue();
					maxCat = key.intValue();
				}
			}

			send(itemId.toString()+" "+maxCat);
		}
	}

	public void process(String src, Object obj){
                Entry<Integer, List<Entry<Integer, Double>>> data =  (Entry<Integer, List<Entry<Integer, Double>>>)obj;
                //Entry<String, String> data = (Entry<String, String>)obj;
		if(data!=null && data.getValue()!=null){
			//List<Entry<Integer, Double>> nearneihbors = Json.loads(data.getValue(), new TypeToken< List<Entry<Integer, Double>> >() {}.getType());
			//Logger.info("processing: "+data.getKey()+": "+data.getValue());
                       try{
                       lock.lock();
			  if(!candidates.containsKey(data.getKey())){
				candidates.put(data.getKey(), data.getValue());
			  }else{
				candidates.get(data.getKey()).addAll(data.getValue());
		  	  }
                       }finally{
                         lock.unlock();
                       }
		}
	}

	public void onChannelHalt(String channelName){
		//Logger.info("input channel halted: "+channelName);
	}

	public void onChannelsHalted(){
		//Logger.info("all input channels have halted");
	}

	public void send(String data){
		for(String channel: outputChannels()){
			outputChannel(channel).send(data);
		}
	}
}
