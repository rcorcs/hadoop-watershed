package sample.knn;

//import java.io.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Comparator;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;

import hws.core.Filter;
//import hws.util.Json;

public class CompareFilter extends Filter{
	private int k;
	private List<Entry<Integer, Vector2D>> training;
	private List<Entry<Integer, Vector2D>> samples;
	private boolean finishedTraining;

        private final Lock lock = new ReentrantLock();

        //private PrintWriter out;

	public void start(){
		super.start();
/*      try{ 
         out = new PrintWriter(new BufferedWriter(new FileWriter("/home/yarn/rcor/yarn/filter-"+name()+".out"))); 
         out.println("Starting Filter: "+name()+" instance "+instanceId()); 
         out.flush(); 
      }catch(IOException e){ 
         e.printStackTrace(); 
      } */
		this.samples = new ArrayList<Entry<Integer, Vector2D>>();
		this.training = new ArrayList<Entry<Integer, Vector2D>>();
		this.k = Integer.parseInt(super.attribute("k"));
		//this.finishedTraining = false;
	}

	public void finish(){
                //out.println("Finishing...");
                //out.flush();
		for(Entry<Integer, Vector2D> data : this.samples){
                       Entry<Integer, List<Entry<Integer, Double>>> outData = new SimpleEntry<Integer, List<Entry<Integer, Double>>>(data.getKey(), classify(data.getValue()));
                       //out.println("sending : "+outData.toString());
                       //out.flush();
                       send(outData);
                       //out.println("data Sent!");
                       //out.flush();
                }
                this.samples.clear();
		this.training.clear();
                //out.println("Finished...");
                //out.flush();


		super.finish();

                //out.println("Finishing Filter: "+name()+" instance "+instanceId());
                //out.flush();
                //out.close();

	}

        public Vector2D parseTest(String str){
           String []split = str.trim().split(" ");
           return new Vector2D(Double.parseDouble(split[0]), Double.parseDouble(split[1]));
        }

        public Entry<Integer, Vector2D> parseTrain(String str){
           String []split = str.trim().split(" ");
           return new SimpleEntry<Integer, Vector2D>(new Integer(split[2]), new Vector2D(Double.parseDouble(split[0]), Double.parseDouble(split[1])));
        }

	public void process(String src, Object obj){
             //out.println("processing "+src+" : "+obj.toString());
             //out.flush();

           Entry<Integer, String> data = (Entry<Integer, String>)obj;
           if(this.training==null || this.samples==null){
			//Logger.info("LOSING DATA: instance not initialized yet");
		}else if(data==null || data.getValue()==null || data.getValue().trim().length()==0){
			//Logger.info("EMPTY DATA");
		}else if("Training".equals(src)){
			//Logger.info("process: "+Json.dumps(data));
                        try{ lock.lock();
  			   this.training.add(parseTrain(data.getValue()));
                        }finally{
                           lock.unlock();
                        }
		}else if("Samples".equals(src)){
			//Logger.info("process: "+Json.dumps(data));
			//if(!this.finishedTraining){
                        try{ lock.lock();
				this.samples.add( new SimpleEntry<Integer, Vector2D>(data.getKey(), parseTest(data.getValue())) );
                        }finally{
                           lock.unlock();
                        }
			/*}else{
				//Logger.info("Classifying sample:");
				SimpleEntry<Integer, String> outData = new SimpleEntry<Integer, String>(data.getKey(), classify(data.getValue()));
				send(outData);
			}*/
		}
	}

	public void onChannelHalt(String channelName){
	}

	public void onChannelsHalted(){
	}

	public void send(Object obj){
		for(String channel: outputChannels()){
                        /*out.println("Sending to channel: "+channel);
                        out.flush();
                        if(outputChannel(channel)==null){
                           out.println("channel is NULL");
                           out.flush();
                        }*/
			outputChannel(channel).send(obj);
		}
	}

	public List<Entry<Integer, Double>> classify(Vector2D data){
		List<Entry<Integer, Double>> neighbors = new ArrayList<Entry<Integer, Double>>();
		for(Entry<Integer, Vector2D> trainData: this.training){
			double distance = Vector2D.distance(data, trainData.getValue());
			int category = trainData.getKey();
			neighbors.add(new SimpleEntry<Integer, Double>(new Integer(category), new Double(distance)));
		}
		
		Collections.sort(neighbors, new Comparator() {
			  public int compare(Object o1, Object o2) {

			      Double x1 = ((Entry<Integer, Double>)o1).getValue();
			      Double x2 = ((Entry<Integer, Double>)o2).getValue();

			      return x1.compareTo(x2);
			  }
		 });

		List<Entry<Integer, Double>> nearneighbors = new ArrayList<Entry<Integer, Double>>();
		for(int ik = 0; ik<k; ik++){
			nearneighbors.add(neighbors.get(ik));
		}
                return nearneighbors;
		//String strOut = Json.dumps(nearneihbors);
		//return strOut;
	}
}
