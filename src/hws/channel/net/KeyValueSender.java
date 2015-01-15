package hws.channel.net;

import java.io.IOException;

import java.util.AbstractMap.SimpleEntry;

import org.apache.zookeeper.KeeperException;

import hws.net.NodeCommunicator;
import hws.util.Json;

public class KeyValueSender extends NetSender<SimpleEntry<String, String>> {

	public void send(SimpleEntry<String, String> data){
		int key = ((String)data.getKey()).hashCode()%numConsumerInstances();
		NodeCommunicator comm = getCommunicator(key);
		if(comm!=null){
			try{
				String json = Json.dumps(data);
				comm.writeLine(json);
				//Logger.info("sending: "+key+": "+json);
				//comm.flush();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
}
