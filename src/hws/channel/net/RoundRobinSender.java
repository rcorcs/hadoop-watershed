package hws.channel.net;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import hws.net.NodeCommunicator;

public class RoundRobinSender extends NetSender<String> {
	private int nextComm = 0;
	
	public void send(String data){
		NodeCommunicator comm = getCommunicator(nextComm);
		if(comm!=null){
			try{
				comm.writeLine(data);
				comm.flush();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		nextComm++;
		if(nextComm>=numConsumerInstances()) nextComm = 0;
	}
}
