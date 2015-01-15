package hws.channel.net;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

/*import cloudos.kernel.Global;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.NodeCommunicator;
import cloudos.kernel.SystemCallInterface;

import cloudos.util.Logger;
import cloudos.util.Json;

import watershed.core.InstanceAddress;
import watershed.core.ChannelSender;

import watershed.core.ControlMessageReceiver;
import watershed.core.ControlMessage;
*/
import hws.net.NodeCommunicator;

public class BroadcastSender extends NetSender<String> {
	public void send(String data){
		//Logger.info("Sending: "+data);
		for(int i = 0; i<numConsumerInstances(); i++){
			NodeCommunicator comm = getCommunicator(i);
			if(comm!=null){
				try{
					comm.writeLine(data);
					comm.flush();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}
	}
}
