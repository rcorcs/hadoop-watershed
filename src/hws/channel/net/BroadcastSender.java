package hws.channel.net;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.codec.binary.Base64;

import org.apache.zookeeper.KeeperException;

import hws.net.NodeCommunicator;

public class BroadcastSender extends NetSender {
   public void send(Object obj){
      //Logger.info("Sending: "+data);
      for(int i = 0; i<numConsumerInstances(); i++){
         NodeCommunicator comm = getCommunicator(i);
         if(comm!=null){
            try{
               Serializable data = (Serializable)obj;
               byte[] dataBytes = SerializationUtils.serialize(data);
               String dataBase64 = Base64.encodeBase64String(dataBytes).replaceAll("\\s","");
               comm.writeLine(dataBase64);
               comm.flush();
            }catch(IOException e){
               e.printStackTrace();
            }
         }
      }
   }
}
