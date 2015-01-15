package hws.channel.net;

import java.io.IOException;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.codec.binary.Base64;

import org.apache.zookeeper.KeeperException;

import hws.net.NodeCommunicator;

public class RoundRobinSender extends NetSender<String> {
   private int nextComm = 0;

   public void send(String data){
      NodeCommunicator comm = getCommunicator(nextComm);
      if(comm!=null){
         try{
            byte[] dataBytes = SerializationUtils.serialize(data);
            String dataBase64 = Base64.encodeBase64String(dataBytes).replaceAll("\\s","");
            comm.writeLine(dataBase64);
            comm.flush();
         }catch(IOException e){
            e.printStackTrace();
         }
      }
      nextComm++;
      if(nextComm>=numConsumerInstances()) nextComm = 0;
   }
}
