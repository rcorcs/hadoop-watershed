package hws.channel.net;

import java.io.*;
import java.io.IOException;
import java.io.Serializable;

import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.codec.binary.Base64;

import hws.net.NodeCommunicator;
import hws.util.Json;

public class KeyValueSender extends NetSender {
   private PrintWriter out;
   public void start(){
      super.start();
      try{
         out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/channel-sender-"+channelName()+".out")));
         out.println("Starting channel sender: "+channelName()+" instance "+instanceId());
         out.flush();
      }catch(IOException e){
         e.printStackTrace();
      }


   }
   public void finish(){
      out.close();
      super.finish();
   }
   public void send(Object obj){
      int key;
      if(obj instanceof Entry){
         key = ((Entry<?,?>)obj).getKey().hashCode()%numConsumerInstances();
      }else key = obj.hashCode()%numConsumerInstances();
      out.println("Channel sending: "+Json.dumps(obj)+" to "+key);
      out.flush();
      NodeCommunicator comm = getCommunicator(key);
      if(comm!=null){
         try{
            //String json = Json.dumps(data);
            Serializable data = (Serializable)obj;
            byte[] dataBytes = SerializationUtils.serialize(data);
            String dataBase64 = Base64.encodeBase64String(dataBytes).replaceAll("\\s","");
            comm.writeLine(dataBase64);
            //Logger.info("sending: "+key+": "+json);
            //comm.flush();
         }catch(IOException e){
            e.printStackTrace();
         }
      }
   }
}
