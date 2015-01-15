package hws.channel.net;

import java.io.*;
import java.io.IOException;

import java.util.AbstractMap.SimpleEntry;

import hws.net.NodeCommunicator;
import hws.util.Json;

public class KeyValueSender extends NetSender<SimpleEntry<String, String>> {
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
   public void send(SimpleEntry<String, String> data){
      int key = ((String)data.getKey()).hashCode()%numConsumerInstances();
      out.println("Channel sending: "+Json.dumps(data)+" to "+key);
      out.flush();
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
