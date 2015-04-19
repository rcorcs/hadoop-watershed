package hws.channel.net;

import java.io.IOException;
import java.io.Serializable;

import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.codec.binary.Base64;

import hws.net.NodeCommunicator;
import hws.util.Json;
import hws.util.Logger;

public class KeyValueSender extends NetSender {
   public void start(){
      super.start();
      Logger.info("Starting channel sender: "+channelName()+" instance "+instanceId());
   }
   public void finish(){
      Logger.info("Finishing channel sender: "+channelName()+" instance "+instanceId());
      super.finish();
   }
   public void send(Object obj){
      int key;
      //out.println("Channel sending: "+Json.dumps(obj));
      //out.flush();
      if(obj instanceof Entry){
         //out.println("Is Entry: "+((Entry<?,?>)obj).getKey().toString());
         //out.flush();
         key = ((Entry<?,?>)obj).getKey().hashCode()%numConsumerInstances();
      }else {
         //out.println("Is NOT Entry");
         //out.flush();
         key = obj.hashCode()%numConsumerInstances();
      }
      if(key<0){
         key = Math.abs(key)%numConsumerInstances();
      }
      NodeCommunicator comm = getCommunicator(key);
      if(comm!=null){
         //boolean retry = false;
         //do{
         try{
            //if(retry){
            //   comm.reconnect();
            //   retry = false;
            //}
            //String json = Json.dumps(data);
            Serializable data = (Serializable)obj;
            byte[] dataBytes = SerializationUtils.serialize(data);
            String dataBase64 = Base64.encodeBase64String(dataBytes).replaceAll("\\s","");
            comm.writeLine(dataBase64);
            //Logger.info("sending: "+key+": "+json);
            //comm.flush();
         }catch(IOException e){
            Logger.warning(e.toString());
            //retry = true;
         }
         //}while(retry);
      }
   }
}
