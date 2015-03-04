package hws.channel.net;

import java.io.IOException;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.util.concurrent.CountDownLatch;

import java.util.AbstractMap.SimpleEntry;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.codec.binary.Base64;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import org.apache.zookeeper.KeeperException;

import hws.util.Json;
import hws.util.Logger;

import hws.net.MessageHandler;

import hws.core.ChannelDeliver;


class MessageDeliver extends MessageHandler {
   private ChannelDeliver deliver;

   public MessageDeliver(ChannelDeliver deliver){
      this.deliver = deliver;
   }

   public void handleMessage(String msg){
      //SimpleEntry<String, String> data = Json.loads(msg, new TypeToken< SimpleEntry<String, String> >() {}.getType());
      //out.println("received: "+msg);
      byte[] dataBytes = Base64.decodeBase64(msg);
      Object data = (Object)SerializationUtils.deserialize(dataBytes);
      //out.println("data received: "+data.toString());
      this.deliver.deliver(data);
   }
}

public class NetDeliver extends ChannelDeliver {
   private ExecutorService serverExecutor;
   private ServerSocket server;
   private CountDownLatch latch;

   public void start(){
      super.start();

      Logger.info("Starting channel deliver: "+channelName()+" instance "+instanceId());

      this.latch = new CountDownLatch(1);
      serverExecutor = Executors.newCachedThreadPool();
      try{
         Logger.info("Binding to a listening port");
         server = new ServerSocket(0);
      }catch(IOException e){
         Logger.severe(e.toString());
      }

      int port = server.getLocalPort();
      Logger.info("Connected to port: "+port);
      try{
         Logger.info("Host: "+hws.net.NetUtil.getLocalCanonicalHostName());
         shared().set("host-"+instanceId(), hws.net.NetUtil.getLocalCanonicalHostName());
         shared().set("port-"+instanceId(), new Integer(port));
      }catch(UnknownHostException e){
         Logger.severe(e.toString());
      }
      Logger.info("Running server, waiting for a close command");
      while(this.latch.getCount()>0){
         try{
            MessageDeliver handler = new MessageDeliver(this);
            handler.setSocket(this.server.accept());
            serverExecutor.execute( handler );
         }catch(IOException e){
            e.printStackTrace();
         }
      }
   }

   public void finish(){
      Logger.info("Finishing NetDeliver");
      try {
         this.latch.await(); //await server channel to be closed
      }catch(InterruptedException e){
         Logger.severe("Waiting ERROR: "+e.getMessage());
      }
      super.finish();
   }

   public void onProducersHalted(){
      Logger.info("Closing server channel");
      this.latch.countDown();
      try{
         this.server.close();
      }catch(IOException e){
         Logger.warning(e.toString());
      }

      halt();
      Logger.info("Closing command completed");
   }
}
