package hws.channel.net;

import java.io.*; //TODO debug
import java.io.IOException;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.codec.binary.Base64;

import org.apache.zookeeper.KeeperException;

import hws.util.Json;

import hws.net.MessageHandler;

import hws.core.ChannelDeliver;


class MessageDeliver extends MessageHandler {
  private ChannelDeliver<String> deliver;

  public MessageDeliver(ChannelDeliver<String> deliver){
    this.deliver = deliver;
  }

  public void handleMessage(String msg){
    byte[] dataBytes = Base64.decodeBase64(msg);
    //SimpleEntry<String, String> data = (SimpleEntry<String, String>)SerializationUtils.deserialize(dataBytes);
    this.deliver.deliver((String)SerializationUtils.deserialize(dataBytes));
  }
}

public class NetDeliver extends ChannelDeliver<String> {
  private PrintWriter out;

  private ExecutorService serverExecutor;
  private ServerSocket server;
  private CountDownLatch latch;

  public void start(){
    super.start();

    try{
       out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/channel-deliver-"+channelName()+".out")));
       out.println("Starting channel deliver: "+channelName()+" instance "+instanceId());
       out.flush();
    }catch(IOException e){
       e.printStackTrace();
    }

    this.latch = new CountDownLatch(1);
    serverExecutor = Executors.newCachedThreadPool();
    try{
      out.println("Binding to a listening port");
      out.flush();
      server = new ServerSocket(0);
    }catch(IOException e){
      e.printStackTrace();
    }
    
    int port = server.getLocalPort();
    out.println("Connected to port: "+port);
    out.flush();
    try{
       out.println("Host: "+hws.net.NetUtil.getLocalCanonicalHostName());
       out.flush();
       shared().set("host-"+instanceId(), hws.net.NetUtil.getLocalCanonicalHostName());
       shared().set("port-"+instanceId(), new Integer(port));
    }catch(UnknownHostException e){
       e.printStackTrace();
    }
    out.println("Running server, waiting for a close command");
    out.flush();
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
    try {
      this.latch.await(); //await server channel to be closed
    }catch(InterruptedException e){
      // handle
      out.println("Waiting ERROR: "+e.getMessage());
      out.flush();
    }
    out.close();
    super.finish();
  }

  public void onProducersHalted(){
    out.println("Closing server channel");
    out.flush();
    this.latch.countDown();
    try{
    this.server.close();
    }catch(IOException e){
       e.printStackTrace();
    }
    
    halt();
    out.println("Closing command completed");
    out.flush();
  }
}
