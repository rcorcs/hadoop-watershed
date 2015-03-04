package hws.channel.net;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import hws.net.NodeCommunicator;

import hws.core.ChannelSender;

import hws.util.Logger;

public abstract class NetSender extends ChannelSender {
  private NodeCommunicator []comm = null;
  //private PrintWriter out;

  public void start(){
    super.start();
    /*try{
       out = new PrintWriter(new BufferedWriter(new FileWriter("/home/yarn/rcor/yarn/channel-deliver-NetSender.out")));
    }catch(IOException e){
    }*/
    Logger.info("Starting NetSender");

    comm = new NodeCommunicator[numConsumerInstances()];
    for(int id = 0; id<numConsumerInstances(); id++){
      String host = shared().wait("host-"+id);
      while(host==null){ host = shared().wait("host-"+id);}
      Logger.info("connect to Host: "+host);
      //out.flush();
      Integer port = shared().wait("port-"+id);
      while(port==null){ port = shared().wait("port-"+id);}
      Logger.info("connect to Port: "+port);
      //out.flush();
      try{
        //out.println("Connecting to server id: "+id);
        //out.flush();
        comm[id] = connectToServer(host, port.intValue());
        Logger.info("Connection established");
        //out.flush();
        //if(comm[id]==null){out.println("Error, channel is null");out.flush();}
      }catch(Exception e){
        comm[id] = null;
        Logger.severe(e.toString());
      }
    }
  }

  public void finish(){
    super.finish();
    Logger.info("Finishing NetSender");
  }

  protected NodeCommunicator getCommunicator(int index){
    if(index>=0 && comm!=null && index<comm.length){
      return comm[index];
    }else return null;
  }

  private NodeCommunicator connectToServer(final String host, final int port) throws Exception{
    NodeCommunicator comm = new NodeCommunicator(host);
    comm.connect(port);
    return comm;
  }
}
