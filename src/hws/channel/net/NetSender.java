package hws.channel.net;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import hws.net.NodeCommunicator;

import hws.core.ChannelSender;

public abstract class NetSender<DataType> extends ChannelSender<DataType> {
  private NodeCommunicator []comm = null;

  public void start(){
    super.start();
    comm = new NodeCommunicator[numConsumerInstances()];
    for(int id = 0; id<numConsumerInstances(); id++){
      String host = shared().wait("host-"+id);
      //out.println("connect to Host: "+host);
      //out.flush();
      Integer port = shared().wait("port-"+id);
      //out.println("connect to Port: "+port);
      //out.flush();
      try{
        //out.println("Connecting to server id: "+id);
        //out.flush();
        comm[id] = connectToServer(host, port.intValue());
        //out.println("Connection established");
        //out.flush();
        //if(comm[id]==null){out.println("Error, channel is null");out.flush();}
      }catch(Exception e){
        comm[id] = null;
      }
    }
  }

  public void finish(){
    super.finish();
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
