package hws.channel.net;

import java.io.*; //DEBUG
import java.io.IOException;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.AbstractMap.SimpleEntry;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import org.apache.zookeeper.KeeperException;

import hws.net.MessageHandler;

import hws.core.ChannelDeliver;
import hws.core.ChannelReceiver;

import hws.util.Json;

class KeyValueMessageDeliver extends MessageHandler {
	private ChannelDeliver<SimpleEntry<String, String>> deliver;

	public KeyValueMessageDeliver(ChannelDeliver<SimpleEntry<String, String>> deliver){
		this.deliver = deliver;
	}
	public void handleMessage(String msg){
		//Logger.info("Received: "+msg);
		SimpleEntry<String, String> data = Json.loads(msg, new TypeToken< SimpleEntry<String, String> >() {}.getType());
		//Logger.info("Delivering: "+data.toString());
		this.deliver.deliver(data);
	}
}

public class KeyValueDeliver extends ChannelDeliver<SimpleEntry<String, String>>{
    private PrintWriter out;
	private ExecutorService serverExecutor;
	private ServerSocket server;

	public void start(){
		super.start();
        
        try{
           out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/channel-deliver-"+channelName()+".out")));
           out.println("Starting channel deliver: "+channelName()+" instance "+instanceId());
           out.flush();
        }catch(IOException e){
           e.printStackTrace();
        }

		serverExecutor = Executors.newCachedThreadPool();
		try{
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

		while(true){
			try{
				KeyValueMessageDeliver handler = new KeyValueMessageDeliver(this);
				handler.setSocket(this.server.accept());
				serverExecutor.execute( handler );
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}

	public void finish(){
		super.finish();
        out.println("Finishing channel deliver: "+channelName()+" instance "+instanceId());
        out.close();
	}

    public void onProducersHalted(){
       try{
       server.close();
       }catch(IOException e){
         e.printStackTrace();
       }
       serverExecutor.shutdown();
       halt();
    }
    /*
	public void receiveCtrlMsg(ControlMessage ctrlMsg){
		Logger.info("NETDELIVER: received ctrl msg: "+Json.dumps(ctrlMsg));
		if("InstanceAddress".equals(ctrlMsg.getMessage())){
			NodeAddress nodeAddr = new NodeAddress(Global.getLocalNodeInfo(), this.server.getLocalPort());
			InstanceAddress addr = new InstanceAddress(nodeAddr, getInstance());
			ControlMessage replyCtrlMsg = new ControlMessage(ctrlMsg.getDestination(), ctrlMsg.getSource(), Json.dumps(addr));
			try{
				getSystemCallInterface().request("watershed","sendCtrlMsg", replyCtrlMsg);
			}catch(Exception e){
				e.printStackTrace();
			}
		}else if("ProducersHalted".equals(ctrlMsg.getMessage())){
			//TODO evaluate this awaiting mechanism
			//serverExecutor.shutdown();
			halt();
		}
	}*/
}
