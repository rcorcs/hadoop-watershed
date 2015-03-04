package hws.channel.lfs;

import java.io.*;

import hws.core.ChannelSender;
import hws.util.Logger;

public class LineWriter extends ChannelSender{
	private PrintWriter out;
	public void start(){
		super.start();
                
		String fileName = "/home/yarn/foo";
		if(attribute("file")!=null){
			fileName = attribute("file");
		}
		String path = fileName+"."+instanceId()+".out";
                Logger.info("Starting LineWriter "+channelName());
                Logger.info("Path: "+path);
		try{
			out = new PrintWriter(new BufferedWriter(new FileWriter(path)));
		}catch(IOException e){
			Logger.severe(e.toString());
		}
	}
	public void finish(){
                Logger.info("Finishing LineWriter");
		super.finish();
		out.close();
	}
	public void send(Object data){
		out.println(data.toString());
		out.flush();
	}
}
