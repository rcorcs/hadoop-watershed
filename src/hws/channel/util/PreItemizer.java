package hws.channel.util;

import java.util.AbstractMap.SimpleEntry;

import hws.core.ChannelEncoder;
import hws.util.Logger;

public class PreItemizer extends ChannelEncoder{
	private int id;
	public void start(){
		super.start();
		id = 0;
                Logger.info("Starting PreItemizer");
	}
	public void finish(){
		super.finish();
                Logger.info("Finishing PreItemizer");
	}

	public void encode(Object obj){
		send(new SimpleEntry<Integer, Object>(new Integer(id), obj));
		id++;
	}
}
