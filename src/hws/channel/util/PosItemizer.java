package hws.channel.util;

import java.util.AbstractMap.SimpleEntry;

import hws.core.ChannelDecoder;
import hws.util.Logger;

public class PosItemizer extends ChannelDecoder{
	private int id;
	public void start(){
		super.start();
		id = 0;
                Logger.info("Starting PosItemizer");
	}
	public void finish(){
		super.finish();
                Logger.info("Finishing PosItemizer");
	}

	public void decode(Object obj){
		deliver(new SimpleEntry<Integer, Object>(new Integer(id), obj));
		id++;
	}
}
