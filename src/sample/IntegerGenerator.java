package sample;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedInputStream;

import hws.core.ChannelDeliver;
import hws.util.Logger;

public class IntegerGenerator extends ChannelDeliver{
	
	public void start(){
		super.start();
                Logger.info("Starting channel deliver: "+channelName()+" instance "+instanceId());
		int n = Integer.parseInt(attribute("n"));
		for(int i = 1; i<=n; i++){
			deliver(new Integer(i));
		}
	}

   public void finish(){
      super.finish();
      Logger.info("Finishing channel deliver: "+channelName()+" instance "+instanceId());
   }

   public void onProducersHalted(){
      Logger.info("PRODUCERS HALTED!!");
   }
}
