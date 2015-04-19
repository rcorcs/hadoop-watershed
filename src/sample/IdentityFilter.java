package sample;

import java.io.*;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.AbstractMap.SimpleEntry;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import hws.core.Filter;
import hws.util.Json;
import hws.util.Logger;

public class IdentityFilter extends Filter{
   public void start(){
      super.start();
      Logger.info("Starting Filter: "+name()+" instance "+instanceId());
   }

   public void finish(){
      super.finish();
      Logger.info("Finishing Filter: "+name()+" instance "+instanceId());
   }

   public void process(String src, Object obj){
	for(String channelName: outputChannels()){
            outputChannel(channelName).send(obj);
         }
   }
   public void onChannelHalt(String channelName){
      Logger.info("Input Channel Halted: "+channelName);
   }
   public void onChannelsHalted(){}
}
