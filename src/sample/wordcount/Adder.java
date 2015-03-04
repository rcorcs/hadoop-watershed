/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sample.wordcount;

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

public class Adder extends Filter{
   private PrintWriter out;
   private Map<String, Integer> counts;
   private final Lock lock = new ReentrantLock();

   public void start(){
      super.start();
      Logger.info("Starting Filter: "+name()+" instance "+instanceId());
      counts = new ConcurrentHashMap<String, Integer>();
   }

   public void finish(){
      for(String key: counts.keySet()){
         for(String channelName: outputChannels()){
            outputChannel(channelName).send(key+"="+counts.get(key).toString());
         }
      }

      super.finish();
      //try{
      Logger.info("Finishing Filter: "+name()+" instance "+instanceId());
      /*}catch(IOException e){
        e.printStackTrace();
        }*/
   }

   public void process(String src, Object obj){
     Entry<String, String> data = (Entry<String, String>)obj;
//   public void process(String src, Object obj){
//      SimpleEntry<String, String> data = (SimpleEntry<String, String>)obj;
      String word = data.getKey().toLowerCase();
      int val = Integer.parseInt(data.getValue());
      try{lock.lock();
      if(counts.containsKey(word)){
         counts.put(word, new Integer(val+counts.get(word).intValue()));
      }else {
         counts.put(word, new Integer(val));
      }
      }finally{lock.unlock();}
   }
   public void onChannelHalt(String channelName){
      Logger.info("Input Channel Halted: "+channelName);
   }
   public void onChannelsHalted(){}
}
