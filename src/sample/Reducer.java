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

package sample;

import java.io.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.AbstractMap.SimpleEntry;

import hws.core.Filter;
import hws.util.Json;

public class Reducer extends Filter<SimpleEntry<String, String>, String>{
   private PrintWriter out;
   private Map<String, Integer> counts;

   public void start(){
      super.start();
      try{
         out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/filter-"+name()+".out")));
         out.println("Starting Filter: "+name()+" instance "+instanceId());
         out.flush();
      }catch(IOException e){
         e.printStackTrace();
      }
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
      out.println("Finishing Filter: "+name()+" instance "+instanceId());
      out.flush();
      out.close();
      /*}catch(IOException e){
        e.printStackTrace();
        }*/
   }
   public void process(String src, SimpleEntry<String, String> data){
      out.println("processing "+src+" : "+Json.dumps(data));
      out.flush();
      String word = data.getKey().toLowerCase();
      int val = Integer.parseInt(data.getValue());
      if(counts.containsKey(word)){
         counts.put(word, new Integer(val+counts.get(word).intValue()));
      }else {
         counts.put(word, new Integer(val));
      }
   }
   public void onChannelHalt(String channelName){}
   public void onChannelsHalted(){}
}
