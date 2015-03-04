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

import java.util.AbstractMap.SimpleEntry;

import hws.core.Filter;
import hws.util.Json;
import hws.util.Logger;

public class Counter extends Filter{
   private PrintWriter out;

   public void start(){
      super.start();
         Logger.info("Starting Filter: "+name()+" instance "+instanceId());
   }

   public void finish(){
      super.finish();
      //try{
      Logger.info("Finishing Filter: "+name()+" instance "+instanceId());
      /*}catch(IOException e){
        e.printStackTrace();
        }*/
   }
   public void process(String src, Object obj){
      String data = (String)obj;
      String []words = data.split("\\W+");
      for(String word : words){
         SimpleEntry<String, String> pair = new SimpleEntry<String,String>(word, "1");
         for(String channelName: outputChannels()){
            outputChannel(channelName).send(pair);
         }
      }
   }
   public void onChannelHalt(String channelName){
      Logger.info("Input Channel Halted: "+channelName);
   }
   public void onChannelsHalted(){}
}
