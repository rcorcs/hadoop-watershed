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

package hws.channel.hdfs;

import java.io.*;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import hws.core.ChannelSender;

public class LineWriter extends ChannelSender{
   private PrintWriter out;
   private FileSystem fileSystem;
   private FSDataOutputStream writer;

   public void start(){
      super.start();
      try{
         out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/channel-deliver-"+channelName()+".out")));
         out.println("Starting channel sender: "+channelName()+" instance "+instanceId());
         out.flush();
      }catch(IOException e){
         e.printStackTrace();
      }

      try{
         Configuration conf = new Configuration();
         //       conf.setBoolean("fs.hdfs.impl.disable.cache", true);
         fileSystem = FileSystem.get(conf);

         String pathAttr = attribute("path");
         //check if starts with "hdfs://"
         if(!pathAttr.startsWith("hdfs://")){
            pathAttr = "hdfs://"+pathAttr;
         }
         out.println("Opening path: " + pathAttr);
         out.flush();
         Path filePath;

         //TODO if we have just one instance, create a file instead a folder of files.
         if( numConsumerInstances() == 1 ){
            filePath = new Path( pathAttr );
         } else{
            // create a new folder to put the files
            Path folderPath = new Path ( pathAttr );
            if( !fileSystem.exists( folderPath ) ){
               out.println("Creating folder: " + pathAttr);
               out.flush();
               fileSystem.mkdirs(folderPath);
            }
            //create a file for this instance
            filePath = new Path(pathAttr + "/" + instanceId());
         }
         writer = fileSystem.create(filePath);

      }catch(IOException e){
         out.println("EXCEPTION: "+e.getMessage());
         out.flush();
         e.printStackTrace();
      }
   }

   public void finish(){
      super.finish();
      out.println("attribute: 'wait' = "+attribute("wait"));
      if("true".equals(attribute("wait"))){
         out.println("Waiting for producers to end");
         out.flush();
         /*try {
         //latch.await(); //await the input threads to finish
         }catch(InterruptedException e){
         // handle
         out.println("Waiting ERROR: "+e.getMessage());
         out.flush();
         }*/
      }
      try{
      writer.close();
      fileSystem.close();
      }catch(IOException e){
         e.printStackTrace();
      }

      //try{
      out.println("Finishing channel deliver: "+channelName()+" instance "+instanceId());
      out.flush();
      out.close();
      /*}catch(IOException e){
        e.printStackTrace();
        }*/
   }

   public void send( Object obj ){
      String data = (String)obj;
      byte[] dataBytes = (data + "\n").getBytes();
      try{
         writer.write(dataBytes, 0, dataBytes.length);
         writer.flush();
      }catch(IOException e){ ////TODO throw Exception
         e.printStackTrace();
      }
      out.println( "data: " + data );
      out.flush();
   }
}
