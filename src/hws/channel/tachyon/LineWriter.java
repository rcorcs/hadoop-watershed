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

package hws.channel.tachyon;

import java.io.IOException;
import java.io.DataOutputStream;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.util.CommonUtils;

import hws.core.ChannelSender;
import hws.util.Logger;

public class LineWriter extends ChannelSender{
   private TachyonFS fileSystem;
   private DataOutputStream writer;

   public void start(){
      super.start();
      Logger.info("Starting channel sender: "+channelName()+" instance "+instanceId());

      try{
         String masterAttr = attribute( "master" );
         TachyonURI masterAddress = new TachyonURI( masterAttr );
         fileSystem = TachyonFS.get( masterAddress );

         String pathAttr = attribute("path");
         //check if starts with "hdfs://"
         //if(!pathAttr.startsWith("hdfs://")){
         //   pathAttr = "hdfs://"+pathAttr;
         //}
         Logger.info("Opening path: " + pathAttr);
         TachyonURI filePath = null;

         //TODO if we have just one instance, create a file instead a folder of files.
         if( numConsumerInstances() == 1 ){
            filePath = new TachyonURI( pathAttr );
         } else{
            // TODO create a new folder to put the files
            //Path folderPath = new Path ( pathAttr );
            //if( !fileSystem.exists( folderPath ) ){
            //   Logger.info("Creating folder: " + pathAttr);
            //   fileSystem.mkdirs(folderPath);
            //}
            //create a file for this instance
            //filePath = new Path(pathAttr + "/" + instanceId());
         }
         fileSystem.createFile( filePath );
         TachyonFile file = fileSystem.getFile( filePath );
         OutStream out = file.getOutStream( WriteType.ASYNC_THROUGH );
         writer = new DataOutputStream( out );

      }catch(IOException e){
         Logger.severe("EXCEPTION: "+e.toString());
      }
   }

   public void finish(){
      super.finish();
      Logger.info("attribute: 'wait' = "+attribute("wait"));
      if("true".equals(attribute("wait"))){
         Logger.info("Waiting for producers to end");
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
      Logger.info("Finishing channel deliver: "+channelName()+" instance "+instanceId());
      /*}catch(IOException e){
        e.printStackTrace();
        }*/
   }

   public void send( Object obj ){
      String data = obj.toString();
      byte[] dataBytes = (data + "\n").getBytes();
      try{
        Logger.info( "--- line: " + data );
        writer.write(dataBytes, 0, dataBytes.length);
        writer.flush();
      }catch(IOException e){ ////TODO throw Exception
         Logger.severe(e.toString());
      }
   }
}
