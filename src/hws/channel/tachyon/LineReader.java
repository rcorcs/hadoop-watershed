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
import java.io.EOFException;
import java.io.DataInputStream;

import java.util.List;
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
import tachyon.thrift.ClientFileInfo;

import hws.core.ChannelDeliver;
import hws.util.Logger;

public class LineReader extends ChannelDeliver{
   private TachyonFS fileSystem;
   private DataInputStream reader;
   private Map<String, TachyonFile> files;
   private SortedSet<String> fileSet;
   private long totalBytes;
   private long beginPos;
   private long endPos;
   private long pos;
   private StringBuffer buffer;

   public void start(){
      super.start();
      Logger.info("Starting channel deliver: "+channelName()+" instance "+instanceId());

      files = new HashMap<String, TachyonFile>();
      fileSet = new TreeSet<String>();
      buffer = new StringBuffer();
      try{
         String masterAttr = attribute("master");
         TachyonURI masterAddress = new TachyonURI( masterAttr );
         fileSystem = TachyonFS.get( masterAddress );

		//partition or not the file among the instances
         boolean partition = !("false".equals(attribute("partition")));

         String pathAttr = attribute("path");
         //check if starts with "hdfs://"
         if(!pathAttr.startsWith("tachyon://"))
            pathAttr = "tachyon://"+pathAttr;

         Logger.info("Opening path: " + pathAttr);
         TachyonURI  path = new TachyonURI( pathAttr );
         TachyonFile file = fileSystem.getFile( path );

         //verifies if we have a path to a directory or to a file
         if(file.isDirectory()){
            List<ClientFileInfo> status = fileSystem.listStatus(path);
            totalBytes = 0;
            for(ClientFileInfo fileInfo : status){
              totalBytes += fileInfo.getLength();
              TachyonURI fileInfoPath = new TachyonURI( fileInfo.getPath() );
              files.put( fileInfo.getName(), fileSystem.getFile( fileInfoPath ) );
              fileSet.add( fileInfo.getName() );
              Logger.info("fileLen: " + fileInfo.getLength());
              Logger.info("file path: " + fileInfoPath.toString());
            }
         }else{
            //a file
            files.put(path.getName(), file);
            fileSet.add(path.getName());
            totalBytes = file.length();
            Logger.info("fileLen: " + file.length());
            Logger.info("file path: "+path.toString());
         }

         if(partition){
            //split
            int split = (int)Math.ceil((double)(totalBytes)/(double)(super.numFilterInstances()));
            beginPos = (super.instanceId())*split;
            endPos = (super.instanceId()+1)*split;
            Logger.info("split: "+split);
         }else{
            beginPos = 0;
            endPos = totalBytes;
            Logger.info("Reading full file: "+totalBytes);
         }

         pos = beginPos;

         Logger.info("beginPos: "+beginPos);
         Logger.info("endPos: "+endPos);
         Logger.info("pos: "+pos);

         //loading first file
         nextFile();

         //deliverLine
         String line = readLine();

         while( line != null ){
            //Logger.info( "--- line: " + line );
            deliver( line );
            line = readLine();
         }
      }catch(IOException e){
         Logger.severe("EXCEPTION: "+e.toString());
      }
   }

   public String readLine() throws IOException{
      if(pos>=endPos) return null;
      if(reader==null) throw new IOException();
      else {
         while(true){
            byte ch;
            try{
               ch = reader.readByte();
               pos++; //position tracker
            }catch(EOFException e){
               String str = buffer.toString();
               buffer = new StringBuffer(); //TODO reset the same StringBuffer object
               nextFile(); //nextFile
               return str;
            }

            /*out.println("ch: "+((char)ch));
              out.flush();
              out.println("pos: "+pos);
              out.flush();*/
            if(ch==((byte)'\n')){
               String str = buffer.toString();
               buffer = new StringBuffer(); //TODO reset the same StringBuffer object
               return str;
            }else if(ch==((byte)'\r')){
               pos++; //position tracker
               ch = reader.readByte();
               if(ch==((byte)'\n')){
                  String str = buffer.toString();
                  buffer = new StringBuffer(); //TODO reset the same StringBuffer object
                  return str;
               }else {
                  String str = buffer.toString();
                  buffer = new StringBuffer(); //TODO reset the same StringBuffer object
                  buffer.append((char)ch);
                  return str;
               }

            }else {
               buffer.append((char)ch);
            }
         }
      }
   }

   private boolean nextFile() throws IOException{
      if(reader!=null){
         reader.close();
      }
      reader=null;
      long totalRead = 0;
      String nextFileName = null;
      long filePos = 0;
      for(String fName : fileSet){
         if((totalRead+files.get(fName).length())>pos){
            nextFileName = fName;
            filePos = pos-totalRead;
            break;
         }
         totalRead += files.get(fName).length();
      }
      if(nextFileName!=null){
         InStream in = files.get( nextFileName ).getInStream( ReadType.CACHE );
         reader = new DataInputStream( in );
         if(filePos>0){ //if the position to read is in the middle of the file, skip the previous line.
            //reader.seek(filePos-1); // funcionava com FSDataInputStream
            //Logger.info( "--- skiped " + ( filePos-1 ) + " bytes" );
            reader.skipBytes( (int) filePos-1 ); // nao temos seek, portanto pulamos os bytes desnecessarios.
            readLine();//go to the next line of the file
         }
         return true;
      }else {
         pos = endPos;
         return false;
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
      //try{
      Logger.info("Finishing channel deliver: "+channelName()+" instance "+instanceId());
      /*}catch(IOException e){
        e.printStackTrace();
        }*/
   }

   public void onProducersHalted(){
      Logger.info("PRODUCERS HALTED!!");
   }
}
