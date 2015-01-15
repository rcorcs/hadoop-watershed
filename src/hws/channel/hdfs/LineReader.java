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

import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import hws.core.ChannelDeliver;

public class LineReader extends ChannelDeliver<String>{
    private PrintWriter out;
    private FSDataInputStream reader;
    private long totalBytes;
    private long beginPos;
    private long endPos;
    private long pos;
    private StringBuffer buffer;

    public void start(){
       
        super.start();
        try{
           out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/channel-deliver-"+channelName()+".out")));
           out.println("Starting channel deliver: "+channelName()+" instance "+instanceId());
           out.flush();
        }catch(IOException e){
           e.printStackTrace();
        }
         buffer = new StringBuffer();
         try{
         Configuration conf = new Configuration();
//       conf.setBoolean("fs.hdfs.impl.disable.cache", true);
         FileSystem fileSystem = FileSystem.get(conf);

         String path = attribute("path");
         //check if starts with "hdfs://"
         if(!path.startsWith("hdfs://")){
            path = "hdfs://"+path;
         }
         out.println("Opening path: " + path);
         out.flush();
         ///TODO FileSystem:: public boolean isDirectory(Path f)
         reader = fileSystem.open(new Path(path));

         //totalBytes
         //a file
         FileStatus status = fileSystem.getFileStatus(new Path(path));
         totalBytes = 0;
         totalBytes += status.getLen();
         out.println("fileLen: " + status.getLen());
         out.flush();
         out.println("file path: "+status.getPath().toString());
         out.flush();

         //TODO a list of files
         FileStatus status[] = fileSystem.get

         
         //split
         int split = (int)Math.ceil((double)(totalBytes)/(double)(super.numFilterInstances()));
         beginPos = (super.instanceId())*split;
         endPos = (super.instanceId()+1)*split;

         pos = beginPos;

         out.println("split: "+split);
         out.flush();
         out.println("beginPos: "+beginPos);
         out.flush();
         out.println("endPos: "+endPos);
         out.flush();
         out.println("pos: "+pos);
         out.flush();

         //deliverLine
         String line = readLine();
         
         while( line != null ){
               out.println("processing line: "+line);
               out.flush();
               deliver( line );
               line = readLine();
         }
         }catch(IOException e){
            out.println("EXCEPTION: "+e.getMessage());
            out.flush();
            e.printStackTrace();
         }
    }

   public String readLine() throws IOException{
            if(pos>=endPos) return null;
            if(reader==null) throw new IOException();
            else {
               while(true){
                  byte ch;
                  try{
                     pos++; //position tracker
                     ch = reader.readByte();
                  }catch(EOFException e){
                     String str = buffer.toString();
                     buffer = new StringBuffer(); //TODO reset the same StringBuffer object
                     //nextFile(); //TODO nextFile
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
         if((totalRead+files.get(fName).getLen())>pos){
            nextFileName = fName;
            filePos = pos-totalRead;
            break;
         }
         totalRead += files.get(fName).getLen();
      }
      if(nextFileName!=null){
         reader = fileSystem.open(files.get(nextFileName).getPath());
         if(filePos>0){ //if the position to read is in the middle of the file, skip the previous line.
            reader.seek(filePos-1);
            
            read();//go to the next line of the file
         }
         return true;
      }else {
         pos = endPos;
         return false;
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
        //try{
           out.println("Finishing channel deliver: "+channelName()+" instance "+instanceId());
           out.flush();
           out.close();
        /*}catch(IOException e){
           e.printStackTrace();
        }*/
	}

    public void onProducersHalted(){
       out.println("PRODUCERS HALTED!!");
       out.flush();
    }
}
