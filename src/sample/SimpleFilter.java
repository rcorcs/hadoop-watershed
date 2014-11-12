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

import hws.core.Filter;

public class SimpleFilter extends Filter<String, String>{
    private PrintWriter out;
    public void start(){
        super.start();
        try{
           out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/filter-"+getName()+".out")));
           out.println("Starting Filter: "+getName()+" instance "+getInstanceId());
           out.flush();
        }catch(IOException e){
           e.printStackTrace();
        }
	}

	public void finish(){
        super.finish();
        //try{
           out.println("Finishing Filter: "+getName()+" instance "+getInstanceId());
           out.flush();
           out.close();
        /*}catch(IOException e){
           e.printStackTrace();
        }*/
	}
    public void process(String src, String data){
    }
    public void onChannelHalt(String channelName){}
	public void onChannelsHalted(){}
}