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

import java.util.concurrent.CountDownLatch;

import hws.core.ChannelDeliver;

public class IntegerDeliver extends ChannelDeliver<String>{
    private PrintWriter out;

    public void start(){
        super.start();
        try{
           out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/channel-deliver-"+channelName()+".out")));
           out.println("Starting channel deliver: "+channelName()+" instance "+instanceId());
           out.flush();
        }catch(IOException e){
           e.printStackTrace();
        }
        String strN = attribute("n");
        if(strN==null)strN="100";
        out.println("Generating n: "+strN);
        out.flush();
        Integer n = new Integer(strN);
        for(int i = 0; i<n.intValue(); i++){
           deliver(""+i);
        }
        out.println("Finished generating integers");
        out.flush();
	}

	public void finish(){
        super.finish();
        out.println("attribute: 'wait' = "+attribute("wait"));
        if("true".equals(attribute("wait"))){
           out.println("Waiting for producers to end");
           out.flush();
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
