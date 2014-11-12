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

package hws.core;

import java.io.IOException;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import java.lang.reflect.ParameterizedType;

public class ChannelOutputSet<DataType>{

	private Map<String, ChannelSender<DataType>> senders;
	private boolean hasStarted;

	public ChannelOutputSet(){
		this.senders = new ConcurrentHashMap<String, ChannelSender<DataType>>();
		this.hasStarted = false;
	}

	public void addChannelSender(String filterName, ChannelSender<DataType> sender) {
		System.out.println("adding channel writer set: "+filterName);
		if(hasStarted){
			//System.out.println("starting channel writer set: "+channel.getHostName()+" "+channel.getTaskId());
			sender.start();
		}
		this.senders.put(filterName,sender);
	}

	public void start() {
		hasStarted = true;
		//this.hostName = hostName;
		//this.taskId = taskId;
		System.out.println("starting channel writer set");
		for(String filterName : this.senders.keySet()){
			this.senders.get(filterName).start();
			//ChannelWriter<DataType> output = outputChannels.get(filterName);
			//output.start(hostName, taskId);
		}
	}

	public Map<String, ChannelSender<DataType>> getChannelSenders(){
		return this.senders;
	}

	public ChannelSender<DataType> getChannelSender(String filterName){
		return this.senders.get(filterName);
	}

	public void send(DataType data) {
		//System.out.println("WRITER SET: writing in ("+outputChannels.keySet().size()+")");
		for(String filterName : this.senders.keySet()){
			//ChannelWriter<DataType> output = outputChannels.get(filterName);
			//output.write(data);
			this.senders.get(filterName).send(data);
		}
	}

	public void removeChannelSender(String filterName) {
		ChannelSender<DataType> sender = this.senders.remove(filterName);
		if(sender!=null) sender.finish();
	}

	public void finish() {
		for(String filterName : this.senders.keySet()){
			ChannelSender<DataType> sender = this.senders.remove(filterName);
			if(sender!=null) sender.finish();
		}
		
		hasStarted = false;
	}
}
