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
import java.util.concurrent.ConcurrentHashMap;

import java.lang.reflect.ParameterizedType;

public class ChannelOutputSet{

	private Map<String, ChannelSender> senders;
	private boolean hasStarted;

	ChannelOutputSet(){
		this.senders = new ConcurrentHashMap<String, ChannelSender>();
		this.hasStarted = false;
	}

	void addChannelSender(String filterName, ChannelSender sender) {
		System.out.println("adding channel writer set: "+filterName);
		if(hasStarted){
			//System.out.println("starting channel writer set: "+channel.getHostName()+" "+channel.getTaskId());
			sender.start();
		}
		this.senders.put(filterName,sender);
	}

	void start() {
		hasStarted = true;
		System.out.println("starting channel writer set");
		for(String filterName : this.senders.keySet()){
			this.senders.get(filterName).start();
		}
	}

	Map<String, ChannelSender> channelSenders(){
		return this.senders;
	}

	ChannelSender channelSender(String filterName){
		return this.senders.get(filterName);
	}

	public void send(Object data) {
		//System.out.println("WRITER SET: writing in ("+outputChannels.keySet().size()+")");
		for(String filterName : this.senders.keySet()){
			//ChannelWriter<DataType> output = outputChannels.get(filterName);
			//output.write(data);
			this.senders.get(filterName).send(data);
		}
	}

	void removeChannelSender(String filterName) {
		ChannelSender sender = this.senders.remove(filterName);
		if(sender!=null) sender.finish();
	}

	void finish() {
		for(String filterName : this.senders.keySet()){
			ChannelSender sender = this.senders.remove(filterName);
			if(sender!=null) sender.finish();
		}
		
		hasStarted = false;
	}
}
