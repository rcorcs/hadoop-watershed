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

import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import java.util.AbstractMap.SimpleEntry;

public abstract class Filter<InputType, OutputType> extends DefaultExecutor implements ChannelReceiver<InputType> {
	private String name;
	private int instanceId;
	private Map<String, String> attrs;
	private Set<String> inputChannels;
	private Map<String, Boolean> inChannelsHalted;

	private Set<String> outputChannels;
	private Map<String, ChannelOutputSet<OutputType>> outputSets;
	/*
	public Filter(){
		this.outputSets = new ConcurrentHashMap<String, ChannelOutputSet<OutputType>>();
	}
	*/
	//public abstract void start();
	//public abstract void finish();
	public abstract void process(String src, InputType data);

	public void receive(String src, InputType data){
		process(src, data);
	}

	public void setName(String name){
		this.name = name;
	}

	public String getName(){
		return this.name;
	}

	public void setInstanceId(int instanceId){
		this.instanceId = instanceId;
	}

	public int getInstanceId(){
		return this.instanceId;
	}

	public void setAttributes(Map<String, String> attrs){
		this.attrs = attrs;
	}

	public String getAttribute(String attr){
		return this.attrs.get(attr);
	}

	public void setInputChannels(Set<String> inputChannels){
		this.inputChannels = inputChannels;
		inChannelsHalted = new ConcurrentHashMap<String, Boolean>();
		for(String chann: this.inputChannels){
			inChannelsHalted.put(chann, Boolean.FALSE);
		}
	}
	
	public Set<String> getInputChannels(){
		return this.inputChannels;
	}

	public void setOutputChannels(Set<String> outputChannels){
		this.outputChannels = outputChannels;
		this.outputSets = new ConcurrentHashMap<String, ChannelOutputSet<OutputType>>();
		for(String channelName: this.outputChannels){
			this.outputSets.put(channelName, new ChannelOutputSet<OutputType>());
		}
	}

	public Set<String> getOutputChannels(){
		return this.outputChannels;
	}

	public ChannelOutputSet<OutputType> getOutputChannel(String channelName){
		return this.outputSets.get(channelName);
	}

	public void halt() throws IOException {
		SimpleEntry<String, Integer> pair = new SimpleEntry<String,Integer>(getName(), new Integer(getInstanceId()));
		//TODO: halt (name,instance)
	}

	public void halt(String channelName){
		if(inChannelsHalted.keySet().contains(channelName)){
			inChannelsHalted.put(channelName, Boolean.TRUE);

			onChannelHalt(channelName); //call event

			boolean allHalted = true;
			for(String chann: inChannelsHalted.keySet()){
				if(!inChannelsHalted.get(chann).booleanValue()){
					allHalted = false;
					break;
				}
			}
			if(allHalted) {
				onChannelsHalted(); //call event
			}
		}
	}

	public abstract void onChannelHalt(String channelName);

	public abstract void onChannelsHalted();
}
