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

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

//import cloudos.kernel.DefaultExecutor;

public abstract class ChannelSender<DataType> extends DefaultExecutor {
	private int instanceId;

	private String channelName;
	private String srcFilterName;
	private String dstFilterName;
	private int dstInstances;

	private Map<String, String> attrs;

	//public abstract void start();
	//public abstract void finish();
	public abstract void send(DataType data);

	public void setInstanceId(int instanceId){
		this.instanceId = instanceId;
	}

	public int getInstanceId(){
		return this.instanceId;
	}

	public void setChannelName(String channelName){
		this.channelName = channelName;
	}

	public String getChannelName(){
		return this.channelName;
	}

	public void setSourceFilterName(String srcFilterName){
		this.srcFilterName = srcFilterName;
	}

	public String getSourceFilterName(){
		return this.srcFilterName;
	}

	public void setDestinationFilterName(String dstFilterName){
		this.dstFilterName = dstFilterName;
	}

	public String getDestinationFilterName(){
		return this.dstFilterName;
	}

	public void setDestinationInstances(int dstInstances){
		this.dstInstances = dstInstances;
	}

	public int getDestinationInstances(){
		return this.dstInstances;
	}

	public void setAttribute(String key, String value){
		this.attrs.put(key, value);
	}

	public String getAttribute(String key){
		return this.attrs.get(key);
	}

	public Map<String, String> getAttributes(){
		return this.attrs;
	}


	public void setAttributes(Map<String, String> attrs){
		this.attrs = attrs;
	}

}
