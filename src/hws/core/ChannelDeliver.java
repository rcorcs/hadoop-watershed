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
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.CountDownLatch;

public abstract class ChannelDeliver extends DefaultExecutor {
	private ChannelReceiver receiver;
	private int instanceId;
	private int nInstances;

	private String channelName;
	private Filter filter;

	private Map<String, String> attrs;
	private Shared shared;

        private CountDownLatch haltLatch;

        public void start(){
            super.start();
            this.haltLatch = new CountDownLatch(1);
        }

	public ChannelReceiver channelReceiver(){
		return this.receiver;
	}

	void channelReceiver(ChannelReceiver receiver){
		this.receiver = receiver;
	}

	void instanceId(int instanceId){
		this.instanceId = instanceId;
	}

	public int instanceId(){
		return this.instanceId;
	}

	void numFilterInstances(int nInstances){
		this.nInstances = nInstances;
	}

	public int numFilterInstances(){
		return this.nInstances;
	}

	void channelName(String channelName){
		this.channelName = channelName;
	}

	public String channelName(){
		return this.channelName;
	}

	public void deliver(Object data){
		channelReceiver().receive(channelName(), data);
	}

	void filter(Filter filter){
		this.filter = filter;
	}

	Filter filter(){
		return this.filter;
	}

	public void halt(){
                this.haltLatch.countDown();
		//filter.halt(channelName());
	}

        void waitToHalt() throws InterruptedException{
           this.haltLatch.await(); //await channel deliver to be halted
        }

        boolean hasHalted(){
            return this.haltLatch.getCount()==0;
        }

	void attribute(String key, String value){
		this.attrs.put(key, value);
	}

	public String attribute(String key){
		return this.attrs.get(key);
	}

	Map<String, String> attributes(){
		return this.attrs;
	}


	void attributes(Map<String, String> attrs){
		this.attrs = attrs;
	}


    void shared(Shared shared){
        this.shared = shared;
    }

    public Shared shared(){
        return this.shared;
    }

//EVENTS   
	public abstract void onProducersHalted();
}
