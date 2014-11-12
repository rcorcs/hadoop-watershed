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

package hws.core.info;

import java.util.Deque;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;

public class ChannelInfo {
	private String name;
	private StubInfo senderInfo;
	private Deque<StubInfo> encoderInfoStack;
	private Deque<StubInfo> decoderInfoStack;
	private StubInfo deliverInfo;

	public ChannelInfo(String name){
		this.name = name;
		this.senderInfo = null;
		this.encoderInfoStack = new ArrayDeque<StubInfo>();
		this.decoderInfoStack = new ArrayDeque<StubInfo>();
		this.deliverInfo = null;
	}

	void name(String name){
		this.name = name;
	}

	public String name(){
		return this.name;
	}
	
	void senderInfo(StubInfo senderInfo){
		this.senderInfo = senderInfo;
	}

	public StubInfo senderInfo(){
		return this.senderInfo;
	}

	void deliverInfo(StubInfo deliverInfo){
		this.deliverInfo = deliverInfo;
	}

	public StubInfo deliverInfo(){
		return this.deliverInfo;
	}

	public Deque<StubInfo> encoderInfoStack(){
		return this.encoderInfoStack;
	}

	public Deque<StubInfo> decoderInfoStack(){
		return this.decoderInfoStack;
	}

	public void pushEncoderInfo(StubInfo encoderInfo){
		this.encoderInfoStack.push(encoderInfo);
	}

	public StubInfo popEncoderInfo(){
		try{
			return this.encoderInfoStack.pop();
		}catch(NoSuchElementException e){
			return null;
		}
	}

	public void pushDecoderInfo(StubInfo decoderInfo){
		this.decoderInfoStack.push(decoderInfo);
	}

	public StubInfo popDecoderInfo(){
		try{
			return this.decoderInfoStack.pop();
		}catch(NoSuchElementException e){
			return null;
		}
	}
}

