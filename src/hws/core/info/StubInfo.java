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

import java.io.IOException;
import java.io.File;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class StubInfo extends ExecutorInfo{	
	//private ExecutorInfo executorInfo;
	//private String className;
	//private String fileName;
	private Map<String, String> attrs;
	
	public StubInfo(String className, File file) throws IOException{
		super(className, file);
		//this.className = className;
		this.attrs = new ConcurrentHashMap<String, String>();
		//this.fileName = null;
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
}

