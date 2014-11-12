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

public class OutputChannelInfo {
	private StubInfo senderInfo;
	private Deque<StubInfo> encoderInfoStack;
    private String consumerName;
    private int numConsumerInstances;

    OutputChannelInfo(StubInfo senderInfo, Deque<StubInfo> encoderInfoStack){
       this.senderInfo = senderInfo;
       this.encoderInfoStack = encoderInfoStack;
    }

    void consumerName(String consumerName){
       this.consumerName = consumerName;
    }

    public String consumerName(){
       return this.consumerName;
    }

    void numConsumerInstances(int numConsumerInstances){
       this.numConsumerInstances = numConsumerInstances;
    }

    public int numConsumerInstances(){
       return this.numConsumerInstances;
    }

    public StubInfo senderInfo(){
       return this.senderInfo;
    }

    public Deque<StubInfo> encoderInfoStack(){
       return this.encoderInfoStack;
    }
}
