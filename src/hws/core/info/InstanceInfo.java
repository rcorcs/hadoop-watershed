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
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class InstanceInfo {
    private final FilterInfo filterInfo;
    private final int numFilterInstances;
    private Map<String, InputChannelInfo> inputChannels;
    private Map<String, List<OutputChannelInfo>> outputChannels;
    private int instanceId;

    private InstanceInfo(Builder builder){
       this.filterInfo = builder.filterInfo;
       this.numFilterInstances = builder.numFilterInstances;
       this.instanceId = builder.instancesBuilt;
       this.inputChannels = builder.inputs;
       this.outputChannels = builder.outputs;
    }

    public FilterInfo filterInfo(){
        return this.filterInfo;
    }

    public Map<String, InputChannelInfo> inputChannels(){
        return this.inputChannels;
    }

    public Map<String, List<OutputChannelInfo>> outputChannels(){
        return this.outputChannels;
    }

    public int numFilterInstances(){
       return this.numFilterInstances;
    }

    public int instanceId(){
       return this.instanceId;
    }

    public static class Builder{
       private final FilterInfo filterInfo;
       private final int numFilterInstances;
       private Map<String, InputChannelInfo> inputs;
       private Map<String, List<OutputChannelInfo>> outputs;
       private int instancesBuilt;

       public Builder(FilterInfo filterInfo, int numFilterInstances){
          this.instancesBuilt = 0;
          this.filterInfo = filterInfo;
          this.numFilterInstances = numFilterInstances;
          this.inputs = new HashMap<String, InputChannelInfo>();
          this.outputs = new HashMap<String, List<OutputChannelInfo>>();
       }

       public Builder addInput(String channelName, StubInfo deliverInfo, Deque<StubInfo> decoderInfoStack){
          this.inputs.put(channelName, new InputChannelInfo(deliverInfo, decoderInfoStack));
          return this;
       }

       public Builder addOutput(String channelName, String consumerName, int numConsumerInstances, StubInfo senderInfo, Deque<StubInfo> encoderInfoStack){
          if(!this.outputs.containsKey(channelName)){
             this.outputs.put(channelName, new ArrayList<OutputChannelInfo>());
          }
          OutputChannelInfo outputChannelInfo = new OutputChannelInfo(senderInfo, encoderInfoStack);
          outputChannelInfo.consumerName(consumerName);
          outputChannelInfo.numConsumerInstances(numConsumerInstances);
          this.outputs.get(channelName).add(outputChannelInfo);
          return this;
       }

       public int instancesBuilt(){
          return this.instancesBuilt;
       }

       public InstanceInfo build(){
          InstanceInfo instanceInfo = new InstanceInfo(this);
          this.instancesBuilt++;
          return instanceInfo;
       }
    }
}
