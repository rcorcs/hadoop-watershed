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

import java.io.File;
import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Arrays;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
/**
 * {@link ModulePipeline} represents the collection of filters that compose the application to be executed.
 **/
public class ModulePipeline implements Iterable<ModuleInfo> {
    private ModuleInfo []modulePipeline;
    private Set<String> files;

    public ModulePipeline(ModuleInfo []modulePipeline){
        this.modulePipeline = modulePipeline;
        this.files = null;
    }

	public static ModulePipeline fromXMLFiles(String []xmlFiles) throws ParserConfigurationException, SAXException, IOException{
       ModuleInfo []moduleArray = null;
       if(xmlFiles!=null){
          moduleArray = new ModuleInfo[xmlFiles.length];
          for(int i = 0; i<xmlFiles.length; ++i){
             ModuleInfo moduleInfo = ModuleInfo.loadFromXMLFile(xmlFiles[i]);
             moduleArray[i] = moduleInfo;
          }
       }else throw new NullPointerException();
       return new ModulePipeline(moduleArray);
    }

    public int size(){
       return this.modulePipeline.length;
    }

    public ModuleInfo get(int i){
       return this.modulePipeline[i];
    }

    public Set<String> files(){
       if(this.files==null){
          this.files = new HashSet<String>();
          for(ModuleInfo moduleInfo: this.modulePipeline){
             File file = moduleInfo.getFilterInfo().getFile();
             if(file!=null) files.add(file.getPath());
             for(String channelName: moduleInfo.getInputChannelInfo().keySet()){
                ChannelInfo channelInfo = moduleInfo.getInputChannelInfo(channelName);
                if(channelInfo.getSenderInfo()!=null){
                   file = channelInfo.getSenderInfo().getFile();
                   if(file!=null) files.add(file.getPath());
                }
                file = channelInfo.getDeliverInfo().getFile();
                if(file!=null) files.add(file.getPath());
                for(StubInfo stubInfo: channelInfo.getEncoderInfoStack()){
                   file = stubInfo.getFile();
                  if(file!=null) files.add(file.getPath());
                }
                for(StubInfo stubInfo: channelInfo.getDecoderInfoStack()){
                   file = stubInfo.getFile();
                   if(file!=null) files.add(file.getPath());
                }
             }
          }
       }
       return this.files;
    }

    public Map<String, InstanceInfo.Builder> instances(){
       Map<String, InstanceInfo.Builder> instances = new HashMap<String, InstanceInfo.Builder>();

       Map<String, ModuleInfo> modules = new HashMap<String, ModuleInfo>();
       Map<String, List<String>> inputs = new HashMap<String, List<String>>();
       Map<String, List<String>> outputs = new HashMap<String, List<String>>();

       for(ModuleInfo moduleInfo: this.modulePipeline){
          modules.put(moduleInfo.getFilterInfo().getName(), moduleInfo);
          instances.put(moduleInfo.getFilterInfo().getName(), new InstanceInfo.Builder(moduleInfo.getFilterInfo(), moduleInfo.getInstances()));
          for(String inputChannel: moduleInfo.getInputChannelInfo().keySet()){
             if(!inputs.keySet().contains(inputChannel)){
                inputs.put(inputChannel, new ArrayList<String>());
             }
             inputs.get(inputChannel).add(moduleInfo.getFilterInfo().getName());
             //setup
             StubInfo deliverInfo = moduleInfo.getInputChannelInfo(inputChannel).getDeliverInfo();
             Deque<StubInfo> decoderInfoStack = moduleInfo.getInputChannelInfo(inputChannel).getDecoderInfoStack();
             instances.get(moduleInfo.getFilterInfo().getName()).addInput(inputChannel, deliverInfo, decoderInfoStack);
          }
          for(String outputChannel: moduleInfo.getOutputChannelInfo().keySet()){
             if(!outputs.keySet().contains(outputChannel)){
                outputs.put(outputChannel, new ArrayList<String>());
             }
             outputs.get(outputChannel).add(moduleInfo.getFilterInfo().getName());
          }
       }

       for(ModuleInfo moduleInfo: this.modulePipeline){
          //for each filter, bind input channels with producers
          for(String inputChannel: moduleInfo.getInputChannelInfo().keySet()){
             if(outputs.keySet().contains(inputChannel)){
                for(String producerName: outputs.get(inputChannel)){
                   StubInfo senderInfo = moduleInfo.getInputChannelInfo(inputChannel).getSenderInfo();
                   Deque<StubInfo> encoderInfoStack = moduleInfo.getInputChannelInfo(inputChannel).getEncoderInfoStack();
                   instances.get(producerName).addOutput(inputChannel, moduleInfo.getFilterInfo().getName(), moduleInfo.getInstances(), senderInfo, encoderInfoStack);
                }
             }
          }

          //for each filter, bind output channels with consumers
          /*
          for(String outputChannel: moduleInfo.getOutputChannelInfo().keySet()){
             if(inputs.keySet().contains(outputChannel)){
                for(String consumerName: inputs.get(outputChannel)){
                   StubInfo senderInfo = modules.get(consumerName).getInputChannelInfo(outputChannel).getSenderInfo();
                   Deque<StubInfo> encoderInfoStack = modules.get(consumerName).getInputChannelInfo(outputChannel).getEncoderInfoStack();
                   instances.get(moduleInfo.getFilterInfo().getName()).addOutput(outputChannel, consumerName, modules.get(consumerName).getInstances(), senderInfo, encoderInfoStack);
                }
             }
          }
          */
       }
       return instances;
    }

    public Iterator<ModuleInfo> iterator(){
       return Arrays.asList(this.modulePipeline).iterator();
    }
}
