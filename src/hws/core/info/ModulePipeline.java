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
    
    private Map<String, ModuleInfo> modules;
    private Map<String, List<String>> inputs;
    private Map<String, List<String>> outputs;
    private Map<String, InstanceInfo.Builder> instances;

    private Set<String> files;

    public ModulePipeline(ModuleInfo []modulePipeline){
        this.modulePipeline = modulePipeline;
        this.files = null;
        this.modules = null;
        this.inputs = null;
        this.outputs = null;
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

    public ModuleInfo get(String filterName){
       return this.modules.get(filterName);
    }

    public Map<String, List<String>> inputBindings(){
       return this.inputs;
    }

    public Map<String, List<String>> outputBindings(){
       return this.outputs;
    }


    public Set<String> files(){
       if(this.files==null){
          this.files = new HashSet<String>();
          for(ModuleInfo moduleInfo: this.modulePipeline){
             File file = moduleInfo.filterInfo().file();
             if(file!=null) files.add(file.getPath());
             for(String channelName: moduleInfo.inputChannels().keySet()){
                ChannelInfo channelInfo = moduleInfo.inputChannelInfo(channelName);
                if(channelInfo.senderInfo()!=null){
                   file = channelInfo.senderInfo().file();
                   if(file!=null) files.add(file.getPath());
                }
                file = channelInfo.deliverInfo().file();
                if(file!=null) files.add(file.getPath());
                for(StubInfo stubInfo: channelInfo.encoderInfoStack()){
                   file = stubInfo.file();
                  if(file!=null) files.add(file.getPath());
                }
                for(StubInfo stubInfo: channelInfo.decoderInfoStack()){
                   file = stubInfo.file();
                   if(file!=null) files.add(file.getPath());
                }
             }
          }
       }
       return this.files;
    }

    public Map<String, InstanceInfo.Builder> instances(){
       if(this.instances==null){
		   /*
		   Map<String, InstanceInfo.Builder> instances = new HashMap<String, InstanceInfo.Builder>();
		   Map<String, ModuleInfo> modules = new HashMap<String, ModuleInfo>();
		   Map<String, List<String>> inputs = new HashMap<String, List<String>>();
		   Map<String, List<String>> outputs = new HashMap<String, List<String>>();
		   */
           this.instances = new HashMap<String, InstanceInfo.Builder>();
		   this.modules = new HashMap<String, ModuleInfo>();
		   this.inputs = new HashMap<String, List<String>>();
		   this.outputs = new HashMap<String, List<String>>();

		   for(ModuleInfo moduleInfo: this.modulePipeline){
		      modules.put(moduleInfo.filterInfo().name(), moduleInfo);
		      instances.put(moduleInfo.filterInfo().name(), new InstanceInfo.Builder(moduleInfo.filterInfo(), moduleInfo.numFilterInstances()));
		      for(String inputChannel: moduleInfo.inputChannels().keySet()){
		         if(!inputs.keySet().contains(inputChannel)){
		            inputs.put(inputChannel, new ArrayList<String>());
		         }
		         inputs.get(inputChannel).add(moduleInfo.filterInfo().name());
		         //setup
		         StubInfo deliverInfo = moduleInfo.inputChannelInfo(inputChannel).deliverInfo();
		         Deque<StubInfo> decoderInfoStack = moduleInfo.inputChannelInfo(inputChannel).decoderInfoStack();
		         instances.get(moduleInfo.filterInfo().name()).addInput(inputChannel, deliverInfo, decoderInfoStack);
		      }
		      for(String outputChannel: moduleInfo.outputChannels().keySet()){
		         if(!outputs.keySet().contains(outputChannel)){
		            outputs.put(outputChannel, new ArrayList<String>());
		         }
		         outputs.get(outputChannel).add(moduleInfo.filterInfo().name());
		         if(moduleInfo.outputChannelInfo(outputChannel)!=null && moduleInfo.outputChannelInfo(outputChannel).senderInfo()!=null){ 
		            StubInfo senderInfo = moduleInfo.outputChannelInfo(outputChannel).senderInfo();
		            Deque<StubInfo> encoderInfoStack = moduleInfo.outputChannelInfo(outputChannel).encoderInfoStack();
		            instances.get(moduleInfo.filterInfo().name()).addOutput(outputChannel, moduleInfo.filterInfo().name(), moduleInfo.numFilterInstances(), senderInfo, encoderInfoStack);
		         }
		      }
		   }

		   for(ModuleInfo moduleInfo: this.modulePipeline){
		      //for each filter, bind input channels with producers
		      for(String inputChannel: moduleInfo.inputChannels().keySet()){
		         if(outputs.keySet().contains(inputChannel)){
		            for(String producerName: outputs.get(inputChannel)){
		               StubInfo senderInfo = moduleInfo.inputChannelInfo(inputChannel).senderInfo();
		               Deque<StubInfo> encoderInfoStack = moduleInfo.inputChannelInfo(inputChannel).encoderInfoStack();
		               instances.get(producerName).addOutput(inputChannel, moduleInfo.filterInfo().name(), moduleInfo.numFilterInstances(), senderInfo, encoderInfoStack);
		            }
		         }
		      }

		      //for each filter, bind output channels with consumers
		      /*
		      for(String outputChannel: moduleInfo.outputChannels().keySet()){
		         if(inputs.keySet().contains(outputChannel)){
		            for(String consumerName: inputs.get(outputChannel)){
		               StubInfo senderInfo = modules.get(consumerName).inputChannelInfo(outputChannel).senderInfo();
		               Deque<StubInfo> encoderInfoStack = modules.get(consumerName).inputChannelInfo(outputChannel).encoderInfoStack();
		               instances.get(moduleInfo.filterInfo().name()).addOutput(outputChannel, consumerName, modules.get(consumerName).numFilterInstances(), senderInfo, encoderInfoStack);
		            }
		         }
		      }
		      */
		   }
       }
       return this.instances;
    }

    public Iterator<ModuleInfo> iterator(){
       return Arrays.asList(this.modulePipeline).iterator();
    }
}
