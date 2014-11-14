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

import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import hws.core.info.InstanceInfo;
import hws.core.info.StubInfo;
import hws.core.info.FilterInfo;
import hws.core.info.OutputChannelInfo;
import hws.core.info.InputChannelInfo;
import hws.util.Json;

class ExecutorThread<ExecutorType extends DefaultExecutor>  extends Thread {
	private ExecutorType executor;
    private List<DefaultExecutor> startingOrder;
    private CountDownLatch latch;
    private ZkClient zk;
    private String finishZnode;
   
	public ExecutorThread(ExecutorType executor, List<DefaultExecutor> startingOrder, CountDownLatch latch, ZkClient zk, String finishZnode){
		this.executor = executor;
        this.startingOrder = startingOrder;
        this.latch = latch;
        this.zk = zk;
        this.finishZnode = finishZnode;
	}

	public void run(){
        for(DefaultExecutor defaultExecutor: this.startingOrder){
            defaultExecutor.start();
        }
        ListIterator<DefaultExecutor> li = this.startingOrder.listIterator(this.startingOrder.size());
        while(li.hasPrevious()){
            DefaultExecutor defaultExecutor = li.previous();
            defaultExecutor.finish();
        }
        zk.createPersistent(finishZnode, "");
        this.latch.countDown();
		//this.executor.start();
		//this.executor.finish();
        
	}

	public ExecutorType getExecutor(){
		return this.executor;
	}
}

/**
    {@link InstanceDriver} is responsible for handling the whole execution process of a specific a module instance.
*/
public class InstanceDriver {
    private Filter filter;
    private List<DefaultExecutor> outputStartingOrder;
    private Map<String, List<DefaultExecutor>> inputStartingOrder;
    private Map<String, ExecutorThread<ChannelDeliver>> executors;
    private CountDownLatch latch;

    public InstanceDriver(){
       this.outputStartingOrder = new ArrayList<DefaultExecutor>();
       this.inputStartingOrder = new ConcurrentHashMap<String, List<DefaultExecutor>>();
    }

    public static void main(String []args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, ParseException {
        InstanceDriver driver = new InstanceDriver();
        driver.run(args);
    }

    public void run(String []args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, ParseException {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("app-id")
                                   .withDescription( "String of the Application Id" )
                                   .hasArg()
                                   .withArgName("AppId")
                                   .create("aid"));
        options.addOption(OptionBuilder.withLongOpt( "load" )
                                       .withDescription( "load module instance" )
                                       .hasArg()
                                       .withArgName("Json-Base64")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("zk-servers")
                                   .withDescription( "List of the ZooKeeper servers" )
                                   .hasArgs()
                                   .withArgName("zkAddrs")
                                   .create("zks"));
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        String appIdStr = null;
        String instanceInfoBase64 = null;
        String instanceInfoJson = null;
        InstanceInfo instanceInfo = null;

        if(cmd.hasOption("aid")){
           appIdStr = cmd.getOptionValue("aid");
        }
        String zksArgs = "";
        String []zkServers = null;
        if(cmd.hasOption("zks")){
           zksArgs = "-zks";
           zkServers = cmd.getOptionValues("zks");
           for(String zks:zkServers){
              zksArgs += " "+zks;
           }
        }
        if(cmd.hasOption("load")){
           instanceInfoBase64 = cmd.getOptionValue("load");
           instanceInfoJson = StringUtils.newStringUtf8(Base64.decodeBase64( instanceInfoBase64 ));
           instanceInfo = Json.loads(instanceInfoJson, InstanceInfo.class);
        }

        this.latch = new CountDownLatch(instanceInfo.inputChannels().keySet().size());

        ZkClient zk = new ZkClient(zkServers[0]); //TODO select a ZooKeeper server

        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/instance-driver.out")));
        out.println("Decoding instance info: "+instanceInfoBase64);
        out.flush();
        out.println("Instance info: "+instanceInfoJson);
        out.flush();
        loadInstance(instanceInfo, zk, "/hadoop-watershed/"+appIdStr+"/");
        out.println("Load Instance "+instanceInfo.instanceId());
        out.flush();

        ExecutorService serverExecutor = Executors.newCachedThreadPool();


        //wait for a start command from the ApplicationMaster via ZooKeeper
        String znode = "/hadoop-watershed/"+appIdStr+"/"+instanceInfo.filterInfo().name()+"/start";
        out.println("znode: "+znode);
        out.flush();
        zk.waitUntilExists(znode, TimeUnit.MILLISECONDS, 250);
        out.println("Exists: "+zk.exists(znode));
        out.flush();
        /*
        while(!zk.waitUntilExists(znode,TimeUnit.MILLISECONDS, 500)){
           //out.println("TIMEOUT waiting for start znode: "+znode);
           //out.flush();
        }*/
        //start and execute this instance
        out.println("Starting Instance");
        startExecutors(serverExecutor);
        out.println("Instance STARTED");
        out.flush();

        out.println("Waiting TERMINATION");
        out.flush();

        try {
           this.latch.await(); //await the input threads to finish
        }catch(InterruptedException e){
           // handle
           out.println("Waiting ERROR: "+e.getMessage());
           out.flush();
        }

        out.println("Finishing Instance");
        out.flush();
        finishExecutors();
        out.println("FINISHED Instance "+instanceInfo.instanceId());
        out.close();
    }

    private void startExecutors(ExecutorService serverExecutor){
        for(DefaultExecutor defaultExecutor: this.outputStartingOrder){
            defaultExecutor.start();
        }
        for(String channelName: this.executors.keySet()){
            serverExecutor.execute(this.executors.get(channelName));
        }
    }

    private void finishExecutors(){
        ListIterator<DefaultExecutor> li = this.outputStartingOrder.listIterator(this.outputStartingOrder.size());
        while(li.hasPrevious()){
            DefaultExecutor defaultExecutor = li.previous();
            defaultExecutor.finish();
        }
    }

    /**
    */
    private void loadInstance(InstanceInfo instanceInfo, ZkClient zk, String znodeBase) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.executors = new ConcurrentHashMap<String, ExecutorThread<ChannelDeliver>>();

        //loading filter instance from the instance info specification
        this.filter = (Filter)Class.forName(instanceInfo.filterInfo().className()).newInstance();
        this.filter.name(instanceInfo.filterInfo().name());
        this.filter.instanceId(instanceInfo.instanceId());
        this.filter.attributes(instanceInfo.filterInfo().attributes());
        //this.filter.shared(new Shared(zk, znodeBase+instanceInfo.filterInfo().name()));

        //loading output channels
        this.filter.outputChannels(instanceInfo.outputChannels().keySet());
        for(String channelName: instanceInfo.outputChannels().keySet()){
           for(OutputChannelInfo outputChannelInfo: instanceInfo.outputChannels().get(channelName)){
              ChannelSender sender = (ChannelSender)Class.forName(outputChannelInfo.senderInfo().className()).newInstance();
              sender.instanceId(instanceInfo.instanceId());
              sender.channelName(channelName);
              sender.attributes(outputChannelInfo.senderInfo().attributes());
              sender.producerName(instanceInfo.filterInfo().name());
              sender.consumerName(outputChannelInfo.consumerName());
              sender.numConsumerInstances(outputChannelInfo.numConsumerInstances());
              sender.shared(new Shared(zk, znodeBase+outputChannelInfo.consumerName()+"/"+channelName));
              this.outputStartingOrder.add(sender);

              //StubInfo encoderInfo = null;
              //while( (encoderInfo = outputChannelInfo.encoderInfoStack().popEncoderInfo())!=null ){
              for(StubInfo encoderInfo: outputChannelInfo.encoderInfoStack()){
                 ChannelEncoder encoder = (ChannelEncoder)Class.forName(encoderInfo.className()).newInstance();
                 encoder.channelSender(sender); //bind the encoder to the stack of senders
			     encoder.instanceId(instanceInfo.instanceId());
                 encoder.channelName(channelName);
                 encoder.attributes(encoderInfo.attributes());
                 encoder.producerName(instanceInfo.filterInfo().name());
                 encoder.consumerName(outputChannelInfo.consumerName());
                 encoder.numConsumerInstances(outputChannelInfo.numConsumerInstances());
                 //encoder.shared(new Shared(zk, znodeBase+outputChannelInfo.consumerName()+"/"+channelName));
                 this.outputStartingOrder.add(encoder);
                 sender = encoder; //update the bottom of the stack of channel senders
              }
              
              this.filter.outputChannel(channelName).addChannelSender(outputChannelInfo.consumerName(), sender);
           }
        }
        this.outputStartingOrder.add(filter);

        //loading input channels
        this.filter.inputChannels(instanceInfo.inputChannels().keySet());
        for(String channelName: instanceInfo.inputChannels().keySet()){
           InputChannelInfo inputChannelInfo = instanceInfo.inputChannels().get(channelName);
           if(!this.inputStartingOrder.containsKey(channelName)){
              this.inputStartingOrder.put(channelName, new ArrayList<DefaultExecutor>());
           }
           ChannelReceiver receiver = this.filter; //update the top of the stack of channel receivers
           //StubInfo decoderInfo = null;
           //while( (encoderInfo = inputChannelInfo.decoderInfoStack().popDecoderInfo())!=null ){
           for(StubInfo decoderInfo: inputChannelInfo.decoderInfoStack()){
              ChannelDecoder decoder = (ChannelDecoder)Class.forName(decoderInfo.className()).newInstance();
              decoder.channelReceiver(receiver);
              decoder.filter(this.filter);
              decoder.instanceId(instanceInfo.instanceId());
              decoder.numFilterInstances(instanceInfo.numFilterInstances());
              decoder.channelName(channelName);
              decoder.attributes(decoderInfo.attributes());
              //decoder.shared(new Shared(zk, znodeBase+instanceInfo.filterInfo().name()+"/"+channelName));
              this.inputStartingOrder.get(channelName).add(decoder);
              receiver = decoder; //update the top of the stack of channel receivers
           }

           ChannelDeliver deliver = (ChannelDeliver)Class.forName(inputChannelInfo.deliverInfo().className()).newInstance();
           deliver.channelReceiver(receiver);
           deliver.filter(this.filter);
           deliver.instanceId(instanceInfo.instanceId());
           deliver.numFilterInstances(instanceInfo.numFilterInstances());
           deliver.channelName(channelName);
           deliver.attributes(inputChannelInfo.deliverInfo().attributes());
           deliver.shared(new Shared(zk, znodeBase+instanceInfo.filterInfo().name()+"/"+channelName));
           this.inputStartingOrder.get(channelName).add(deliver);

           //each channel deliver of a filter has a distinct thread for its execution
           executors.put(channelName, new ExecutorThread<ChannelDeliver>(deliver, this.inputStartingOrder.get(channelName), this.latch, zk, znodeBase+instanceInfo.filterInfo().name()+"/finish/"+instanceInfo.instanceId() ));
        }
    }
}
