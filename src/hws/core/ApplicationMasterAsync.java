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

import java.io.File;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.IZkChildListener;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;


import hws.core.info.ModuleInfo;
import hws.core.info.FilterInfo;
import hws.core.info.ChannelInfo;
import hws.core.info.StubInfo;
import hws.core.info.InstanceInfo;
import hws.core.info.ModulePipeline;

import hws.util.Json;


/**
 * This class implements a simple async app master.
 * In real usages, the callbacks should execute in a separate thread or thread pool
 */
public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {
    private Configuration configuration;
    private NMClient nmClient;
    private String appIdStr;
    private ModulePipeline modulePipeline;
    private Map<String, InstanceInfo.Builder> instances;
    private Map<String, IZkChildListener> finishListeners;

    private String zksArgs;
    private String []zkServers;
    private ZkClient zk;
    //TODO 
    private int numContainersToWaitFor;
    private int currentModuleIndex = 0;

    private static final Log LOG = LogFactory.getLog(ApplicationMasterAsync.class);

    public ApplicationMasterAsync(ModulePipeline modulePipeline, String appIdStr, String zksArgs, String []zkServers) {
        this.numContainersToWaitFor = 0; //TODO remove

        configuration = new YarnConfiguration();
        this.appIdStr = appIdStr;
        this.modulePipeline = modulePipeline;
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
        this.instances = this.modulePipeline.instances();
        this.finishListeners = new ConcurrentHashMap<String, IZkChildListener>();

        this.zksArgs = zksArgs;
        this.zkServers = zkServers;
        zk = new ZkClient(zkServers[0]); //TODO choose the ZooKeeper server
    }

    public void onContainersAllocated(List<Container> containers) {
        FileSystem fs = null;
        PrintWriter out = null;
        try{
        fs = FileSystem.get(getConfiguration());
        out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/app-master-log.out")));
        }catch(IOException e){
           e.printStackTrace();
        }
        for (Container container : containers) {
            try{
               //PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/app-master-log.out")));
               out.println("Selecting instance to build");
               out.flush();
               //dado o container, escolher a instancia que tem dado de entrada mais perto daquele container
               InstanceInfo instanceInfo = null;
               if(instances.get(modulePipeline.get(currentModuleIndex).filterInfo().name()).instancesBuilt()>=modulePipeline.get(currentModuleIndex).numFilterInstances()){
                  currentModuleIndex++;
               }
               if(currentModuleIndex<modulePipeline.size()){
                  instanceInfo = instances.get(modulePipeline.get(currentModuleIndex).filterInfo().name()).build();
               }else break;

                out.println("Encoding instance info to json-base64");
                out.flush();
                String instanceInfoBase64 = Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(instanceInfo))).replaceAll("\\s","");
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java -Xmx256M hws.core.InstanceDriver --load " + instanceInfoBase64 +
                                        " -aid " + this.appIdStr +
                                        " " + this.zksArgs +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));
                out.println("Creating FileSystem");
                out.flush();

                //FileSystem fs = FileSystem.get(getConfiguration());

                out.println("Listing YARN-Watershed files for app-id: "+this.appIdStr);
                out.flush();
                RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("hdfs:///hws/bin/"), false);
                Map<String, LocalResource> resources = new HashMap<String, LocalResource>();
                out.println("Setup YARN-Watershed files as resources");
                out.flush();
                while(files.hasNext()){
                   LocatedFileStatus fileStatus = files.next();
                   // Setup jar for ApplicationMaster
                   LocalResource containerJar = Records.newRecord(LocalResource.class);
                   ContainerUtils.setupContainerJar(fs, fileStatus.getPath(), containerJar);
                   resources.put(fileStatus.getPath().getName(), containerJar);
                }

                out.println("Listing application files for app-id: "+this.appIdStr);
                out.flush();
                files = fs.listFiles(new Path("hdfs:///hws/apps/"+this.appIdStr+"/"), false);
                out.println("Setup application files as resources");
                out.flush();
                while(files.hasNext()){
                   LocatedFileStatus fileStatus = files.next();
                   // Setup jar for ApplicationMaster
                   LocalResource containerJar = Records.newRecord(LocalResource.class);
                   ContainerUtils.setupContainerJar(fs, fileStatus.getPath(), containerJar);
                   resources.put(fileStatus.getPath().getName(), containerJar);
                }
                out.println("container resource setup");
                out.flush();
                ctx.setLocalResources(resources);

                out.println("Environment setup");
                out.flush();
                // Setup CLASSPATH for ApplicationMaster
                Map<String, String> containerEnv = new HashMap<String, String>();
                ContainerUtils.setupContainerEnv(containerEnv, getConfiguration());
                ctx.setEnvironment(containerEnv);
                out.println("Starting containers");
                //out.close();

                //fs.close();
                System.out.println("[AM] Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
                out.println("Container started!");
                out.flush();
                /*String znode = "/hadoop-watershed/"+this.appIdStr+"/"+instanceInfo.filterInfo().name()+"/"+instanceInfo.instanceId();
                out.println("Saving instance znode: "+znode);
                out.flush();
                zk.createPersistent(znode, "");
                zk.createPersistent(znode+"/host", container.getNodeId().getHost());
                out.println("saved location: "+container.getNodeId().getHost());
                out.flush();
                */
                if(instances.get(modulePipeline.get(currentModuleIndex).filterInfo().name()).instancesBuilt()>=modulePipeline.get(currentModuleIndex).numFilterInstances()){
                   out.println("starting via ZooKeeper filter: "+instanceInfo.filterInfo().name());
                   out.flush();
                   zk.createPersistent("/hadoop-watershed/"+this.appIdStr+"/"+instanceInfo.filterInfo().name()+"/start", "");
                   out.println("Filter started");
                   out.flush();
                }
                //out.close();
            } catch (Exception e) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + e);
            }
        }
        try{
           fs.close();
        }catch(IOException e){
           out.println("ERROR: "+e.getMessage());
           out.flush();
        }
        out.close();
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            synchronized (this) {
                numContainersToWaitFor--;
            }
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("app-id")
                                   .withDescription( "String of the Application Id " )
                                   .hasArg()
                                   .withArgName("AppId")
                                   .create("aid"));
        options.addOption(OptionBuilder.withLongOpt( "load" )
                                       .withDescription( "load module pipeline" )
                                       .hasArg()
                                       .withArgName("Json-Base64")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt( "remove" )
                                       .withDescription( "remove modules" )
                                       .hasArgs()
                                       .withArgName("ModuleNames")
                                       .create("rm"));
        options.addOption(OptionBuilder.withLongOpt("zk-servers")
                                   .withDescription( "List of the ZooKeeper servers" )
                                   .hasArgs()
                                   .withArgName("zkAddrs")
                                   .create("zks"));
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

		String appIdStr = null;
        String modulePipelineBase64 = null;
        String modulePipelineJson = null;
        ModulePipeline modulePipeline = null;
        String []moduleNames = null;
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
           modulePipelineBase64 = cmd.getOptionValue("load");
           modulePipelineJson = StringUtils.newStringUtf8(Base64.decodeBase64( modulePipelineBase64 ));
           modulePipeline = Json.loads(modulePipelineJson, ModulePipeline.class);
        }else if(cmd.hasOption("rm")){
           moduleNames = cmd.getOptionValues("rm");
        }

        ApplicationMasterAsync master = new ApplicationMasterAsync(modulePipeline, appIdStr, zksArgs, zkServers);
        if(modulePipelineJson!=null){
           LOG.info("Load Module Pipeline: "+modulePipelineJson);
           PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/app-master.out")));
           out.println("Module Pipeline base 64: "+modulePipelineBase64);
           out.println("Load Module Pipeline: "+modulePipelineJson);
           out.println("Instances: "+Json.dumps(modulePipeline.instances()));
           out.close();
        }
        master.runMainLoop();

    }

    public IZkChildListener createFinishListener(final String filterName, final int numFilterInstances, final CountDownLatch doneLatch){
       IZkChildListener childListener = new IZkChildListener(){
           private String _filterName = filterName;
           private int _numFilterInstances = numFilterInstances;
           private CountDownLatch _doneLatch = doneLatch;
           public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception{
               if(currentChilds.size()==_numFilterInstances){
                  //TODO if all producers for a input port have halted, send a signal znode(/hadoop-watershed/appId/consumerFilter/halted/channelName) to all consumers
                  try{
                     PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/finished-"+_filterName+".out")));
                     out.println(parentPath);
                     for(String child: currentChilds){
                        out.print(child+";");
                     }
                     out.println();
                     out.close();
                     _doneLatch.countDown();
                  }catch(Exception e){}
               }
           }
       };
       this.finishListeners.put(filterName, childListener);
       return childListener;
    }

    public void runMainLoop() throws Exception {

        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        final CountDownLatch doneLatch = new CountDownLatch(this.modulePipeline.size());
        // Make container requests to ResourceManager
        for(ModuleInfo moduleInfo: this.modulePipeline){ //create containers for each instance of each module
           zk.createPersistent("/hadoop-watershed/"+this.appIdStr+"/"+moduleInfo.filterInfo().name(), "");
           zk.createPersistent("/hadoop-watershed/"+this.appIdStr+"/"+moduleInfo.filterInfo().name()+"/finish", "");
           zk.subscribeChildChanges("/hadoop-watershed/"+this.appIdStr+"/"+moduleInfo.filterInfo().name()+"/finish", createFinishListener(moduleInfo.filterInfo().name(), moduleInfo.numFilterInstances(), doneLatch));
           for(int i = 0; i<moduleInfo.numFilterInstances(); i++){
              this.numContainersToWaitFor++;
              ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
              System.out.println("[AM] Making res-req for " +moduleInfo.filterInfo().name()+" "+ i);
              rmClient.addContainerRequest(containerAsk);
           }
        }
        //TODO: process for starting the whole application
        //create containers
        // -> create instances
        // -> start output channels and filters
        // -> start input channels in reversed topological order (considering that there is no cycle)
        //    * if there is cycle, then inicially start in any order
        //TODO "send" the start signal via ZooKeeper

        System.out.println("[AM] waiting for containers to finish");
        /*while (!doneWithContainers()) {
            Thread.sleep(100);
        }*/
        try {
           doneLatch.await(); //await the input threads to finish
        }catch(InterruptedException e){
           e.printStackTrace();
        }
        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }
}
