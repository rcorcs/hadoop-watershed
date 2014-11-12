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
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.io.FileUtils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import hws.core.info.ModuleInfo;
import hws.core.info.FilterInfo;
import hws.core.info.ChannelInfo;
import hws.core.info.StubInfo;
import hws.core.info.ModulePipeline;

import hws.util.Json;

public class Client {

  private static final Log LOG = LogFactory.getLog(Client.class);

  Configuration conf = new YarnConfiguration();
  
  public void run(String[] args) throws Exception {
    //final String command = args[0];
    //final int n = Integer.valueOf(args[1]);
    //final Path jarPath = new Path(args[2]);
    Options options = new Options();
    options.addOption(OptionBuilder.withLongOpt("jar")
                                   .withDescription( "Jar path" )
                                   .hasArg()
                                   .withArgName("JarPath")
                                   .create());
    options.addOption(OptionBuilder.withLongOpt("scheduler")
                                   .withDescription( "Scheduler class name" )
                                   .hasArg()
                                   .withArgName("ClassName")
                                   .create());
    options.addOption(OptionBuilder.withLongOpt("zk-servers")
                                   .withDescription( "List of the ZooKeeper servers" )
                                   .hasArgs()
                                   .withArgName("zkAddrs")
                                   .create("zks"));
    options.addOption("l", "list", false, "list modules");
    options.addOption(OptionBuilder.withLongOpt( "load" )
                                   .withDescription( "load new modules" )
                                   .hasArgs()
                                   .withArgName("XMLFiles")
                                   .create());
    options.addOption(OptionBuilder.withLongOpt( "remove" )
                                   .withDescription( "remove modules" )
                                   .hasArgs()
                                   .withArgName("ModuleNames")
                                   .create("rm"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    Path jarPath = null;
    String schedulerClassName = null;
    String []xmlFileNames = null;
    String []moduleNames = null;
    String zksArgs = "";
    String []zkServers = null;
    if(cmd.hasOption("zks")){
       zksArgs = "-zks";
       zkServers = cmd.getOptionValues("zks");
       for(String zks:zkServers){
          zksArgs += " "+zks;
       }
    }
    if(cmd.hasOption("l")){
       LOG.warn("Argument --list (-l) is not supported yet.");
    }
    if(cmd.hasOption("jar")){
       jarPath = new Path(cmd.getOptionValue("jar")); 
    }
    if(cmd.hasOption("scheduler")){
       schedulerClassName = cmd.getOptionValue("scheduler");
    }
    if(cmd.hasOption("load")){
       xmlFileNames = cmd.getOptionValues("load");
    }else if(cmd.hasOption("rm")){
       moduleNames = cmd.getOptionValues("rm");
    }

    LOG.info("Jar-Path "+jarPath);
    if(xmlFileNames!=null){
       String paths = "";
       for(String path : xmlFileNames){
          paths += path+"; ";
       }
       LOG.info("Load XMLs: "+paths);
    }
    if(moduleNames!=null){
       String modules = "";
       for(String module: moduleNames){
          modules += module+"; ";
       }
       LOG.info("remove: "+modules);
    }
    // Create yarnClient
    YarnConfiguration conf = new YarnConfiguration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    
    // Create application via yarnClient
    YarnClientApplication app = yarnClient.createApplication();

    System.out.println("LOG Path: "+ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = 
        Records.newRecord(ContainerLaunchContext.class);

    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    
    ZkClient zk = new ZkClient(zkServers[0]); //TODO select a ZooKeeper server
    if(!zk.exists("/hadoop-watershed")){
       zk.createPersistent("/hadoop-watershed", "");
    }
	zk.createPersistent("/hadoop-watershed/"+appId.toString(), "");

    FileSystem fs = FileSystem.get(conf);

    LOG.info("Collecting files to upload");
	fs.mkdirs(new Path("hdfs:///hws/apps/"+appId.toString()));
	
    ModulePipeline modulePipeline = ModulePipeline.fromXMLFiles(xmlFileNames);
    LOG.info("Uploading files to HDFS");
    for(String path: modulePipeline.files()){
       uploadFile(fs, new File(path), appId);
    }
    LOG.info("Upload finished");

    String modulePipelineJson = Json.dumps(modulePipeline);
    String modulePipelineBase64 = Base64.encodeBase64String(StringUtils.getBytesUtf8(modulePipelineJson)).replaceAll("\\s","");
    LOG.info("ModulePipeline: "+modulePipelineJson);
    LOG.info("ModulePipeline: "+modulePipelineBase64);
    amContainer.setCommands(
        Collections.singletonList(
            "$JAVA_HOME/bin/java" +
            " -Xmx256M" +
            " hws.core.ApplicationMasterAsync" +
            " -aid "+ appId.toString() +
            " --load " + modulePipelineBase64 +
            " " + zksArgs +
            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr" 
            )
        );
    
    // Setup jar for ApplicationMaster
    //LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    //setupAppMasterJar(jarPath, appMasterJar);
    //amContainer.setLocalResources(Collections.singletonMap("hws.jar", appMasterJar));


    LOG.info("Listing files for YARN-Watershed");
    RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(new Path("hdfs:///hws/bin/"), false);
    Map<String, LocalResource> resources = new HashMap<String, LocalResource>();
    LOG.info("Files setup as resource");
    while(filesIterator.hasNext()){
       LocatedFileStatus fileStatus = filesIterator.next();
       // Setup jar for ApplicationMaster
       LocalResource containerJar = Records.newRecord(LocalResource.class);
       ContainerUtils.setupContainerJar(fs, fileStatus.getPath(), containerJar);
       resources.put(fileStatus.getPath().getName(), containerJar);
    }
    LOG.info("container resource setup");
    amContainer.setLocalResources(resources);

    fs.close(); //closing FileSystem interface

    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    ContainerUtils.setupContainerEnv(appMasterEnv, conf);
    amContainer.setEnvironment(appMasterEnv);
    
    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    capability.setVirtualCores(1);

    // Finally, set-up ApplicationSubmissionContext for the application
    //ApplicationSubmissionContext appContext = 
    //app.getApplicationSubmissionContext();
    appContext.setApplicationName("Hadoop-Watershed"); // application name
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // queue 

    // Submit application
    LOG.info("Submitting application " + appId);
    yarnClient.submitApplication(appContext);
    
    LOG.info("Waiting for containers to finish");
    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    YarnApplicationState appState = appReport.getYarnApplicationState();
    while (appState != YarnApplicationState.FINISHED && 
           appState != YarnApplicationState.KILLED && 
           appState != YarnApplicationState.FAILED) {
      Thread.sleep(100);
      appReport = yarnClient.getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
    }

    //zk.deleteRecursive("/hadoop-watershed/"+appId.toString());

    System.out.println(
        "Application " + appId + " finished with" +
    		" state " + appState + 
    		" at " + appReport.getFinishTime());

  }

  private void uploadFile(FileSystem fs, File file, ApplicationId appId) throws IOException {
	Path filePath = new Path("hdfs:///hws/apps/"+appId.toString()+"/"+file.getName());
	if(fs.exists(filePath)){ fs.delete(filePath, true); }
    FSDataOutputStream ostream = fs.create(filePath);
    byte[] data = FileUtils.readFileToByteArray(file);
    ostream.write(data);
    ostream.close();
  }

  public static void main(String[] args) throws Exception {
    Client c = new Client();
    c.run(args);
  }
}
