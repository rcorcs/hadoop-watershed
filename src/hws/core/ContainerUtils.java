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

import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Apps;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;

public class ContainerUtils {

    private ContainerUtils(){
       throw new AssertionError();
    }

    public static void setupContainerJar(FileSystem fs, Path jarPath, LocalResource containerJar) throws IOException {
       FileStatus jarStat = fs.getFileStatus(jarPath);
       containerJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
       containerJar.setSize(jarStat.getLen());
       containerJar.setTimestamp(jarStat.getModificationTime());
       containerJar.setType(LocalResourceType.FILE);
       containerJar.setVisibility(LocalResourceVisibility.PUBLIC);
   }

   public static void setupContainerEnv(Map<String, String> containerEnv, Configuration conf) {
        for (String c : conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
              Apps.addToEnvironment(containerEnv, Environment.CLASSPATH.name(),
              c.trim());
        }
        Apps.addToEnvironment(containerEnv,
            Environment.CLASSPATH.name(),
            Environment.PWD.$() + File.separator + "*");
    }
}
