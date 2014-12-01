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

import java.util.concurrent.TimeUnit;

//import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

/**
 * {@link Shared} area for stubs of a particular stream channel.
 * The shared area is implemented as a collection of key-value pairs, where the value can be any non-null object.
 */
public class Shared {
   private ZkClient zk;
   private String znodeBase;

   Shared(ZkClient zk, String znodeBase){
      this.zk = zk;
      this.znodeBase = znodeBase;
      if(!this.zk.exists(znodeBase)){
         this.zk.createPersistent(znodeBase);
      }
      this.znodeBase = this.znodeBase+"/";
   }

   private String encodeKey(String key){
      return key;
   }

   /**
    * Creates or updates the value of a key-value pair.
    * @param key the key associated with the shared data specified by value.
    * @param value the value associated with the specified key.
    */
   public void set(String key, Object val){
      if(val==null) throw new NullPointerException();
      this.zk.createPersistent(znodeBase+encodeKey(key), val);
   }

   /**
    * Returns the value associated with key.
    * @param key the key associated with the shared data required.
    */
   public <T extends Object> T get(String key){
      return this.zk.readData(znodeBase+encodeKey(key), true);
   }

   /**
    * Waits for a key to exists. When the key happens to exist, the value associated with the key will be returned.
    * @param key the key associated with the shared data required.
    */
   public <T extends Object> T wait(String key){
      String znode = znodeBase+encodeKey(key);
      this.zk.waitUntilExists(znode,TimeUnit.MILLISECONDS, 250);
      return get(key);
   }


}
