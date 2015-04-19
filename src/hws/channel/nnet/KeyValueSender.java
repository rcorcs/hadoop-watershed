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
package hws.channel.nnet;

import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

public class KeyValueSender extends NetSender{
    public void send(Object obj){
      int key;
      //out.println("Channel sending: "+Json.dumps(obj));
      //out.flush();
      if(obj instanceof Entry){
         //out.println("Is Entry: "+((Entry<?,?>)obj).getKey().toString());
         //out.flush();
         key = ((Entry<?,?>)obj).getKey().hashCode()%numConsumerInstances();
      }else {
         //out.println("Is NOT Entry");
         //out.flush();
         key = obj.hashCode()%numConsumerInstances();
      }
      if(key<0){
         key = Math.abs(key)%numConsumerInstances();
      }
       send(obj, key);
    }
}

