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

package hws.util;

import java.io.IOException;
import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.SerializationUtils;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

class ZkDataMonitor implements Watcher {
	private ZooKeeper zk;
	private String path;
	private Watcher watcher;
	private boolean keepWatching;

    public static void watch(ZooKeeper zk, String path, Watcher watcher, boolean keepWatching) throws KeeperException, InterruptedException {
       new ZkDataMonitor(zk, path, watcher, keepWatching);
    }

	private ZkDataMonitor(ZooKeeper zk, String path, Watcher watcher, boolean keepWatching) throws KeeperException, InterruptedException{
		this.zk = zk;
		this.path = path;
		this.watcher = watcher;
		this.keepWatching = keepWatching;
		try{
			zk.exists(path, this);
		}catch(KeeperException e){
			e.printStackTrace();
		}catch(InterruptedException  e){
			e.printStackTrace();
		}
	}
	
	public void process(WatchedEvent event){
		//System.out.println(event.toString());
		this.watcher.process(event);
		if(this.keepWatching){
			try{
				zk.exists(this.path, this);
			}catch(KeeperException e){
				e.printStackTrace();
			}catch(InterruptedException  e){
				e.printStackTrace();
			}
		}
	}
}

public class ZkClient {

   //private String []serverAddrs;
   private ZooKeeper zk;

   public ZkClient(String serverAddr) throws IOException, InterruptedException{
      //this.serverAddrs = new String[1];
      //this.serverAddrs[0] = serverAddr;
      this(serverAddr, Integer.MAX_VALUE);
   }

   public ZkClient(String serverAddr, int sessionTimeout) throws IOException, InterruptedException{
      final CountDownLatch connectedSignal = new CountDownLatch(1);
      this.zk = new ZooKeeper(serverAddr, sessionTimeout, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
             if(event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
             }
          }
      });
      connectedSignal.await();
   }

	public void delete(String path) throws KeeperException, InterruptedException{
		this.zk.delete(path, -1);
	}

    public void deleteAll(String path) throws KeeperException, InterruptedException{
        List<String> children = null;
		try{
		   children = this.zk.getChildren(path,false);
		}catch(KeeperException e){
           if(e.code()!=KeeperException.Code.NONODE){
              throw e;
           }
		}
        if(children!=null){
		   for(String child: children){
              deleteAll((new File(path)).getPath()+"/"+child);
		   }
        }
		this.zk.delete(path, -1);
	}

	public void create(String path, Serializable data) throws KeeperException, InterruptedException{
		this.zk.create(path, SerializationUtils.serialize(data), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

    public void watch(String path, Watcher watcher) throws KeeperException, InterruptedException{
		ZkDataMonitor.watch(this.zk, path, watcher, false);
	}

	public void watch(String path, Watcher watcher, boolean loop) throws KeeperException, InterruptedException{
		ZkDataMonitor.watch(this.zk, path, watcher, loop);
	}

    public <T> T read(String path) throws KeeperException, InterruptedException{
        Stat stat = new Stat();
        byte []data = this.zk.getData(path, false, stat);
        return (T)SerializationUtils.deserialize(data);
    }
}
