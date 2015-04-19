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


import java.io.*; //TODO debug


import java.net.UnknownHostException;
import java.net.SocketAddress;
import java.net.InetSocketAddress;

import java.util.concurrent.CountDownLatch;

/*
import org.apache.commons.lang.exception.NestableException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
*/

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.DefaultEventExecutorGroup; //for non-blocking event handler
import io.netty.handler.codec.serialization.ClassResolvers; //serialization
import io.netty.handler.codec.serialization.ObjectDecoder;  //serialization
import io.netty.handler.codec.serialization.ObjectEncoder;  //serialization


import hws.util.Logger;
import hws.core.ChannelDeliver;

public class NetDeliver extends ChannelDeliver{

    static final boolean SSL = false;
    private Channel serverChannel;
    private CountDownLatch latch;

	public void start() {
       super.start();
       
           Logger.info("Starting channel deliver: "+channelName()+" instance "+instanceId());
       // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            try{
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
            }catch(Exception e){
               Logger.warning(e.getMessage());
            }
        } else {
            sslCtx = null;
        }
        

        final ChannelDeliver deliverHandler = this;
        // Configure the server.
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 100)
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline p = ch.pipeline();
                     if (sslCtx != null) {
                         p.addLast(sslCtx.newHandler(ch.alloc()));
                     }
                     //p.addLast(new LoggingHandler(LogLevel.INFO));
                     //p.addLast(new EchoServerHandler());
                     p.addLast(
                            new ObjectEncoder(),
                            new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                     p.addLast(new DefaultEventExecutorGroup(2), new NetDeliverHandler(deliverHandler));
                 }
             });

            Logger.info("Binding to a listening port");
            // Start the server.
            ChannelFuture f = b.bind(0).sync();
            this.serverChannel = f.channel();
            this.latch = new CountDownLatch(1);

            SocketAddress socketAddress = this.serverChannel.localAddress();
            if(socketAddress instanceof InetSocketAddress){
               Logger.info("Connected to port: "+((InetSocketAddress)socketAddress).getPort());
               shared().set("host-"+instanceId(), hws.net.NetUtil.getLocalCanonicalHostName());
               shared().set("port-"+instanceId(), new Integer(((InetSocketAddress)socketAddress).getPort()));
            }
            Logger.info("Host: "+hws.net.NetUtil.getLocalCanonicalHostName());
            Logger.info("Connected to: "+f.channel().localAddress().toString());
            
            Logger.info("Running server, waiting for a close command");
            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
            Logger.info("Channel closed");
        } catch(Exception e){
           Logger.warning(e.getMessage());
        }finally {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        Logger.info("Counting down the latch");
        this.latch.countDown();
    }

    public void onProducersHalted(){
       Logger.info("Closing server channel");
       this.serverChannel.close();
       Logger.info("Closing command completed");
    }

    public void finish(){
       Logger.info("Waiting server channel to be closed");
       try {
           this.latch.await(); //await server channel to be closed
       }catch(InterruptedException e){
           // handle
           Logger.warning(e.getMessage());
       }
       Logger.info("Finishing channel deliver: "+channelName()+" instance "+instanceId());
       super.finish();
    }

}

