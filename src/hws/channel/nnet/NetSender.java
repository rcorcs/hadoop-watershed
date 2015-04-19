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

import java.io.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.DefaultEventExecutorGroup; //for non-blocking event handler
import io.netty.handler.codec.serialization.ClassResolvers; //serialization
import io.netty.handler.codec.serialization.ObjectDecoder;  //serialization
import io.netty.handler.codec.serialization.ObjectEncoder;  //serialization
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler; //channel event handler

import hws.util.Logger;
import hws.core.ChannelSender;

public abstract class NetSender extends ChannelSender{

    //private EventLoopGroup []groups;
    private EventLoopGroup group;
    private Channel []channels;

    private Channel connectToServer(final String host, final int port) throws Exception{
       final boolean SSL = false;
		// Configure SSL.git
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        } else {
            sslCtx = null;
        }

        // Configure the client.
        //EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(this.group)
             .channel(NioSocketChannel.class)
             .option(ChannelOption.TCP_NODELAY, true)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline p = ch.pipeline();
                     if (sslCtx != null) {
                         p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                     }
                     //p.addLast(new LoggingHandler(LogLevel.INFO));
                     //p.addLast(new EchoClientHandler());
                     p.addLast(
                            new ObjectEncoder(),
                            new ObjectDecoder(ClassResolvers.cacheDisabled(null)) );
					p.addLast(new DefaultEventExecutorGroup(2), new SimpleChannelInboundHandler<Object>(){
						@Override
						public void channelActive(ChannelHandlerContext ctx) {	
							//ctx.writeAndFlush(null);
						}
						@Override
						public void channelRead0(ChannelHandlerContext ctx, Object msg) {
							//future.setReply(msg);
							//ctx.close();
						}
						@Override
						public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
							// Close the connection when an exception is raised.
							//cause.printStackTrace();
							//future.setReply(future.getContext().error(cause.getMessage(), SystemCallErrorType.FATAL));
							ctx.close();
						}
					});
                 }
             });

            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();

            // Wait until the connection is closed.
            //f.channel().closeFuture().sync();
            return f.channel();
            //this.channels[id] = f.channel();
            //this.groups[id] = group;
        } finally {
            // Shut down the event loop to terminate all threads.
            //group.shutdownGracefully(); //TODO keep group reference so that it can be shutdown afterwards, during the finish
        }
    }
    public void start(){
        super.start();

        //groups = new EventLoopGroup[numConsumerInstances()];
        this.group = new NioEventLoopGroup();
        channels = new Channel[numConsumerInstances()];
           Logger.info("Starting channel sender: "+channelName()+" instance "+instanceId());
           for(int id = 0; id<numConsumerInstances(); id++){
              String host = shared().wait("host-"+id);
              Logger.info("connect to Host: "+host);
              Integer port = shared().wait("port-"+id);
              Logger.info("connect to Port: "+port);
              try{
                 Logger.info("Connecting to server id: "+id);
                 channels[id] = connectToServer(host, port.intValue());
                 Logger.info("Connection established");
                 if(channels[id]==null){Logger.warning("Error, channel is null");}
              }catch(Exception e){
                 channels[id] = null;
              }
           }
	}

	public void finish(){
           Logger.info("Finishing channel sender: "+channelName()+" instance "+instanceId());
           for(int id = 0; id<numConsumerInstances(); id++){
              Logger.info("Closing connection to server id "+id);
              this.channels[id].flush();
	      try{
              	this.channels[id].close().sync();
	      }catch(InterruptedException e){
		Logger.warning(e.getMessage());
	      }
              Logger.info("Connection closed");
           }
           Logger.info("shuting down eventLoop");
           this.group.shutdownGracefully();
           Logger.info("Done finishing NetSender");
           super.finish();
	}

    protected void send(Object obj, int consumerId){
       Logger.info("Sending data to consumerId "+consumerId+" : "+obj.toString());
       this.channels[consumerId].writeAndFlush(obj);
    }
}

