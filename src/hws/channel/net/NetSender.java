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

package hws.channel.net;

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

import hws.core.ChannelSender;

public abstract class NetSender<DataType> extends ChannelSender<DataType>{
    private PrintWriter out;

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
        try{
           out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hadoop/rcor/yarn/channel-sender-"+channelName()+".out")));
           out.println("Starting channel sender: "+channelName()+" instance "+instanceId());
           out.flush();
           for(int id = 0; id<numConsumerInstances(); id++){
              String host = shared().wait("host-"+id);
              out.println("connect to Host: "+host);
              out.flush();
              Integer port = shared().wait("port-"+id);
              out.println("connect to Port: "+port);
              out.flush();
              try{
                 out.println("Connecting to server id: "+id);
                 out.flush();
                 channels[id] = connectToServer(host, port.intValue());
                 out.println("Connection established");
                 out.flush();
                 if(channels[id]==null){out.println("Error, channel is null");out.flush();}
              }catch(Exception e){
                 channels[id] = null;
              }
           }
        }catch(IOException e){
           e.printStackTrace();
        }
	}

	public void finish(){
        super.finish();
        //try{
           out.println("Finishing channel sender: "+channelName()+" instance "+instanceId());
           out.flush();
           for(int id = 0; id<numConsumerInstances(); id++){
              out.println("Closing connection to server id "+id);
              out.flush();
              this.channels[id].flush();
              this.channels[id].close();
              out.println("Connection closed");
              out.flush();
           }
           out.println("shuting down eventLoop");
           out.flush();
           this.group.shutdownGracefully();
           out.println("Done finishing NetSender");
           out.close();
        /*}catch(IOException e){
           e.printStackTrace();
        }*/
	}

    protected void send(DataType data, int consumerId){
       out.println("Sending data to consumerId "+consumerId+" : "+data.toString());
       this.channels[consumerId].writeAndFlush(data);
    }
}

