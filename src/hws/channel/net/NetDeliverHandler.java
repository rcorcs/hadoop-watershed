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

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import hws.core.ChannelDeliver;
/**
 * Handler implementation for the system call server.
 */
@Sharable
public class NetDeliverHandler<DataType> extends ChannelInboundHandlerAdapter{
    private ChannelDeliver<DataType> channelDeliver;
	public NetDeliverHandler(ChannelDeliver<DataType> channelDeliver){
		this.channelDeliver = channelDeliver;
	}
	
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg){
        this.channelDeliver.deliver((DataType)msg);
		/*SystemCallRequest req = (SystemCallRequest)msg;
		System.out.println("Server received: "+req.module()+"."+req.method());
		SystemCallReply reply = new SystemCallReply(req.module(),req.method(),null,"Unknown Module: "+req.module(),SystemCallErrorType.WARNING);
		if(req.module().equals(this.moduleController.getModuleName())){
			reply = this.moduleController.handleSystemCall(req);
		}
		if(req.waitReply()){
			ctx.writeAndFlush(reply);
		}else{
			ctx.close();
		}*/
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx){
        //ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
        cause.printStackTrace();
        ctx.close();
    }
}
