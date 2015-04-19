package hws.net;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.io.Closeable;

import java.net.Socket;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import hws.net.StandardOutputStream;

//Lazy connection
public class NodeCommunicator implements Closeable{
	//private NodeInfo nodeInfo;
	private String addr;
        private int port;

	private Socket socket = null;
	private StandardOutputStream out = null;
	private BufferedReader in = null;

	public NodeCommunicator(String addr){
		//this.nodeInfo = null;
		this.addr = addr;
		this.socket = null;
		this.out = null;
		this.in = null;
	}

	public void connect(int port) throws IOException {
                this.port = port;
		//Logger.info("Connecting to: "+this.addr+": "+port);
		/*
		socket = new Socket(this.addr, port);
                socket.setSoTimeout(0);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                //socket.shutdownInput();
		out = new StandardOutputStream(socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		*/
	}
        public void reconnect() throws IOException {
                //socket.close();
		socket = new Socket(this.addr, this.port);
                socket.setSoTimeout(0);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                //socket.shutdownInput();
		out = new StandardOutputStream(socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }

	public String readLine() throws IOException {
		if(socket==null){
		socket = new Socket(this.addr, this.port);
                socket.setSoTimeout(0);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                //socket.shutdownInput();
		out = new StandardOutputStream(socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		}
		return in.readLine();
	}	
	
	public int read() throws IOException {
		if(socket==null){
		socket = new Socket(this.addr, this.port);
                socket.setSoTimeout(0);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                //socket.shutdownInput();
		out = new StandardOutputStream(socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		}
		return in.read();
	}
	
	public int read(char[] cbuf, int off, int len) throws IOException {
		if(socket==null){
		socket = new Socket(this.addr, this.port);
                socket.setSoTimeout(0);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                //socket.shutdownInput();
		out = new StandardOutputStream(socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		}
		return in.read(cbuf, off, len);
	}

	public void writeLine(String str) throws IOException {
		if(socket==null){
		socket = new Socket(this.addr, this.port);
                socket.setSoTimeout(0);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                //socket.shutdownInput();
		out = new StandardOutputStream(socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		}
		out.writeLine(str);
	}

	public void flush() throws IOException {
		if(socket==null){
		socket = new Socket(this.addr, this.port);
                socket.setSoTimeout(0);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                //socket.shutdownInput();
		out = new StandardOutputStream(socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		}
		out.flush();
	}

	public void close() throws IOException {
		if(socket!=null){
		out.close();
		in.close();
		socket.close();
		}
	}
}
