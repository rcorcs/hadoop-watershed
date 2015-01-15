package hws.net;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.io.Closeable;

import java.net.Socket;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import hws.net.StandardOutputStream;

public class NodeCommunicator implements Closeable{
	//private NodeInfo nodeInfo;
	private String addr;

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
		//Logger.info("Connecting to: "+this.addr+": "+port);
		socket = new Socket(this.addr, port);
		out = new StandardOutputStream(socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	}

	public String readLine() throws IOException {
		return in.readLine();
	}	
	
	public int read() throws IOException {
		return in.read();
	}
	
	public int read(char[] cbuf, int off, int len) throws IOException {
		return in.read(cbuf, off, len);
	}

	public void writeLine(String str) throws IOException {
		out.writeLine(str);
	}

	public void flush() throws IOException {
		out.flush();
	}

	public void close() throws IOException {
		out.close();
		in.close();
		socket.close();
	}
}
