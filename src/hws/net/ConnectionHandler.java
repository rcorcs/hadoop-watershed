package hws.net;

import java.net.Socket;

public class ConnectionHandler extends Thread {
	private Socket socket;

	/*
	public ConnectionHandler(Socket socket){
		this.socket = socket;
	}
	*/
	public static interface Factory {
		public ConnectionHandler newConnectionHandler();
	}

	public void setSocket(Socket socket){
		this.socket = socket;
	}

	public Socket getSocket(){
		return this.socket;
	}


}
