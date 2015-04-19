package hws.net;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

//import cloudos.util.Logger;

public abstract class MessageHandler extends ConnectionHandler {
	public void run() {
		//Logger.info("Handling message");
		try{
			BufferedReader in = new BufferedReader(new InputStreamReader(getSocket().getInputStream()));
			String inputLine;
			while(true) {
				inputLine = in.readLine();
				//if(inputLine == null || "\0".equals(inputLine)) {
				if(inputLine == null) {
					break;
				}
				handleMessage(inputLine);
			}
			//this.server.count++;
			in.close();
			getSocket().close();
		}catch(IOException e) {
			e.printStackTrace();
		}
		//Logger.info("Finishing client connection");
	}

	public abstract void handleMessage(String msg);
}
