package hws.net;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
//import java.util.concurrent.ExecutionException;

import java.net.ServerSocket;

import java.io.IOException;

import java.lang.reflect.Type;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.InvocationTargetException;

//import cloudos.util.Logger;

public class NetServer<HandlerType extends ConnectionHandler> extends Thread {
	private ExecutorService serverExecutor;

	private ServerSocket socket;
	private final int port;
	private boolean listening;

	//Class<HandlerType> handlerType;
	private final ConnectionHandler.Factory factory;
	//public NetServer(int port, Class<HandlerType> handlerType) throws IOException {
	public NetServer(int port, ConnectionHandler.Factory factory) throws IOException{
		super("NetServer");
		//this.handlerType = handlerType;
		this.factory = factory;

		//super.setDaemon(true);
		serverExecutor = Executors.newCachedThreadPool();

		this.listening = true;
		//this.socket = null;
		//this.port = port;
		this.socket = new ServerSocket(port);
		this.port = this.socket.getLocalPort();
		//Logger.info("LISTENING AT: "+this.port);
		//this.socket.setSoTimeout(timeout);
	}

	public int getPort(){
		return this.port;
	}

	/**
	 * Finishes the server and stop listening.
	 */
	/*public void finish(){
		this.listening = false;
	}*/

	/*
	@SuppressWarnings ("unchecked")
	public Class<?> getTypeParameterClass(){
		//Type type = getClass().getGenericSuperclass();
		//ParameterizedType paramType = (ParameterizedType) type;
		//return (Class<HandlerType>) paramType.getActualTypeArguments()[0];
		//return ((Class)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
		HandlerType instance = ((Class)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
		return instance.getClass();
	}*/

	

	/**
	 * Accepts multiple connections and start a separate {@link ConnectionHandler} thread for each one.
	 */
	public void run(){
		//while(this.listening){
		for(;;){
			try{
				//HandlerType handler = getTypeParameterClass().newInstance();
				//HandlerType handler = (HandlerType)((Class)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
				//HandlerType handler = handlerType.getConstructor().newInstance();
				//Logger.info("New ConnectionHandler");
				ConnectionHandler handler = this.factory.newConnectionHandler();
				//Logger.info("HERE A");
				handler.setSocket(socket.accept());
				//Logger.info("HERE B");
				//Logger.info("Running ConnectionHandler");
				serverExecutor.execute( handler );
			}catch(IOException e){
				this.listening = false;
				e.printStackTrace();
				break;
			}
			/*}catch(InstantiationException e){
				e.printStackTrace();
			}catch(IllegalAccessException e){
				e.printStackTrace();
			}catch(InvocationTargetException e){
				e.printStackTrace();
			}catch(NoSuchMethodException e){
				e.printStackTrace();
			}
			*/

		}

		/*for(Thread t : threads){
			try{t.join();}catch(InterruptedException e){e.printStackTrace();}
		}*/
		/*try{socket.close();}catch(IOException e){
			e.printStackTrace();
		}*/
	}

}

