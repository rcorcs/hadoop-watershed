package hws.util;
/**
 * @author Rodrigo Caetano O. ROCHA
 * @date 25 July 2013
 */

//import java.io.Writer;
import java.io.OutputStream;
import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

import java.util.List;
import java.util.ArrayList;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Logger {
	private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss:S");
        private final static List<OutputStream> writers = new ArrayList<OutputStream>();

	private static final Lock lock = new ReentrantLock();

	// Suppress default constructor for noninstantiability
	private Logger() {
		throw new AssertionError();
	}

	private static void output(String str){
		try{ lock.lock();
                        for(OutputStream writer: writers){
                            try{
   			        writer.write(str.getBytes());
                                writer.flush();
                            }catch(IOException e){}
                        }
		}finally{
			lock.unlock();
		}
	}
	
	public static void addOutputStream(OutputStream output){
		try{ lock.lock();
			writers.add(output);
		}finally{
			lock.unlock();
		}
	}

	public static void info(String message){
		printlog("INFO: "+message);
	}
	
	public static void warning(String message){
		printlog("WARNING: "+message);
	}

	public static void severe(String message){
		printlog("SEVERE: "+message);
	}

	public static void fatal(String message){
		printlog("FATAL: "+message);
	}

	public static void log(String message){
		printlog(message);
	}

	private static void printlog(String message){
		StringBuffer sBuffer = new StringBuffer();
                sBuffer.append(dateFormat.format(new Date()));
		//sBuffer.append(dateFormat.format(Calendar.getInstance().getTime()));

		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		if(trace.length>=3){
			sBuffer.append(' ');
			sBuffer.append(trace[3].getClassName());
			sBuffer.append(' ');
			sBuffer.append(trace[3].getMethodName());
		}
		sBuffer.append('\n');
		sBuffer.append(message);
		sBuffer.append('\n');

		output(sBuffer.toString());
	}
}
