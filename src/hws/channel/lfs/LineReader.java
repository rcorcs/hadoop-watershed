package hws.channel.lfs;


import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedInputStream;

import hws.core.ChannelDeliver;
import hws.util.Logger;

public class LineReader extends ChannelDeliver{
	//private RandomAccessFile reader;
	private BufferedInputStream reader;
	private long totalBytesRead;
        

	//private String readLine(RandomAccessFile reader) throws IOException {
	private String readLine(BufferedInputStream reader) throws IOException {
		StringBuffer str = new StringBuffer();
		int ch = reader.read();
		totalBytesRead++;
		while(ch>0 && ch!='\n'){
			str.append((char)ch);
			ch = reader.read();
			totalBytesRead++;
		}
		if(ch<0 && str.length()==0) return null;
		else return str.toString();
	}
	
	public void start(){
		super.start();
                Logger.info("Starting channel deliver: "+channelName()+" instance "+instanceId());
		boolean partition = !("false".equals(attribute("partition")));

		this.totalBytesRead = 0;
		int idx = super.instanceId();
		int n = super.numFilterInstances();
		long bytesToRead = 0;
		Logger.info("IDX: "+idx);
		Logger.info("N: "+n);
		Logger.info("READING FILE: "+attribute("file"));
		try{
			File file = new File(attribute("file"));
			//reader = new RandomAccessFile(file, "r");
			long size = file.length();
			bytesToRead = (size/((long)n));
			Logger.info("READING: "+bytesToRead+" bytes");
			if(idx>0 && partition){
				long pos = ((long)idx)*bytesToRead;
				Logger.info("START READING AT: "+pos);
				RandomAccessFile raf = new RandomAccessFile(file, "r");
				raf.seek(pos-1);
				reader = new BufferedInputStream(new FileInputStream(raf.getFD()));
				String temp = readLine(reader);
				Logger.info("PAD LINE: "+temp);
				this.totalBytesRead = 0;
			}else {
				reader = new BufferedInputStream(new FileInputStream(file));
			}
			String line;
			while( (line=readLine(reader))!=null){
				//out.println(line);
				deliver(line);
				if(this.totalBytesRead>=bytesToRead && partition) break;
			}
			reader.close();
		}catch(IOException e){
			Logger.severe(e.toString());
		}
		//halt();
	}
	public void finish(){
		super.finish();
                Logger.info("Finishing channel deliver: "+channelName()+" instance "+instanceId());
	}

public void onProducersHalted(){
      Logger.info("PRODUCERS HALTED!!");
   }
}
