package hws.net;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.IOException;

public class StandardOutputStream extends DataOutputStream {
	public StandardOutputStream(OutputStream out){
		super(out);
	}

	public void writeLine(String str) throws IOException{
		if(str!=null){
			StringBuffer buff = new StringBuffer(str);
			buff.append('\n');
			super.writeBytes(buff.toString());
		}else super.writeBytes(null);
	}
}
