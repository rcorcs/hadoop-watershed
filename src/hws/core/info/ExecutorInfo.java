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

package hws.core.info;

import java.io.IOException;
import java.io.File;

//import org.apache.commons.io.FileUtils;
//import org.apache.commons.codec.binary.Base64;
//import org.apache.commons.codec.binary.StringUtils;

public class ExecutorInfo {
	private String name;
	private String className;
	private File file;

	public ExecutorInfo(String className, File file) throws IOException{
		this.name = null;
		this.className = className;
		/*if(file!=null)
			this.fileBase64 = Base64.encodeBase64String(FileUtils.readFileToByteArray(file));
		else this.fileBase64 = null;
		*/
		this.file = file;
	}
	
	public void setName(String name){
		this.name = name;
	}
	
	public String getName(){
		return this.name;
	}
	
	public void setClassName(String className){
		this.className = className;
	}
	
	public String getClassName(){
		return this.className;
	}

	/*
	public void setFileBase64(String fileBase64){
		this.fileBase64 = fileBase64;
	}
	
	public void writeFile(String fileName) throws IOException {
		File file = new File(fileName);
		File parent = file.getParentFile();
		if(parent!=null) FileUtils.forceMkdir( parent );
		FileUtils.writeByteArrayToFile(file, Base64.decodeBase64(getFileBase64()));
	}

	public String getFileBase64(){
		return this.fileBase64;
	}
	*/

	public File getFile(){
		return this.file;
	}

	public void setFile(File file){
		this.file = file;
	}
}
