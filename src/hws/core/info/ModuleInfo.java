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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.IOException;
import java.io.File;
//import java.io.ByteArrayInputChannel;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.IllegalFormatException;

import java.io.File;
import java.io.IOException;

public class ModuleInfo {
	private int instances;

	private FilterInfo filterInfo;
	private Map<String, ChannelInfo> inChannelInfo;
	private Map<String, ChannelInfo> outChannelInfo;

	public ModuleInfo(){
		this.filterInfo = null;
		this.instances = 1;
		this.inChannelInfo = new ConcurrentHashMap<String, ChannelInfo>();
		this.outChannelInfo = new ConcurrentHashMap<String, ChannelInfo>();
	}

	public ModuleInfo(FilterInfo filterInfo){
		this();
		this.filterInfo = filterInfo;
	}

	private static StubInfo readStubInfo(Element element, String tagName) throws ParserConfigurationException, IOException{
		StubInfo stubInfo = null;
		NodeList nList = element.getElementsByTagName(tagName);
		if(nList.getLength()==1){
			Node nNode = nList.item(0);
			if(nNode.getNodeType() == Node.ELEMENT_NODE){
				Element eElement = (Element)nNode;
				if(eElement.getAttribute("class")==null) throw new ParserConfigurationException("Tag \""+tagName+"\" must contain a \"class\" attribute.");

				File file = null;
				String fileName = eElement.getAttribute("file");
				if(fileName!=null && fileName.trim().length()==0) fileName = null;
				else{
					file = new File(fileName);
					//Logger.info("file: "+file.getPath());
				}
				stubInfo = new StubInfo(eElement.getAttribute("class"), file);
				//stubInfo.setFileName(eElement.getAttribute("file"));
				System.out.println("Stub: "+stubInfo.getClassName()+" "+fileName);
				NodeList attrList = eElement.getElementsByTagName("attr");
				for(int k = 0; k < attrList.getLength(); k++){
					nNode = attrList.item(k);
					if(nNode.getNodeType() == Node.ELEMENT_NODE){
						eElement = (Element)nNode;
						stubInfo.setAttribute(eElement.getAttribute("name"), eElement.getAttribute("value"));
					}
				}
			}
		}else if(nList.getLength()>1){
			//ERROR
		}
		return stubInfo;
	}

	private static StubInfo readInStubInfo(Element element, String tagName) throws ParserConfigurationException, IOException{
		StubInfo stubInfo = null;
		
				if(element.getAttribute("class")==null) throw new ParserConfigurationException("Tag \""+tagName+"\" must contain a \"class\" attribute.");

				File file = null;
				String fileName = element.getAttribute("file");
				if(fileName!=null && fileName.trim().length()==0) fileName = null;
				else{
					file = new File(fileName);
					//Logger.info("file: "+file.getPath());
				}
				stubInfo = new StubInfo(element.getAttribute("class"), file);
				//stubInfo.setFileName(eElement.getAttribute("file"));
				System.out.println("Stub: "+stubInfo.getClassName()+" "+fileName);
				NodeList attrList = element.getElementsByTagName("attr");
				for(int k = 0; k < attrList.getLength(); k++){
					Node nNode = attrList.item(k);
					if(nNode.getNodeType() == Node.ELEMENT_NODE){
						Element eElement = (Element)nNode;
						stubInfo.setAttribute(eElement.getAttribute("name"), eElement.getAttribute("value"));
					}
				}
		return stubInfo;
	}

	private static ChannelInfo readChannelInfo(Element element) throws ParserConfigurationException, IOException{
		//eElement = (Element)nNode;
		if(element.getAttribute("name")==null) throw new ParserConfigurationException("Tag \"channel\" must contain a \"name\" attribute.");
		ChannelInfo channel = new ChannelInfo(element.getAttribute("name"));
		StubInfo deliverInfo = readStubInfo(element, "deliver");
		StubInfo senderInfo = readStubInfo(element, "sender");
		channel.setDeliverInfo(deliverInfo);
		channel.setSenderInfo(senderInfo);

		NodeList temp1 = element.getElementsByTagName("encoders");
		System.out.println("temp1: "+temp1.getLength());
		if(temp1.getLength()>0){
			Node n1 = temp1.item(0);
			if(n1.getNodeType() == Node.ELEMENT_NODE){
				Element e = (Element)n1;
				NodeList encodersList = e.getElementsByTagName("encoder");
				System.out.println("encoders: "+encodersList.getLength());
				for(int j = 0; j < encodersList.getLength(); j++){
					Node node = encodersList.item(j);
					if(node.getNodeType() == Node.ELEMENT_NODE){
						StubInfo encoderInfo = readInStubInfo(((Element)node), "encoder");
						if(encoderInfo!=null){
							System.out.println("**encoder: "+encoderInfo.getClassName());
							channel.pushEncoderInfo(encoderInfo);
						}
					}
				}
			}
		}

		NodeList temp2 = element.getElementsByTagName("decoders");
		System.out.println("temp2: "+temp2.getLength());
		if(temp2.getLength()>0){
			Node n2 = temp2.item(0);
			if(n2.getNodeType() == Node.ELEMENT_NODE){
				Element e = (Element)n2;
				NodeList decodersList = e.getElementsByTagName("decoder");
				System.out.println("decoders: "+decodersList.getLength());
				for(int j = 0; j < decodersList.getLength(); j++){
					Node node = decodersList.item(j);
					if(node.getNodeType() == Node.ELEMENT_NODE){
						StubInfo decoderInfo = readInStubInfo(((Element)node), "decoder");
						if(decoderInfo!=null){
							System.out.println("**decoder: "+decoderInfo.getClassName());
							channel.pushDecoderInfo(decoderInfo);
						}
					}
				}
			}
		}

		return channel;
	}

	public static ModuleInfo loadFromXMLFile(String fileName) throws ParserConfigurationException, SAXException, IOException{
		ModuleInfo module = new ModuleInfo();
		FilterInfo filter;

		File fXmlFile = new File(fileName);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);

		//optional, but recommended
		//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
		doc.getDocumentElement().normalize();

		if(!"filter".equals(doc.getDocumentElement().getNodeName())){
			return null;
		}

		File file = null;
		String filterFileName = doc.getDocumentElement().getAttribute("file");
		if(filterFileName!=null && filterFileName.trim().length()==0) filterFileName = null;
		else{
			file = new File(filterFileName);
			//Logger.info("file: "+file.getPath());
		}
		module.setInstances( Integer.parseInt(doc.getDocumentElement().getAttribute("instances")) );
		filter = new FilterInfo(doc.getDocumentElement().getAttribute("name"), doc.getDocumentElement().getAttribute("class"), file);
		//filter.setFileName(doc.getDocumentElement().getAttribute("file"));

		Node nNode = null;
		Element eElement = null;
		NodeList nList = null;
		NodeList attrList = null;
		
		nList = doc.getElementsByTagName("attr");
		for(int i = 0; i < nList.getLength(); i++){
			nNode = nList.item(i);
			if(nNode.getNodeType() == Node.ELEMENT_NODE){
				eElement = (Element)nNode;
				filter.setAttribute(eElement.getAttribute("name"), eElement.getAttribute("value"));
			}
		}
		module.setFilterInfo(filter);

		nList = doc.getElementsByTagName("output");
		for(int i = 0; i < nList.getLength(); i++){
			nNode = nList.item(i);
			if(nNode.getNodeType() == Node.ELEMENT_NODE){
				eElement = (Element)nNode;
				NodeList channelList = eElement.getElementsByTagName("channel");
				for(int j = 0; j < channelList.getLength(); j++){
					nNode = channelList.item(j);
					if(nNode.getNodeType() == Node.ELEMENT_NODE){
						ChannelInfo channel = readChannelInfo((Element)nNode);
						if(channel.getDeliverInfo()!=null) throw new ParserConfigurationException("Output channel cannot have a deliver stub.");
						System.out.println("adding output channel: "+channel.getName());
						module.setOutputChannelInfo(channel.getName(), channel);
					}
				}
			}
		}

		nList = doc.getElementsByTagName("input");
		for(int i = 0; i < nList.getLength(); i++){
			nNode = nList.item(i);
			if(nNode.getNodeType() == Node.ELEMENT_NODE){
				eElement = (Element)nNode;
				NodeList channelList = eElement.getElementsByTagName("channel");
				for(int j = 0; j < channelList.getLength(); j++){
					nNode = channelList.item(j);
					if(nNode.getNodeType() == Node.ELEMENT_NODE){
						ChannelInfo channel = readChannelInfo((Element)nNode);
						if(channel.getDeliverInfo()==null) throw new ParserConfigurationException("Input channel needs a deliver stub.");
						System.out.println("adding input channel: "+channel.getName());
						module.setInputChannelInfo(channel.getName(), channel);
					}
				}
			}
		}

		return module;
	}

	public void setInstances(int instances){
		this.instances = instances;
	}

	public int getInstances(){
		return this.instances;
	}
	
	public void setInputChannelInfo(String name, ChannelInfo inChannelInfo){
		this.inChannelInfo.put(name, inChannelInfo);
	}

	public ChannelInfo getInputChannelInfo(String name){
		return this.inChannelInfo.get(name);
	}

	public Map<String, ChannelInfo> getInputChannelInfo(){
		return this.inChannelInfo;
	}

	public void setOutputChannelInfo(String name, ChannelInfo outChannelInfo){
		this.outChannelInfo.put(name, outChannelInfo);
	}

	public ChannelInfo getOutputChannelInfo(String name){
		//Logger.info("GETING: "+name);
		ChannelInfo tmp = this.outChannelInfo.get(name);
		//Logger.info("Channel: "+Json.dumps(tmp));
		return tmp;
	}

	public Map<String, ChannelInfo> getOutputChannelInfo(){
		return this.outChannelInfo;
	}

	public void setFilterInfo(FilterInfo filterInfo){
		this.filterInfo = filterInfo;
	}

	public FilterInfo getFilterInfo(){
		return this.filterInfo;
	}
}
