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

package hws.util;

import java.lang.reflect.Type;

import com.google.gson.Gson;

public class Json {
	private static Gson gson = new Gson();

	// Suppress default constructor for noninstantiability
	private Json() {
		throw new AssertionError();
	}

	public static <T> String dumps(T obj){
		return gson.toJson(obj);
	}

	public static <T> String dumps(T obj, Type type){
		return gson.toJson(obj,type);
	}

	public static <T> T loads(String json, Class<T> classOfType){
		return gson.fromJson(json, classOfType);
	}

	public static <T> T loads(String json, Type type){
		return gson.fromJson(json, type);
	}
}
