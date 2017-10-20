/**
 * Copyright 2017 Fondazione Ugo Bordoni
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.fub.bigdataplatform.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Carica un file di proprieta' di default o uno specificato per parametro.
 * 
 * @author Marco Bianchi (mbianchi@fub.it)
 */
public class Configuration {

	/**
	 * Carica un file di proprieta' specificato per parametro.
	 * 
	 * @param propsPath path del file properties da caricare
	 * @return oggetto Properties
	 */
	public static Properties loadProps(String propsPath){
		
		Properties prop = new Properties();
		 
    	try {
    		//prop.load(new FileInputStream(propsPath));    	
    		prop.load(new InputStreamReader(new FileInputStream(propsPath),Charset.forName("UTF-8")));
    	} catch (IOException e) {
    		e.printStackTrace();
        }
    	return prop;
	}
	
	/**
	 * Carica un file di proprieta' di default.
	 * 
	 */
	
	public static Properties loadProps(){
		return loadProps("../etc/system.properties");
	}
	
	
	
}
