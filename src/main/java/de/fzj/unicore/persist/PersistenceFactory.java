/*********************************************************************************
 * Copyright (c) 2008 Forschungszentrum Juelich GmbH 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the disclaimer at the end. Redistributions in
 * binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution.
 * 
 * (2) Neither the name of Forschungszentrum Juelich GmbH nor the names of its 
 * contributors may be used to endorse or promote products derived from this 
 * software without specific prior written permission.
 * 
 * DISCLAIMER
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *********************************************************************************/
 
package de.fzj.unicore.persist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.codahale.metrics.MetricRegistry;

import de.fzj.unicore.persist.impl.ClassScanner;
import de.fzj.unicore.persist.impl.H2Persist;
import de.fzj.unicore.persist.impl.LockSupport;
import de.fzj.unicore.persist.impl.PersistenceDescriptor;
import eu.unicore.util.Log;
import eu.unicore.util.configuration.ConfigurationException;


/**
 * create persistence handlers
 * 
 * @author schuller
 */
public class PersistenceFactory {

	private static final Logger logger=Log.getLogger(Log.PERSISTENCE,PersistenceFactory.class); 
	
	private final PersistenceProperties config;
	
	private final MetricRegistry metricRegistry;
	
	private PersistenceFactory(PersistenceProperties config, MetricRegistry metricRegistry){
		this.config = config;
		this.metricRegistry = metricRegistry;
	}
	
	public PersistenceProperties getConfig() {
		return config;
	}

	public static synchronized PersistenceFactory get(PersistenceProperties config){
		return get(config, null);
	}

	public static synchronized PersistenceFactory get(PersistenceProperties config, MetricRegistry metricRegistry){
		PersistenceProperties realConfig = null;
		//check if we must merge properties from an additional config file
		File cFile=config==null? null : config.getFileValue(PersistenceProperties.FILE, false);
		if(cFile!=null){
			try{
				InputStream in=new FileInputStream(cFile);
				Properties props=new Properties();
				try{
					props.load(in);
				}
				finally{
					in.close();
				}
				props.putAll(config.getRawProperties());
				realConfig = new PersistenceProperties(props);
				logger.info("Reading persistence configuration from <"+cFile+">");
			}catch(IOException ex){
				throw new ConfigurationException("Can't read properties file <"+cFile+">",ex);
			}
		}else{
			realConfig = config;
		}
		return new PersistenceFactory(realConfig, metricRegistry);
	}

	/**
	 * create a persistence handler for the given class, based solely on the annotations
	 * present on the class
	 * 
	 * @param <T> - the type of java class to be persisted
	 * @param daoClass - the Java class to be persisted
	 * @return a {@link Persist} implementation
	 * @throws PersistenceException
	 */
	public <T> Persist<T> getPersist(Class<T> daoClass) throws PersistenceException{
		try{
			return configurePersist(daoClass, getPersistClass(daoClass, null));
		}catch(Exception e){
			throw new PersistenceException(e);
		}
	}
	
	/**
	 * create an instance of the persistence class
	 * 
	 * @param <T> -  the type of Java class to persist
	 * @param daoClass - the Java class to persist
	 * @param pd - the {@link PersistenceDescriptor}
	 * @return a configured {@link Persist} class
	 * @throws PersistenceException
	 */
	public <T> Persist<T> getPersist(Class<T> daoClass, PersistenceDescriptor pd)
	   throws PersistenceException{
		try{
			return configurePersist(daoClass,getPersistClass(daoClass, pd), pd);
		}catch(Exception e){
			throw new PersistenceException(e);
		}
	}

	/**
	 * create an instance of the persistence class
	 * 
	 * @param <T> -  the type of Java class to persist
	 * @param daoClass - the Java class to persist
	 * @param implementation - the persistence implementation
	 * @param pd - the {@link PersistenceDescriptor}
	 * @return a configured {@link Persist} class
	 * @throws Exception
	 */
	public <T> Persist<T> configurePersist(Class<T> daoClass, Class<? extends Persist<T>> implementation, PersistenceDescriptor pd)
	   throws Exception{
		return configurePersist(daoClass, implementation, pd, null);
	}
	
	/**
	 * create an instance of the persistence class
	 * 
	 * @param daoClass
	 * @param implementation
	 * @param pd
	 * @param locks
	 * @throws Exception
	 */
	public <T> Persist<T> configurePersist(Class<T> daoClass, Class<? extends Persist<T>> implementation, PersistenceDescriptor pd, LockSupport locks)
			throws Exception {
		Persist<T>p = implementation.getConstructor().newInstance();
		p.setConfigSource(config);
		p.setPersistenceDescriptor(pd);
		p.setDaoClass(daoClass);
		p.setLockSupport(locks);
		p.init();
		if(metricRegistry!=null){
			metricRegistry.registerAll(p);
		}
		return p;
	}

	/**
	 * create an instance of the persistence class
	 * @param <T> -  the type of Java class to persist
	 * @param daoClass - the Java class to persist
	 * @param implementation - the persistence implementation
	 * @return a configured {@link Persist} class
	 * @throws Exception
	 */
	public <T> Persist<T> configurePersist(Class<T> daoClass, Class<? extends Persist<T>> implementation)
	   throws Exception {
		return configurePersist(daoClass, implementation, null);
	}
	
	@SuppressWarnings("unchecked")
	protected <T> Class<? extends Persist<T>> getPersistClass(Class<T> daoClass, PersistenceDescriptor pd)throws ClassNotFoundException{
		String tableName = null;
		if(pd!=null)tableName = pd.getTableName();
		if(tableName == null)tableName=ClassScanner.getTableName(daoClass);
		
		String clazz=config.getSubkeyValue(PersistenceProperties.DB_IMPL,tableName);
		if(clazz==null){
			clazz=H2Persist.class.getName();
		}
		Class<? extends Persist<T>>persistClazz=(Class<? extends Persist<T>>)Class.forName(clazz);
		return persistClazz;
	}
	
}
