/*********************************************************************************
 * Copyright (c) 2016 Forschungszentrum Juelich GmbH 
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

package de.fzj.unicore.persist.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import de.fzj.unicore.persist.ObjectMarshaller;
import de.fzj.unicore.persist.Persist;
import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;
import eu.unicore.util.Log;
import eu.unicore.util.configuration.ConfigurationException;

/**
 * Persistence base class - handles locking, caching, indexing and basic initialisation
 * 
 * @param <T> -  the Java entity type
 * 
 * @author schuller
 */
public abstract class Base<T> implements Persist<T>{

	private static final Logger logger = Log.getLogger("unicore.persistence", Base.class);

	protected Class<T>daoClass;

	protected PersistenceDescriptor pd;

	protected PersistenceProperties config;

	private Cache<String,T> cache;

	private Boolean caching = null;

	private LockSupport lockSupport;

	private long cacheHits = 0;

	protected ObjectMarshaller<T> marshaller;

	private final Map<String, Metric> metricSet = new HashMap<>();
	
	public Base(){
	}

	public void setConfigSource(PersistenceProperties configSource){
		this.config=configSource;
	}

	public void setPersistenceDescriptor(PersistenceDescriptor pd){
		this.pd=pd;
	}

	public void init()throws PersistenceException{
		if(pd==null)pd=PersistenceDescriptor.get(daoClass);
		String table=pd.getTableName();

		try {
			// check that we can load the driver class
			Class.forName(getDriverName());
			
			initLockSupport();

			initCache();

			createMarshaller();
			
			createMetrics();
			
		} catch (Exception e) {
			logger.error("Error setting up persistence implementation for table <"+table+">",e);
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * return the name of the class required for the implementation
	 */
	protected abstract String getDriverName();
	
	protected void createMarshaller(){
		marshaller = new JSONMarshaller<T>(daoClass);
	}

	public LockSupport getLockSupport() {
		return lockSupport;
	}

	@Override
	public void setLockSupport(LockSupport lockSupport) {
		this.lockSupport = lockSupport;
	}

	private void initLockSupport() throws ConfigurationException {
		if(lockSupport!=null)return;

		String table=pd.getTableName();
		//init lock handler 
		boolean distributed=config==null? false : 
			Boolean.parseBoolean(config.getSubkeyValue(PersistenceProperties.DB_LOCKS_DISTRIBUTED, table));
		if(distributed){
			String configFile=config.getSubkeyValue(PersistenceProperties.DB_CLUSTER_CONFIG, table);
			if(configFile!=null){
				lockSupport=new LockSupport(configFile,table);
			}
			else{
				lockSupport=new LockSupport(true,table);
			}
		}
		else{
			lockSupport=new LockSupport(false,table);
		}
	}

	/**
	 * initialise the cache. If not already set using {@link #setCaching(boolean)}, the
	 * cache is initialised if the per-table property  {@link PersistenceProperties#DB_CACHE_ENABLE} is set to "true"
	 */
	protected synchronized void initCache(){
		boolean cacheEnabled=config==null?true:Boolean.parseBoolean(config.getSubkeyValue(PersistenceProperties.DB_CACHE_ENABLE, pd.getTableName()));
		if( Boolean.TRUE.equals(caching) || (config!=null && cacheEnabled )){
			caching=Boolean.TRUE;
			String defaultCacheSize="10";
			String cacheMaxSizeS=config!=null?config.getSubkeyValue(PersistenceProperties.DB_CACHE_MAX_SIZE,pd.getTableName()):defaultCacheSize;
			int cacheMaxSize = Integer.parseInt(cacheMaxSizeS);
			cache = CacheBuilder.newBuilder()
					.maximumSize(cacheMaxSize)
					.expireAfterAccess(3600, TimeUnit.SECONDS)
					.expireAfterWrite(3600, TimeUnit.SECONDS)
					.softValues()
					.build();
		}
		else{
			caching=Boolean.FALSE;
		}
	}

	public void flush(){

	}


	public T getForUpdate(String id)throws PersistenceException,InterruptedException{
		try{
			return getForUpdate(id,Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		}catch(TimeoutException cannotOccur){
			throw new PersistenceException();
		}
	}

	public void lock(String id, long timeout, TimeUnit unit)throws TimeoutException, InterruptedException {
		Lock lock=lockSupport.getOrCreateLock(id);
		if(!lock.tryLock(timeout, unit)){
			String msg="Time out reached: lock for table= "+pd.getTableName()+" uid=<"+id+"> could not be acquired";
			throw new TimeoutException(msg);
		}
	}

	public T getForUpdate(String id, long timeout, TimeUnit unit)throws PersistenceException, TimeoutException, InterruptedException{
		Lock lock=lockSupport.getOrCreateLock(id);
		T result=null;
		if(lock.tryLock(timeout, unit)){
			try{
				result=read(id);
				return result;
			}finally{
				if(result==null){
					lock.unlock();
				}
			}
		}
		else {
			String msg="Time out reached: lock for table= "+pd.getTableName()+" uid=<"+id+"> could not be acquired";
			throw new TimeoutException(msg);
		}
	}

	public T tryGetForUpdate(String id)throws PersistenceException{
		Lock lock=lockSupport.getOrCreateLock(id);
		T result=null;
		if(lock.tryLock()){
			try{
				result=read(id);
			}
			finally{
				if(result==null){
					lock.unlock();
				}
			}
		}
		return result;
	}

	/**
	 * read an instance from storage
	 * 
	 * @param id - the ID of the instance to read
	 * @return
	 * @throws PersistenceException
	 */
	public T read(String id)throws PersistenceException {
		T result=null; 
		if(caching){
			T element=cache.getIfPresent(id);
			if(element!=null){
				cacheHits++;
				try{
					return copy(element);
				}catch(IOException ex){
					throw new PersistenceException(makeErrorMessage(id, ex));
				}
			}
		}
		result = _read(id);
		if(caching && result!=null){
			try{
				cache.put(id,copy(result));
			}catch(Exception cn){}
		}
		return result;
	}
	
	protected abstract T _read(String id) throws PersistenceException;

	public void write(T dao)throws PersistenceException,IllegalStateException{
		String id=pd.getID(dao);
		Lock lock=lockSupport.getLockIfExists(id);
		if(lock!=null && !lock.tryLock())throw new IllegalStateException("No write permission has been acquired!");
		try{
			if(logger.isDebugEnabled())logger.debug("["+pd.getTableName()+"] Persisting <"+id+">");
			_write(dao, id);
			if(caching){
				try{
					cache.put(pd.getID(dao),copy(dao));
				}catch(Exception cn){
					throw new PersistenceException("Error updating cache!",cn);
				}
			}
		}finally{
			if(lock!=null){
				lock.unlock();
				try{
					// if we got it with getForUpdate(), we want to clear the lock
					lock.unlock();
				}catch(Exception me){}
			}
		}
	}

	protected abstract void _write(T dao, String id)throws PersistenceException;

	public void unlock(T dao)throws PersistenceException{
		String id=pd.getID(dao);
		Lock lock=lockSupport.getLockIfExists(id);
		if(lock!=null)lock.unlock();
	}

	public void remove(String id)throws PersistenceException{
		if(caching){
			cache.invalidate(id);
		}
		try{
			_remove(id);
		}finally{
			lockSupport.cleanup(id);
		}
	}
	
	protected abstract void _remove(String id) throws PersistenceException;
	
	public void removeAll()throws PersistenceException{
		if(caching){
			cache.invalidateAll();
		}
		try{
			_removeAll();
		}finally{
			lockSupport.cleanup();
		}
	}

	protected abstract void _removeAll() throws PersistenceException;

	@SuppressWarnings("unchecked")
	public void setDaoClass(Class<?>daoClass){
		this.daoClass=(Class<T>)daoClass;
	}

	public Class<T> getDaoClass(){
		return daoClass;
	}

	/**
	 * create a true copy of the supplied object using serialisation.<br/>
	 * Careful: this will aquire a lock on the object due to a 
	 * strange Java bug, see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6178997
	 * @param obj
	 * @return a true copy of obj
	 */
	protected T copy(T obj)throws IOException, PersistenceException{
		return marshaller.deserialize(marshaller.serialize(obj));
	}

	public void setCaching(boolean value){
		caching=value;
	}

	public Boolean getCaching(){
		return caching;
	}

	public String makeErrorMessage(String id, Throwable cause){
		StringBuilder sb=new StringBuilder();
		sb.append("Error for table=").append(pd.getTableName());
		sb.append(" uid=").append(id).append(" because of: ");

		String message=null;
		String type=null;type=cause.getClass().getName();
		do{
			type=cause.getClass().getName();
			message=cause.getMessage();
			cause=cause.getCause();
		}
		while(cause!=null);

		if(message!=null)sb.append(type).append(": ").append(message);
		else sb.append(type);

		return sb.toString();
	}

	public long getCacheHits() {
		return cacheHits;
	}

	public String getStatusMessage(){
		return "OK";
	}
	
	protected void createMetrics(){
		Gauge<String> g = new Gauge<String>() {
			@Override
			public String getValue() {
				return getStatusMessage();
			}
		};
		metricSet.put("unicore.persist."+pd.getTableName(), g);
	}
	
	@Override
	public Map<String,Metric>getMetrics() {
		return metricSet;
	}

}
