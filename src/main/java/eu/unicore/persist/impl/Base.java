package eu.unicore.persist.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import eu.unicore.persist.ObjectMarshaller;
import eu.unicore.persist.Persist;
import eu.unicore.persist.PersistenceException;
import eu.unicore.persist.PersistenceProperties;
import eu.unicore.util.configuration.ConfigurationException;

/**
 * Persistence base class - handles locking, caching, indexing and basic initialisation
 * 
 * @param <T> -  the Java entity type
 * 
 * @author schuller
 */
public abstract class Base<T> implements Persist<T>{

	protected final Class<T> daoClass;

	protected final PersistenceDescriptor pd;

	protected PersistenceProperties config = new PersistenceProperties();

	private Cache<String,T> cache;

	private Boolean caching = Boolean.FALSE;

	private LockSupport lockSupport;

	private long cacheHits = 0;

	protected ObjectMarshaller<T> marshaller;

	public Base(Class<T>daoClass, String tableName){
		this.daoClass = daoClass;
		this.pd = PersistenceDescriptor.get(daoClass);
		if(tableName!=null) {
			pd.setTableName(tableName);
		}
	}

	@Override
	public void setConfigSource(PersistenceProperties configSource){
		this.config = configSource;
	}

	@Override
	public void init()throws PersistenceException {
		String table = pd.getTableName();
		try {
			// check that we can load the driver class
			Class.forName(getDriverName());
			initLockSupport();
			initCache();
			createMarshaller();
		} catch (Exception e) {
			throw new PersistenceException("Error setting up persistence implementation for table <"+table+">", e);
		}
	}

	/**
	 * return the name of the class required for the implementation
	 */
	protected abstract String getDriverName();

	protected void createMarshaller(){
		marshaller = new JSONMarshaller<T>(daoClass);
	}

	@Override
	public LockSupport getLockSupport() {
		return lockSupport;
	}

	@Override
	public void setLockSupport(LockSupport lockSupport) {
		this.lockSupport = lockSupport;
	}

	private void initLockSupport() throws ConfigurationException {
		if(lockSupport!=null)return;
		lockSupport = new LockSupport(pd.getTableName());
	}

	/**
	 * initialise the cache. If not already set using {@link #setCaching(boolean)}, the
	 * cache is initialised if the per-table property  {@link PersistenceProperties#DB_CACHE_ENABLE} is set to "true"
	 */
	protected synchronized void initCache(){
		boolean cacheEnabled = Boolean.parseBoolean(config.getSubkeyValue(PersistenceProperties.DB_CACHE_ENABLE, pd.getTableName()));
		if( Boolean.TRUE.equals(caching) || cacheEnabled ){
			caching = Boolean.TRUE;
			int cacheMaxSize = config.getSubkeyIntValue(PersistenceProperties.DB_CACHE_MAX_SIZE,pd.getTableName());
			cache = CacheBuilder.newBuilder()
					.maximumSize(cacheMaxSize)
					.expireAfterAccess(3600, TimeUnit.SECONDS)
					.expireAfterWrite(3600, TimeUnit.SECONDS)
					.softValues()
					.build();
		}
	}

	@Override
	public T getForUpdate(String id)throws PersistenceException, InterruptedException{
		try{
			return getForUpdate(id,Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		}catch(TimeoutException te){
			throw new PersistenceException(te);
		}
	}

	@Override
	public void lock(String id, long timeout, TimeUnit unit)throws TimeoutException, InterruptedException {
		Lock lock = lockSupport.getOrCreateLock(id);
		if(!lock.tryLock(timeout, unit)){
			throw new TimeoutException("Time out reached: lock for "+
					pd.getTableName()+":"+id+" could not be acquired");
		}
	}

	@Override
	public T getForUpdate(String id, long timeout, TimeUnit unit)throws PersistenceException, TimeoutException, InterruptedException{
		Lock lock = lockSupport.getOrCreateLock(id);
		T result = null;
		if(lock.tryLock(timeout, unit)){
			try{
				result = read(id);
			}finally{
				if(result==null){
					lock.unlock();
				}
			}
			return result;
		}
		else {
			throw new TimeoutException("Time out reached: lock for "
					+pd.getTableName()+":"+id+" could not be acquired");
		}
	}

	@Override
	public T tryGetForUpdate(String id)throws PersistenceException {
		Lock lock = lockSupport.getOrCreateLock(id);
		T result = null;
		if(lock.tryLock()){
			try{
				result = read(id);
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
	@Override
	public T read(String id)throws PersistenceException {
		T result = null;
		if(caching){
			T element = cache.getIfPresent(id);
			if(element!=null){
				cacheHits++;
				return copy(element);
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

	/**
	 * read an instance from storage
	 */
	protected abstract T _read(String id) throws PersistenceException;

	@Override
	public void write(T dao)throws PersistenceException, IllegalStateException {
		String id = pd.getID(dao);
		Lock lock = lockSupport.getLockIfExists(id);
		if(lock!=null && !lock.tryLock())throw new IllegalStateException("No write permission has been acquired!");
		try{
			_write(dao, id);
			if(caching){
				try{
					cache.put(pd.getID(dao),copy(dao));
				}catch(Exception cn){}
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

	/**
	 * write an instance to storage
	 */
	protected abstract void _write(T dao, String id)throws PersistenceException;

	@Override
	public void unlock(T dao)throws PersistenceException{
		String id = pd.getID(dao);
		Lock lock = lockSupport.getLockIfExists(id);
		if(lock!=null)lock.unlock();
	}

	@Override
	public void remove(String id)throws PersistenceException {
		try{
			delete(id);
		}finally{
			lockSupport.cleanup(id);
		}
	}

	@Override
	public void delete(String id)throws PersistenceException {
		if(caching){
			cache.invalidate(id);
		}
		_remove(id);
	}

	protected abstract void _remove(String id) throws PersistenceException;

	@Override
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

	/**
	 * create a true copy of the supplied object
	 */
	protected T copy(T obj) {
		return marshaller.deserialize(marshaller.serialize(obj));
	}

	@Override
	public void setCaching(boolean caching){
		this.caching = caching;
	}

	public long getCacheHits() {
		return cacheHits;
	}

}