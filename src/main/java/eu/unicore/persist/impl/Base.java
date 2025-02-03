package eu.unicore.persist.impl;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

	private static final Logger logger = LogManager.getLogger("unicore.persistence.Base");

	protected final Class<T>daoClass;

	protected final PersistenceDescriptor pd;

	protected PersistenceProperties config;

	private Cache<String,T> cache;

	private Boolean caching = null;

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
		this.config=configSource;
	}

	@Override
	public void init()throws PersistenceException, SQLException {
		String table = pd.getTableName();
		try {
			// check that we can load the driver class
			Class.forName(getDriverName());
			initLockSupport();
			initCache();
			createMarshaller();
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

	@Override
	public T getForUpdate(String id)throws PersistenceException, SQLException, InterruptedException{
		try{
			return getForUpdate(id,Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		}catch(TimeoutException cannotOccur){
			throw new PersistenceException();
		}
	}

	@Override
	public void lock(String id, long timeout, TimeUnit unit)throws TimeoutException, InterruptedException {
		Lock lock=lockSupport.getOrCreateLock(id);
		if(!lock.tryLock(timeout, unit)){
			throw new TimeoutException("Time out reached: lock for table= "+pd.getTableName()
										+" uid=<"+id+"> could not be acquired");
		}
	}

	@Override
	public T getForUpdate(String id, long timeout, TimeUnit unit)throws PersistenceException, SQLException, TimeoutException, InterruptedException{
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
			throw new TimeoutException("Time out reached: lock for table= "+pd.getTableName()
										+" uid=<"+id+"> could not be acquired");
		}
	}

	@Override
	public T tryGetForUpdate(String id)throws PersistenceException, SQLException {
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
	@Override
	public T read(String id)throws PersistenceException, SQLException {
		T result=null; 
		if(caching){
			T element=cache.getIfPresent(id);
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
	protected abstract T _read(String id) throws PersistenceException, SQLException;

	@Override
	public void write(T dao)throws PersistenceException, SQLException, IllegalStateException {
		String id=pd.getID(dao);
		Lock lock=lockSupport.getLockIfExists(id);
		if(lock!=null && !lock.tryLock())throw new IllegalStateException("No write permission has been acquired!");
		try{
			logger.debug("[{}] Persisting <{}>", pd.getTableName(), id);
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

	/**
	 * write an instance to storage
	 */
	protected abstract void _write(T dao, String id)throws PersistenceException, SQLException;

	@Override
	public void unlock(T dao)throws PersistenceException{
		String id=pd.getID(dao);
		Lock lock=lockSupport.getLockIfExists(id);
		if(lock!=null)lock.unlock();
	}

	@Override
	public void remove(String id)throws PersistenceException, SQLException {
		try{
			delete(id);
		}finally{
			lockSupport.cleanup(id);
		}
	}
	
	@Override
	public void delete(String id)throws PersistenceException, SQLException{
		if(caching){
			cache.invalidate(id);
		}
		_remove(id);
	}

	
	protected abstract void _remove(String id) throws PersistenceException, SQLException;
	
	@Override
	public void removeAll()throws PersistenceException, SQLException{
		if(caching){
			cache.invalidateAll();
		}
		try{
			_removeAll();
		}finally{
			lockSupport.cleanup();
		}
	}

	protected abstract void _removeAll() throws PersistenceException, SQLException;

	public Class<T> getDaoClass(){
		return daoClass;
	}

	/**
	 * create a true copy of the supplied object
	 */
	protected T copy(T obj) {
		return marshaller.deserialize(marshaller.serialize(obj));
	}

	@Override
	public void setCaching(boolean value){
		caching=value;
	}

	public long getCacheHits() {
		return cacheHits;
	}

	public String getStatusMessage(){
		return "OK";
	}

}
