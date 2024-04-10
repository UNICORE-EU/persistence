package eu.unicore.persist;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import eu.unicore.persist.impl.LockSupport;
import eu.unicore.persist.impl.PersistenceDescriptor;

/**
 * Client interface to the persistence layer<br/>
 * 
 * Entities can be stored and read from the persistence layer, in read-only mode
 * or update mode. When reading an entity in update mode, the implementation assures that
 * a new "copy" is created, and other clients currently using the same entity for reading
 * are not affected.
 * 
 * @param <T> the type of persistent Java entity
 * 
 * @author schuller
 */
public interface Persist<T> {

	/**
	 * get an object from the persistence layer for read-only access
	 * 
	 * @param id
	 * @return T
	 */
	public T read(String id)throws PersistenceException, SQLException;
	
	/**
	 * Get an object from the persistence layer and aquire a write lock<br/>
	 * 
	 * NOTE: The default timeout for lock aquisition is INFINITE<br/>
	 *  
	 * This must always be called using the following pattern:
	 * <pre>
	 *  
	 *   Foo f=persist.getForUpdate(id);
	 *   try{
	 *    ...
	 *   }
	 *   finally{
	 *   	//write it or unlock it
	 *      if(OK) persist.write(f);
	 *      else persist.unlock(f);
	 *   }
	 * </pre> 
	 * 
	 * The getForUpdate/unlock operations MUST be called in the same thread! <br/>
	 * 
	 * 
	 * In case the requested object does not exist, <code>null</code> is returned, and no lock is aquired
	 * 
	 * @param id
	 * @return the requested object , or <code>null</code> if it does not exist
	 * @throws PersistenceException - if errors related to the storage occur
	 * @throws InterruptedException - if the current thread is interrupted while waiting for lock aquisition
	 */
	public T getForUpdate(String id)throws PersistenceException, SQLException, InterruptedException;
	
	/**
	 * get an object from the persistence layer and aquire a write lock. 
	 * Will wait for the given amount of time to aquire the lock. <br/>
	 * 
	 * This must always be called using the following pattern:
	 * <pre>
	 *  
	 *   Foo f=persist.getForUpdate(id,timeout,units);
	 *   try{
	 *    ...
	 *   }
	 *   finally{
	 *   	//write it or unlock it
	 *      if(OK) persist.write(f);
	 *      else persist.unlock(f);
	 *   }
	 * </pre> 
	 * 
	 * The getForUpdate/unlock operations MUST be called in the same thread! <br/>
	 * 
	 * 
	 * In case the requested object does not exist, <code>null</code> is returned, and NO lock is aquired
	 * 
	 * @param id - the id of the object to get
	 * @param timeout - time out value
	 * @param unit - time out unit
	 * @return the requested object , or <code>null</code> if it does not exist
	 * @throws PersistenceException - if errors related to the storage occur
	 * @throws TimeoutException - if the lock cannot be aquired in the given timeout 
	 * @throws InterruptedException - if the current thread is interrupted while waiting for lock aquisition
	 */
	public T getForUpdate(String id,long timeout, TimeUnit unit)throws PersistenceException, SQLException, TimeoutException, InterruptedException;
	
	/**
	 * get an object from the persistence layer and aquire a write lock. 
	 * This method returns immediately returning <code>null</code> if the lock cannot be aquired.<br/>
	 * 
	 * This must always be called using the following pattern:
	 * <pre>
	 *  
	 *   Foo f=persist.tryGetForUpdate(id);
	 *   try{
	 *    ...
	 *   }
	 *   finally{
	 *   	//write it or unlock it
	 *      if(OK) persist.write(f);
	 *      else persist.unlock(f);
	 *   }
	 * </pre> 
	 * 
	 * The getForUpdate/unlock operations MUST be called in the same thread! <br/>
	 * 
	 * In case the requested object does not exist, <code>null</code> is returned, and NO lock is aquired
	 * 
	 * @param id - the id of the object to get
	 * @return the requested object , or <code>null</code> if it does not exist
	 * @throws PersistenceException - if errors related to the storage occur
	 */
	public T tryGetForUpdate(String id)throws PersistenceException, SQLException, TimeoutException,InterruptedException;
	
	public void lock(String id, long timeout, TimeUnit unit)throws TimeoutException, InterruptedException;
	
	public void unlock(T dao)throws PersistenceException;
	
	/**
	 * insert or update
	 * 
	 * @param dao
	 */
	public void write(T dao)throws PersistenceException, SQLException;
	

	/**
	 * delete the entry, but keep the lock.
	 *
	 * @param id of the entry to remove
	 */
	public void delete(String id)throws PersistenceException, SQLException;
	
	/**
	 * delete the entry and remove its lock from {@link LockSupport}
	 * 
	 * @param id of the entry to remove
	 */
	public void remove(String id)throws PersistenceException, SQLException;
	
	/**
	 * delete all entries
	 */
	public void removeAll()throws PersistenceException, SQLException;
	
	
	/**
	 * get all IDs in (descending) order of creation time (ie., latest first)
	 */
	public List<String> getIDs()throws PersistenceException, SQLException;
	
	/**
	 * get all IDs with the specified ordering
	 * @param oldestFirst - if true, the entries will be ordered by ascending creation time
	 */
	public default List<String> getIDs(boolean oldestFirst)throws PersistenceException, SQLException{
		return getIDs();
	}
	
	/**
	 * get a list of dao IDs where a column has a certain value
	 * 
	 * @param column - the column name
	 * @param value - the value
	 */
	public List<String> getIDs(String column, Object value)throws PersistenceException, SQLException;
	
	
	/**
	 * get a list of dao IDs where a column matches certain values (i.e. using SQL 'LIKE')
	 * 
	 * @param orMode - if true, any of the values has to match, if false, all of them have to match
	 * @param column - the column name
	 * @param values - values to match
	 */
	public List<String> findIDs(boolean orMode, String column, String... values)throws PersistenceException, SQLException;
	
	/**
	 * get a list of dao IDs where a column matches certain values (i.e. using SQL 'LIKE')
	 * 
	 * @param column - the column name
	 * @param values - if giving more than one value, the search uses AND to combine
	 */
	public List<String> findIDs(String column, String... values)throws PersistenceException, SQLException;
	
	/**
	 * get a map containing the values of one particular column
	 * 
	 * @param column - the column name
	 * @return a Map<String,String> of column values keyed with the ID
	 */
	public Map<String,String> getColumnValues(String column)throws PersistenceException, SQLException;
	
	/**
	 * get the number of rows
	 * 
	 * @return the number of rows
	 * @throws PersistenceException
	 */
	public int getRowCount()throws PersistenceException, SQLException;
	
	/**
	 * get the number of rows where a column has a certain value
	 * 
	 * @param column
	 * @param value
	 * @return row count
	 * @throws PersistenceException
	 */
	public int getRowCount(String column, Object value)throws PersistenceException, SQLException;
	
	
	//lifecycle methods

	public void setDaoClass(Class<?>daoClass);
	
	public void setConfigSource(PersistenceProperties configSource);
	

	/**
	 * allows to explicitly set the {@link PersistenceDescriptor}. If not set, it will be
	 * created lazily by evaluating annotations on the persisted class
	 */
	public void setPersistenceDescriptor(PersistenceDescriptor pd);

	public LockSupport getLockSupport();

	/**
	 * allows to explicitly set the {@link LockSupport}. If not set, it will be created lazily
	 */
	public void setLockSupport(LockSupport locks);
	
	public void init() throws PersistenceException, SQLException;
	
	public void shutdown()throws PersistenceException, SQLException;
	
	public void setCaching(boolean value);

	/**
	 * Purge persistent data (optional operation!)<br/>
	 * Data will be physically deleted, so use VERY carefully.
	 * In addition to the effect of #removeAll(), any remaining artifacts 
	 * like DB tables or directories will be removed by this operation.
	 */
	public void purge();
	
}
