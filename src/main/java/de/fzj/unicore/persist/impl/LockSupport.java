package de.fzj.unicore.persist.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import eu.unicore.util.configuration.ConfigurationException;

/**
 * helper for dealing with locks
 * 
 * @author schuller
 */
public class LockSupport {

	// store for locks keyed by the unique ID of the DAO
	private final Map<String, Lock>locks = new HashMap<String, Lock>();

	private final String tableName;

	public LockSupport(String tableName) throws ConfigurationException {
		this.tableName=tableName;
	}

	public final synchronized Lock getOrCreateLock(String id){
		String key = tableName + id;
		Lock l = locks.get(key);
		if(l==null){
			l = new ReentrantLock();
			locks.put(key, l);
		}
		return l;
	}

	public final synchronized Lock getLockIfExists(String id){
		String key = tableName+id;
		return locks.get(key);
	}

	/**
	 * unlock and remove lock
	 * @param id
	 * @return <code>true</code> if a lock existed and was cleaned up
	 */
	public synchronized boolean cleanup(String id){
		Lock l=getLockIfExists(id);
		if(l!=null){
			try{
				l.unlock();
			}catch(Exception me){}
		}
		String key=tableName+id;
		return locks.remove(key)!=null;
	}

	public synchronized void cleanup(){
		if(locks!=null){
			locks.clear();
		}
	}

}
