package de.fzj.unicore.persist.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.hazelcast.core.DistributedObject;

//import com.hazelcast.

import de.fzj.unicore.persist.cluster.Cluster;
import eu.unicore.util.configuration.ConfigurationException;

/**
 * helper for dealing with locks
 * 
 * @author schuller
 */
public class LockSupport {

	// store for locks keyed by the unique ID of the DAO
	private final Map<String, Lock>locks = new HashMap<String, Lock>();

	private boolean distributed=false;
	private String clusterConfig=null;
	private Cluster cluster;

	private final String tableName;

	private final Lock foo = new ReentrantLock();

	public LockSupport(boolean tryDistributed, String tableName) throws ConfigurationException {
		this.distributed=tryDistributed;
		this.tableName=tableName;
		if(distributed)setupDistributedMode();
	}

	public LockSupport(String clusterConfigFile, String tableName) throws ConfigurationException {
		clusterConfig=clusterConfigFile;
		this.distributed = clusterConfigFile!=null;
		this.tableName=tableName;
		if(distributed)setupDistributedMode();
	}

	public final synchronized Lock getOrCreateLock(String id){
		String key = tableName + id;
		Lock l = null;
		if(distributed){
			l = cluster.getLock(key);
			locks.put(key, foo);
		}
		else{
			l = locks.get(key);
			if(l==null){
				l = new ReentrantLock();
				locks.put(key, l);
			}
		}
		return l;
	}

	public final synchronized Lock getLockIfExists(String id){
		String key = tableName+id;
		if(distributed){
			if(locks.containsKey(key)){
				return cluster.getLock(key);
			}
			else return null;
		}
		else{
			return locks.get(key);
		}
	}

	private void setupDistributedMode() throws ConfigurationException {
		try {
			if(clusterConfig!=null){
				cluster=Cluster.getInstance(clusterConfig);
			}
			else{
				cluster=Cluster.getDefaultInstance();
			}
		} catch(IOException e){
			throw new ConfigurationException("Error setting up Hazelcast cluster", e);
		}
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
			if(distributed){
				if(l instanceof DistributedObject) {
					((DistributedObject)l).destroy();
				}
			}
		}
		String key=tableName+id;
		return locks.remove(key)!=null;
	}

	public synchronized void cleanup(){
		if(locks!=null){
			if(distributed){
				for(String key: locks.keySet()){
					Lock l = locks.get(key);
					if(l instanceof DistributedObject) {
						((DistributedObject)l).destroy();
					}}
			}
			locks.clear();
		}
	}

	public String getClusterConfig() {
		return clusterConfig;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
		if(cluster!=null)distributed=true;
	}

}
