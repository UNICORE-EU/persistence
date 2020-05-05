package de.fzj.unicore.persist.cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import de.fzj.unicore.persist.PersistenceProperties;

/**
 * clustering support
 * 
 * @author schuller
 */
public class Cluster {

	private static final Logger logger=Logger.getLogger("unicore.persistence."+Cluster.class.getSimpleName());
	
	private static Cluster theDefaultInstance=null;
	
	private final HazelcastInstance hazelcast;
	
	private static final Map<String,Cluster>instances=new HashMap<String, Cluster>();
	
	/**
	 * generate a new cluster instance
	 * @param hz
	 */
	Cluster(HazelcastInstance hz){
		this.hazelcast=hz;
	}
	
	Cluster(File configFile)throws FileNotFoundException{
		System.setProperty("hazelcast.logging.type", "log4j");
		if(configFile==null){
			hazelcast=Hazelcast.newHazelcastInstance(null);
			logger.info("Created cluster instance based on default configuration.");
		}
		else{
			FileSystemXmlConfig config=new FileSystemXmlConfig(configFile);
			hazelcast=Hazelcast.newHazelcastInstance(config);
			logger.info("Created cluster instance based on configuration file "+configFile);
		}
	}
	
	public static synchronized Cluster getInstance(String configFile)throws FileNotFoundException{
		File f=new File(configFile);
		Cluster cluster=instances.get(f.getAbsolutePath());
		if(cluster==null){
			cluster=new Cluster(f);
			instances.put(f.getAbsolutePath(), cluster);
		}
		return cluster;
	}
	
	/**
	 * get the default (per VM) instance
	 * @return <code>null</code> if clustering config file is not specified as system property {@link PersistenceProperties#DB_CLUSTER_CONFIG}
	 */
	public static synchronized Cluster getDefaultInstance()throws FileNotFoundException{
		if(theDefaultInstance==null){
			String configFile=System.getProperty(PersistenceProperties.DB_CLUSTER_CONFIG);
			if(configFile!=null){
				Cluster c=Cluster.getInstance(configFile);
				theDefaultInstance=c;
				logger.info("Created default cluster instance based on configuration file "+configFile);
			}else{
				logger.debug("Clustering disabled, set system property "+PersistenceProperties.DB_CLUSTER_CONFIG+" to the location of the cluster config file.");
			}
		}
		return theDefaultInstance;
	}
	
	/**
	 * get a cluster wide, distributed lock
	 * @param the ID of the lock (must be unique per VM and cluster instance!)
	 */
	public Lock getLock(String key){
		return hazelcast.getLock(key);
	}
	
	/**
	 * get a cluster wide, distributed map
	 * 
	 * @param the ID of the map (must be unique per VM and cluster instance!)
	 * @param key - the class of map key to use
	 * @param value - the class of map value to use
	 */
	public <T,V> Map<T,V> getMap(String name, Class<T> key, Class<V> value){
		return hazelcast.getMap(name);
	}
	
	/**
	 * get a cluster wide, distributed queue
	 * 
	 * @param the ID of the queue (must be unique per VM and cluster instance!)
	 */
	public <T> BlockingQueue<T> getQueue(String key){
		return hazelcast.getQueue(key);
	}
	
	public HazelcastInstance getHazelcast(){
		return hazelcast;
	}

	/**
	 * shutdown this cluster instance
	 */
	public void shutdown(){
		hazelcast.shutdown();
	}
	
	/**
	 * shutdown all cluster activity on the VM
	 */
	public static void shutdownAll(){
		Hazelcast.shutdownAll();
	}
	
}
