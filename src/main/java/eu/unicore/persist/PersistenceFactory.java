package eu.unicore.persist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.unicore.persist.impl.ClassScanner;
import eu.unicore.persist.impl.H2Persist;
import eu.unicore.util.configuration.ConfigurationException;


/**
 * create persistence handlers
 * 
 * @author schuller
 */
public class PersistenceFactory {

	private static final Logger logger = LogManager.getLogger("unicore.persistence,PersistenceFactory"); 
	
	private final PersistenceProperties config;
	
	private PersistenceFactory(PersistenceProperties config){
		this.config = config;
	}

	public static synchronized PersistenceFactory get(PersistenceProperties config){
		PersistenceProperties realConfig = null;
		//check if we must merge properties from an additional config file
		File cFile=config==null? null : config.getFileValue(PersistenceProperties.FILE, false);
		if(cFile!=null){
			try(InputStream in = new FileInputStream(cFile)){
				Properties props = new Properties();
				props.load(in);
				props.putAll(config.getRawProperties());
				realConfig = new PersistenceProperties(props);
				logger.info("Reading persistence configuration from <"+cFile+">");
			}catch(IOException ex){
				throw new ConfigurationException("Can't read properties file <"+cFile+">",ex);
			}
		}else{
			realConfig = config;
		}
		return new PersistenceFactory(realConfig);
	}

	/**
	 * returns the effective configuration
	 */
	public PersistenceProperties getConfig() {
		return config;
	}

	/**
	 * create a persistence handler for the given class
	 *
	 * @param <T> - the type of java class to be persisted
	 * @param daoClass - the Java class to be persisted
	 * @return {@link Persist} implementation
	 * @throws PersistenceException
	 */
	public <T> Persist<T> getPersist(Class<T> daoClass) throws PersistenceException{
		return getPersist(daoClass, null);
	}

	/**
	 * create a persistence handler for the given class storing data in the named table
	 *
	 * @param <T> - the type of java class to be persisted
	 * @param daoClass - the Java class to be persisted
	 * @param tableName - if null, it will be inferred from the daoClass
	 * @return {@link Persist} implementation
	 * @throws PersistenceException
	 */
	public <T> Persist<T> getPersist(Class<T> daoClass, String tableName) throws PersistenceException{
		try{
			return configurePersist(daoClass, getPersistClass(daoClass, tableName), tableName);
		}catch(Exception e){
			throw new PersistenceException(e);
		}
	}

	/**
	 * create an instance of the persistence class
	 * 
	 * @param daoClass
	 * @param implementation
	 * @param tableName
	 * @throws Exception
	 */
	public <T> Persist<T> configurePersist(Class<T> daoClass, Class<? extends Persist<T>> implementation, String tableName)
			throws Exception {
		Persist<T>p = implementation.getConstructor(Class.class, String.class).newInstance(daoClass, tableName);
		p.setConfigSource(config);
		p.init();
		return p;
	}

	@SuppressWarnings("unchecked")
	<T> Class<? extends Persist<T>> getPersistClass(Class<T> daoClass, String tableName)throws ClassNotFoundException{
		if(tableName==null) {
			tableName = ClassScanner.getTableName(daoClass);
		}
		String clazz = config.getSubkeyValue(PersistenceProperties.DB_IMPL, tableName);
		if(clazz==null){
			clazz = H2Persist.class.getName();
		}
		if(clazz.startsWith("de.fzj.unicore.persist")) {
			String old = clazz;
			clazz = clazz.replace("de.fzj.unicore", "eu.unicore");
			logger.warn("DEPRECATED class name: {}, use {}", old, clazz);
		}
		return (Class<? extends Persist<T>>)Class.forName(clazz);
	}
	
}
