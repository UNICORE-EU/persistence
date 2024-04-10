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
import eu.unicore.persist.impl.LockSupport;
import eu.unicore.persist.impl.PersistenceDescriptor;
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
	
	public PersistenceProperties getConfig() {
		return config;
	}

	public static synchronized PersistenceFactory get(PersistenceProperties config){
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
		return new PersistenceFactory(realConfig);
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
		if(clazz.startsWith("de.fzj.unicore.persist")) {
			String old = clazz;
			clazz = clazz.replace("de.fzj.unicore", "eu.unicore");
			logger.warn("DEPRECATED class name: {}, use {}", old, clazz);
		}
		Class<? extends Persist<T>>persistClazz=(Class<? extends Persist<T>>)Class.forName(clazz);
		return persistClazz;
	}
	
}
