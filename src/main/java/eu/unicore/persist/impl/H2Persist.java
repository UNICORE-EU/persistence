package eu.unicore.persist.impl;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.ConnectionPoolDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.engine.Constants;
import org.h2.jdbcx.JdbcDataSource;

import eu.unicore.persist.PersistenceException;
import eu.unicore.persist.PersistenceProperties;

/**
 * H2 database based persistence
 * 
 * @author schuller
 */
public class H2Persist<T> extends PersistImpl<T>{

	private static final Logger logger = LogManager.getLogger("unicore.persistence.H2Persist");

	protected Boolean serverMode=null;

	String connectionURL;

	// setup periodic cache cleanup due to h2 issue with increasing cache memory use
	private static final Set<H2Persist<?>> instances = new HashSet<>();
	
	private static final Thread cleanupThread;
	
	private static long cacheCleanupPeriod = 10; // minutes
	
	static{
		cleanupThread = new Thread(()->
			{
				logger.debug("Will reset H2 caches every <{}> minutes.", cacheCleanupPeriod);
				while(true){
					try{
						Thread.sleep(cacheCleanupPeriod);
						logger.debug("Resetting H2 caches");
						for(H2Persist<?> p : instances){
							p.resetCache();
							Thread.sleep(1000*60*cacheCleanupPeriod);
						}
					}catch(Exception ie){}
				}
			}, "H2-Cache-Cleanup");
		cleanupThread.setDaemon(true);
		cleanupThread.start();
	}
	
	public H2Persist(){}

	@Override
	public void init()throws PersistenceException, SQLException {
		super.init();
		if(config==null)return;
		resetCache();
		instances.add(this);
	}

	@Override
	public void shutdown() throws PersistenceException, SQLException {
		instances.remove(this);
		super.shutdown();
	}
	
	protected void resetCache() throws PersistenceException, SQLException {
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try(Statement s = conn.createStatement()){
					String cacheSize = config.getSubkeyValue(PersistenceProperties.H2_CACHESIZE, pd.getTableName());
					s.execute("SET CACHE_SIZE "+cacheSize);
					logger.debug("Set H2 cache size to {} kb.", cacheSize);
				}
			}
		}
	}
	
	@Override
	protected List<String> getSQLCreateTable() throws PersistenceException, SQLException {
		List<String>cmds = new ArrayList<>();
		String stringType = getSQLStringType();
		cmds.add(String.format("CREATE TABLE IF NOT EXISTS %s (id %s PRIMARY KEY, data %s)",
				pd.getTableName(), stringType, stringType));
		boolean haveTable = tableExists();
		if(pd.getColumns().size()>0){
			for(ColumnDescriptor c: pd.getColumns()){
				if(!haveTable || !columnExists(c.getColumn())){
					cmds.add(String.format("ALTER TABLE %s ADD COLUMN %s %s",
							pd.getTableName(), c.getColumn(), stringType));
				}
			}
		}
		if(!haveTable || !columnExists("CREATED")){
			cmds.add(String.format("ALTER TABLE %s ADD COLUMN CREATED %s NOT NULL DEFAULT '%s'",
					pd.getTableName(), stringType, getTimeStamp()));
		}
		return cmds;
	}

	@Override
	protected String getSQLStringType(){
		return "VARCHAR";
	}

	@Override
	protected String getSQLShutdown(){
		return "SHUTDOWN SCRIPT";
	}

	public Boolean getServerMode() {
		return serverMode;
	}

	public void setServerMode(boolean serverMode) {
		this.serverMode = serverMode;
	}

	private String createConnString(){
		if(connectionURL!=null){
			return connectionURL;
		}

		String tableName=pd.getTableName();
		String dir=null;

		if(serverMode==null){
			serverMode=config==null?false:Boolean.parseBoolean(config.getSubkeyValue(PersistenceProperties.H2_SERVER_MODE, tableName));
		}

		if(config!=null){
			dir=config.getSubkeyValue(PersistenceProperties.DB_DIRECTORY,tableName);
		}

		if(!(new File(dir).isAbsolute())){
			dir="./"+dir;
		}

		String params="DB_CLOSE_ON_EXIT=FALSE";
		String additionalParams=config==null?null:config.getSubkeyValue(PersistenceProperties.H2_OPTIONS, tableName);
		if(additionalParams!=null){
			params+=";"+additionalParams;
		}
		String id=getDatabaseName();
		if(serverMode){
			String host=config==null?"localhost":config.getSubkeyValue(PersistenceProperties.DB_HOST, pd.getTableName());
			String port=config==null?"3306":config.getSubkeyValue(PersistenceProperties.DB_PORT, pd.getTableName());
			connectionURL="jdbc:h2:tcp://"+host+":"+port+"/"+dir+File.separator+id+";AUTO_RECONNECT=TRUE;"+params;
		}else{
			connectionURL= "jdbc:h2:file:"+dir+File.separator+id+";"+params;
		}

		logger.info("Connecting to: "+connectionURL);
		return connectionURL;
	}

	@Override
	protected int getDefaultPort() {
		return  Constants.DEFAULT_TCP_PORT;
	}

	@Override
	protected String getDefaultDriverName(){
		return "org.h2.Driver";
	}

	@Override
	protected ConnectionPoolDataSource getConnectionPoolDataSource(){
		JdbcDataSource ds=new JdbcDataSource();
		ds.setURL(createConnString());
		ds.setUser(getUserName());
		ds.setPassword(getPassword());
		return ds;
	}

	@Override
	protected String getUserName(){
		return "sa";
	}

	@Override
	protected String getPassword(){
		return "";
	}

}
