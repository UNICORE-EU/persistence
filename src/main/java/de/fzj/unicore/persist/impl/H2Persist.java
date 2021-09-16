/*********************************************************************************
 * Copyright (c) 2008 Forschungszentrum Juelich GmbH 
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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

import org.apache.logging.log4j.Logger;
import org.h2.jdbcx.JdbcDataSource;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;
import eu.unicore.util.Log;

/**
 * H2 database based persistence
 * 
 * @author schuller
 */
public class H2Persist<T> extends PersistImpl<T>{

	private static final Logger logger = Log.getLogger("unicore.persistence", H2Persist.class);

	protected Boolean serverMode=null;

	String connectionURL;

	// setup periodic cache cleanup due to h2 issue with increasing cache memory use
	private static final Set<H2Persist<?>> instances = new HashSet<>();
	
	private static final Thread cleanupThread;
	
	private static long cacheCleanupPeriod = 10 * 60 * 1000;
	
	static{
		final Runnable r = new Runnable(){
			public void run(){
				logger.debug("Will reset H2 caches every <"+cacheCleanupPeriod/60000+"> minutes.");
				while(true){
					try{
						Thread.sleep(cacheCleanupPeriod);
						logger.debug("Resetting H2 caches");
						for(H2Persist<?> p : instances){
							p.resetCache();
							Thread.sleep(cacheCleanupPeriod/600);
						}
					}catch(Exception ie){}
				}
			}
		};
		cleanupThread = new Thread(r,"H2-Cache-Cleanup");
		cleanupThread.setDaemon(true);
		cleanupThread.start();
	}
	
	public H2Persist(){}

	@Override
	public void init()throws PersistenceException{
		super.init();
		if(config==null)return;
		resetCache();
		instances.add(this);
	}

	@Override
	public void shutdown() throws PersistenceException {
		instances.remove(this);
		super.shutdown();
	}
	
	protected void resetCache() throws PersistenceException {
		Connection conn = getConnection();
		synchronized (conn) {
			try(Statement s = conn.createStatement()){
				String cacheSize = config.getSubkeyValue(PersistenceProperties.H2_CACHESIZE, pd.getTableName());
				s.execute("SET CACHE_SIZE "+cacheSize);
				logger.debug("Set H2 cache size to "+cacheSize+" kb.");
			}
			catch(SQLException ex){
				logger.error("Error initing H2 database",ex);
			}
			finally{
				disposeConnection(conn);
			}
		}
	}
	
	@Override
	public List<String> getSQLCreateTable() throws PersistenceException, SQLException {
		List<String>cmds = new ArrayList<>();
		String type=getSQLStringType();
		cmds.add("CREATE TABLE IF NOT EXISTS "+pd.getTableName()+
				 " (id "+type+" PRIMARY KEY, data "+type+")");
		boolean haveTable = tableExists();
		if(pd.getColumns().size()>0){
			for(ColumnDescriptor c: pd.getColumns()){
				if(!haveTable || !columnExists(c.getColumn())){
					cmds.add("ALTER TABLE "+pd.getTableName()+" ADD COLUMN "+c.getColumn()+" "+type);
				}
			}
		}
		if(!haveTable || !columnExists("CREATED")){
			cmds.add("ALTER TABLE "+pd.getTableName()+" ADD COLUMN CREATED "+type+" NOT NULL DEFAULT '"+getTimeStamp()+"'");
		}
		return cmds;
	}

	@Override
	public String getSQLStringType(){
		return "VARCHAR";
	}

	@Override
	public String getSQLShutdown(){
		return "SHUTDOWN SCRIPT";
	}

	public Boolean getServerMode() {
		return serverMode;
	}

	public void setServerMode(boolean serverMode) {
		this.serverMode = serverMode;
	}

	@Override
	protected synchronized String createConnString(){
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

		if(dir==null){
			try{
				dir=System.getProperty("java.io.tmpdir")+File.separator+String.valueOf(System.currentTimeMillis());
				File f=new File(dir);
				if(!f.mkdir())throw new IOException("Can't create temporary directory");
			}
			catch(IOException ex){
				throw new RuntimeException(ex);
			}
			logger.debug("Using fallback directory as storage: "+dir);
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
	protected String getDriverName(){
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
	protected DataSource getDataSource(){
		return null;
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
