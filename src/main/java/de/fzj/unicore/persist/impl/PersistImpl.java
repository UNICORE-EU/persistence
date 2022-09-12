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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.ConnectionPoolDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;
import de.fzj.unicore.persist.util.Pool;


/**
 * JDBC based persistence implementation, also dealing with JDBC connection pooling
 * 
 * @param <T> -  the Java entity type
 * 
 * @author schuller
 */
public abstract class PersistImpl<T> extends SQL<T> {

	private static final Logger logger = LogManager.getLogger("unicore.persistence.PersistImpl");

	protected String databaseName=null;

	// only for logging purposes
	protected String connectionURL = "";

	public PersistImpl(){
		super();
	}

	public void init()throws PersistenceException, SQLException {
		super.init();
		String table=pd.getTableName();
		try {
			int maxConn=config==null? 1 :
				Integer.parseInt(config.getSubkeyValue(PersistenceProperties.DB_POOL_MAXSIZE,table));
			int timeout=config==null ? Integer.MAX_VALUE:
				Integer.parseInt(config.getSubkeyValue(PersistenceProperties.DB_POOL_TIMEOUT, table));
			setupConnectionPool(getConnectionPoolDataSource(),maxConn,timeout);

			createTables();

		} catch (Exception e) {
			logger.error("Error setting up persistence implementation for table <"+table+">",e);
			throw new RuntimeException(e);
		}
	}

	public void flush(){

	}

	public void shutdown()throws PersistenceException, SQLException {
		flush();
		try {
			_execute(getSQLShutdown());
		}
		catch(Exception e){
					logger.warn("Shutting down: "+e.getMessage());
		}finally{
			shutdownPool();
		}
	}

	public List<String> getIDs()throws PersistenceException, SQLException {
		List<String>result=new ArrayList<>();
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try (Statement s = conn.createStatement()){
					ResultSet rs = s.executeQuery(getSQLSelectAllKeys());
					while(rs.next()){
						result.add(rs.getString(1));
					}
				}
			}
			return result;
		}
	}

	public List<String> getIDs(String column, Object value)throws PersistenceException, SQLException {
		List<String>result=new ArrayList<>();
		try (Connection conn = getConnection()){
			synchronized (conn) {
				try(PreparedStatement ps = conn.prepareStatement(getSQLSelectKeys(column,value))){
					ps.setString(1, String.valueOf(value));
					ResultSet rs=ps.executeQuery();
					while(rs.next()){
						result.add(rs.getString(1));
					}
				}
			}
		}
		return result;
	}

	public List<String> findIDs(boolean orMode, String column, String... values)throws PersistenceException, SQLException {
		List<String>result=new ArrayList<>();
		try(Connection conn = getConnection()){
			synchronized (conn) {
				String sql = getSQLFuzzySelect(column, values.length, orMode);
				try(PreparedStatement ps = conn.prepareStatement(sql)){
					int i=1;
					for(String val: values){
						ps.setString(i, "%"+val+"%");
						i++;
					}
					ResultSet rs=ps.executeQuery();
					while(rs.next()){
						result.add(rs.getString(1));
					}
				}
			}
			return result;
		}
	}

	public List<String> findIDs(String column, String... values)throws PersistenceException, SQLException {
		return findIDs(false, column, values);
	}

	public int getRowCount(String column, Object value) throws SQLException {
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try (Statement s = conn.createStatement()){
					String sql=getSQLRowCount(column,value);
					ResultSet rs=s.executeQuery(sql);
					if(rs.next()){
						return rs.getInt(1);
					}
				}
			}
			return -1;
		}
	}

	public int getRowCount()throws SQLException {
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try (Statement s = conn.createStatement()){
					String sql=getSQLRowCount();
					ResultSet rs=s.executeQuery(sql);
					if(rs.next()){
						return rs.getInt(1);
					}
				}
			}
			return -1;
		}
	}

	public Map<String,String> getColumnValues(String column)throws SQLException {
		Map<String,String>result=new HashMap<>();
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try(Statement s = conn.createStatement()){
					String select=getSQLSelectColumn(column);
					ResultSet rs=s.executeQuery(select);
					while(rs.next()){
						String key=rs.getString(1);
						String value=rs.getString(2);
						result.put(key, value);
					}
				}
			}
			return result;
		}
	}

	/**
	 * read an instance from storage
	 * 
	 * @param id - the ID of the instance to read
	 * @return
	 * @throws PersistenceException
	 */
	protected T _read(String id)throws PersistenceException, SQLException {
		T result=null; 
		try(Connection conn = getConnection()){
			synchronized(conn){
				try(PreparedStatement ps=conn.prepareStatement(getSQLRead())){
					ps.setString(1, id);
					ResultSet rs = ps.executeQuery();
					while(rs.next()){
						result=marshaller.decode(rs.getString(1));
					}
				}
			}
			return result;
		}
	}

	protected void _write(T dao, String id)throws PersistenceException, SQLException {
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try(Statement s=conn.createStatement()){
					PreparedStatement ps = null;
					String exists=getSQLExists(id);
					if(s.executeQuery(exists).next()){
						//update
						ps=conn.prepareStatement(getSQLUpdate());
						parametrizePSUpdate(ps,id, dao);
						ps.executeUpdate();
						ps.clearParameters();
					}
					else{
						//insert
						ps=conn.prepareStatement(getSQLInsert());
						parametrizePSInsert(ps, id, dao);
						ps.executeUpdate();
						ps.clearParameters();
					}
				}
			}
		}
	}

	protected void _remove(String id) throws PersistenceException, SQLException {
		_executeUpdate(getSQLDelete(id));
	}

	protected void _removeAll()throws PersistenceException, SQLException {
		_executeUpdate(getSQLDeleteAll());
	}

	protected void _execute(String sql) throws SQLException {
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try(Statement s = conn.createStatement()){
					s.execute(sql);
				}
			}
		}
	}
	
	protected void _executeUpdate(String sql) throws SQLException {
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try(Statement s = conn.createStatement()){
					s.executeUpdate(sql);
				}
			}
		}
	}
		
	public void purge() {
		try{
			dropTables();
		}catch(Exception pe){
			logger.info("There was an error purging data", pe);
		}
	}

	/**
	 * create a true copy of the supplied object using serialisation.<br/>
	 * @return a true copy of obj
	 */
	protected T copy(T obj) {
		return marshaller.deserialize(marshaller.serialize(obj));
	}

	protected void createTables()throws PersistenceException, SQLException {
		List<String> cmds = getSQLCreateTable();
		try(Connection conn = getConnection()){
			synchronized (conn) {
				try(Statement s = conn.createStatement()){
					for(String sql: cmds){
						try{
							s.execute(sql);
						}catch(SQLException e){
							logger.error(e);
						}
					}
				}
			}
		}
	}


	public void dropTables()throws SQLException {
		try{
			_execute( getSQLDropTable());
		}catch(SQLException e){
			//OK, probably tables did not exist...
		}
	}

	public void parametrizePSInsert(PreparedStatement psInsert, String id, T dao)throws SQLException, PersistenceException{
		psInsert.setString(1, id);
		String base64=marshaller.encode(dao);
		psInsert.setString(2, base64);
		psInsert.setString(3, getTimeStamp());
		int i=4;
		Object val = null;
		for(ColumnDescriptor c: pd.getColumns()){
			try {
				val=c.getMethod().invoke(dao, (Object[])null);
			}catch(Exception ex) {
				throw new PersistenceException(ex);
			}
			psInsert.setString(i, val!=null?val.toString():null);
			i++;
		}
	}

	public void parametrizePSUpdate(PreparedStatement psUpdate, String id, T dao)
			throws PersistenceException, SQLException {
		String base64=marshaller.encode(dao);
		psUpdate.setString(1, base64);
		int i=2;
		Object val=null;
		for(ColumnDescriptor c: pd.getColumns()){
			try {
				val=c.getMethod().invoke(dao, (Object[])null);
			}catch(Exception ex) {
				throw new PersistenceException(ex);
			}
			psUpdate.setString(i, val!=null?val.toString():null);
			i++;
		}
		psUpdate.setString(i, id);
	}

	protected int getDatabaseServerPort() {
		String tb = pd.getTableName();
		Integer port = config.getSubkeyIntValue(PersistenceProperties.DB_PORT, tb);
		if(port==null) {
			port = getDefaultPort();
		}
		return port;
	}

	protected abstract int getDefaultPort();

	@Override
	protected String getDriverName(){
		String driver=config!=null?config.getSubkeyValue(PersistenceProperties.DB_DRIVER, pd.getTableName()):"com.mysql.cj.jdbc.Driver";
		if(driver==null){
			driver = getDefaultDriverName();
		}
		return driver;
	}

	protected abstract String getDefaultDriverName();

	protected abstract ConnectionPoolDataSource getConnectionPoolDataSource();

	/**
	 * get the database to connect to
	 * If set using {@link #setDatabaseName(String)}, that value is used. Otherwise the config is checked. If no
	 * config is set the table name is used as a fallback.
	 * @return
	 */
	protected String getDatabaseName(){
		if(databaseName!=null)return databaseName;
		if(config!=null){
			String db=config.getSubkeyValue(PersistenceProperties.DB_DATABASE, pd.getTableName());
			if(db!=null)return db;
		}
		return pd.getTableName();
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	protected String getUserName(){
		return config.getSubkeyValue(PersistenceProperties.DB_USER, pd.getTableName());
	}

	protected String getPassword(){
		return config.getSubkeyValue(PersistenceProperties.DB_PASSWORD, pd.getTableName());
	}

	Pool pool;

	/**
	 * @param ds - JDBC poolable data source
	 * @param max_connections - how many connections to keep in the pool
	 * @param timeout - time in seconds when to idle out connections
	 */
	protected void setupConnectionPool(ConnectionPoolDataSource ds, int max_connections, int timeout)throws PersistenceException{
		if(ds==null){
			throw new IllegalStateException("Must ConnectionPoolDataSource");
		}
		pool=new Pool(ds,max_connections,timeout);
		logger.info("Connection pooling enabled, maxConnections="+max_connections+" timeout="+timeout);
	}

	protected synchronized Connection getConnection()throws SQLException {
		return pool.getConnection();
	}

	protected void shutdownPool()throws SQLException{
		pool.dispose();
	}
	
	public int getActiveConnections() {
		return pool.getActiveConnections();
	}
	
	@Override
	public String getStatusMessage(){
		return connectionURL+" <"+getActiveConnections()+"> connections.";
	}
	
	protected boolean tableExists() throws PersistenceException, SQLException {
		try(Connection conn = getConnection()){
			synchronized(conn){
				DatabaseMetaData md = conn.getMetaData();
				ResultSet rs = md.getTables(null, null,  pd.getTableName().toUpperCase(), null);
				return rs.next();
			}
		}
	}
	
	protected boolean columnExists(String column) throws PersistenceException, SQLException {
		try(Connection conn = getConnection()){
			synchronized(conn){
				DatabaseMetaData md = conn.getMetaData();
				ResultSet rs = md.getColumns(null, null, pd.getTableName().toUpperCase(), column.toUpperCase());
				return rs.next();
			}
		}
	}
}
