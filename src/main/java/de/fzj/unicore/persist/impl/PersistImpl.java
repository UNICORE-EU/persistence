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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import de.fzj.unicore.persist.DataVersionException;
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

	private static final Logger logger=Logger.getLogger("unicore.persistence."+PersistImpl.class.getSimpleName());

	protected String databaseName=null;

	public PersistImpl(){
		super();
	}

	public void init()throws PersistenceException{
		super.init();
		String table=pd.getTableName();
		try {

			int maxConn=config==null? 1 :
				Integer.parseInt(config.getSubkeyValue(PersistenceProperties.DB_POOL_MAXSIZE,table));
			int timeout=config==null ? Integer.MAX_VALUE:
				Integer.parseInt(config.getSubkeyValue(PersistenceProperties.DB_POOL_TIMEOUT, table));
			setupConnectionPool(getConnectionPoolDataSource(),maxConn,timeout,getDataSource());

			createTables();

		} catch (Exception e) {
			logger.error("Error setting up persistence implementation for table <"+table+">",e);
			throw new RuntimeException(e);
		}
	}

	public void flush(){

	}

	public void shutdown()throws PersistenceException{
		flush();
		Statement s=null;
		Connection conn=null;
		try{
			conn=getConnection();
			synchronized(conn){
				try{
					String sql=getSQLShutdown();
					if(sql!=null){
						s=conn.createStatement();
						s.execute(sql);
					}
				}catch(Exception e){
					logger.warn("Shutting down: "+e.getMessage());
				}
				finally{
					try{if(s!=null)s.close();}catch(Exception e){}
				}
			}	
		}finally{
			if(conn!=null){
				try{
					disposeConnection(conn);
					if(!conn.isClosed())conn.close();
				}catch(Exception ex){}
			}
			shutdownPool();
		}
	}

	public List<String> getIDs()throws PersistenceException{
		List<String>result=new ArrayList<String>();
		String sql=getSQLSelectAllKeys();

		Connection conn=null;
		try{
			conn=getConnection();
			synchronized (conn) {
				Statement s=null;
				try {
					s=conn.createStatement();
					ResultSet rs=s.executeQuery(sql);
					while(rs.next()){
						result.add(rs.getString(1));
					}
				} catch (Exception e) {
					throw new PersistenceException(e);
				}
				finally{
					if(s!=null)try{	s.close();}catch(Exception e){logger.error("",e);}
				}
			}
			return result;
		}finally{
			disposeConnection(conn);
		}
	}

	public List<String> getIDs(String column, Object value)throws PersistenceException{
		List<String>result=new ArrayList<String>();
		Connection conn=null;
		conn=getConnection();
		synchronized (conn) {
			String sql=getSQLSelectKeys(column,value);
			try(PreparedStatement ps = conn.prepareStatement(sql)){
				ps.setString(1, String.valueOf(value));
				ResultSet rs=ps.executeQuery();
				while(rs.next()){
					result.add(rs.getString(1));
				}
			} catch (Exception e) {
				throw new PersistenceException(e);
			}
			finally{
				disposeConnection(conn);
			}
		}
		return result;
	}

	public List<String> findIDs(boolean orMode, String column, String... values)throws PersistenceException{
		List<String>result=new ArrayList<String>();
		Connection conn=null;
		conn=getConnection();
		synchronized (conn) {
			String sql=getSQLFuzzySelect(column, values.length, orMode);
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
			} catch (Exception e) {
				throw new PersistenceException(e);
			}
			finally{
				disposeConnection(conn);
			}
		}
		return result;
	}

	public List<String> findIDs(String column, String... values)throws PersistenceException{
		return findIDs(false, column, values);
	}

	public int getRowCount(String column, Object value)throws PersistenceException{
		Connection conn=getConnection();
		synchronized (conn) {
			Statement s=null;
			try {
				s=conn.createStatement();
				String sql=getSQLRowCount(column,value);
				ResultSet rs=s.executeQuery(sql);
				if(rs.next()){
					return rs.getInt(1);
				}
			} catch (Exception e) {
				throw new PersistenceException(e);
			}
			finally{
				if(s!=null)try{s.close();}catch(Exception e){logger.error("",e);}
				disposeConnection(conn);
			}
		}
		return -1;
	}

	public int getRowCount()throws PersistenceException{
		Connection conn=getConnection();
		synchronized (conn) {
			Statement s=null;
			try {
				s=conn.createStatement();
				String sql=getSQLRowCount();
				ResultSet rs=s.executeQuery(sql);
				if(rs.next()){
					return rs.getInt(1);
				}
			} catch (Exception e) {
				throw new PersistenceException(e);
			}
			finally{
				if(s!=null)try{s.close();}catch(Exception e){logger.error("",e);}
				disposeConnection(conn);
			}
		}
		return -1;
	}

	public Map<String,String> getColumnValues(String column)throws PersistenceException{
		Map<String,String>result=new HashMap<String,String>();
		Statement s=null;
		Connection conn=getConnection();
		String select=null;
		synchronized (conn) {
			try{
				select=getSQLSelectColumn(column);
				s=conn.createStatement();
				ResultSet rs=s.executeQuery(select);
				while(rs.next()){
					String key=rs.getString(1);
					String value=rs.getString(2);
					result.put(key, value);
				}
			}catch(Exception e){
				logger.error("Error executing: "+select,e);
				throw new PersistenceException(e);
			}
			finally{
				if(s!=null)try{s.close();}catch(Exception e){logger.error("",e);}
				disposeConnection(conn);
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
	protected T _read(String id)throws PersistenceException {
		T result=null; 
		Connection conn=getConnection();
		synchronized(conn){
			try(PreparedStatement ps=conn.prepareStatement(getSQLRead())){
				ps.setString(1, id);
				ResultSet rs = ps.executeQuery();
				while(rs.next()){
					result=marshaller.decode(rs.getString(1));
				}
			}catch(DataVersionException d){
				throw d;
			}catch(Exception e){
				String msg=makeErrorMessage(id, e);
				throw new PersistenceException(msg,e);
			}
			finally{
				disposeConnection(conn);
			}
		}
		return result;
	}

	protected void _write(T dao, String id)throws PersistenceException {
		PreparedStatement ps=null;
		Connection conn=getConnection();
		synchronized (conn) {
			try(Statement s=conn.createStatement()){
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
			}catch(Exception e){
				logger.error(e);
				throw new PersistenceException(e);
			}
			finally{
				if(ps!=null)try{ps.close();}catch(Exception e){logger.error("",e);}
				disposeConnection(conn);
			}
		}
	}

	protected void _remove(String id) throws PersistenceException {
		Connection conn=getConnection();
		synchronized (conn) {
			try(Statement s = conn.createStatement()){
				String delete=getSQLDelete(id);
				s.executeUpdate(delete);
			}catch(Exception e){
				throw new PersistenceException(e);
			}
			finally{
				disposeConnection(conn);
			}
		}
	}

	protected void _removeAll()throws PersistenceException{
		Connection conn=getConnection();
		synchronized (conn) {
			try(Statement s = conn.createStatement()){
				String delete=getSQLDeleteAll();
				s.executeUpdate(delete);
			}catch(Exception e){
				throw new PersistenceException(e);
			}
			finally{
				disposeConnection(conn);
			}
		}
	}

	public void purge(){
		try{
			dropTables();
		}catch(PersistenceException pe){
			logger.info("There was an error purging data",pe);
		}
	}

	/**
	 * create a true copy of the supplied object using serialisation.<br/>
	 * Careful: this will aquire a lock on the object due to a 
	 * strange Java bug, see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6178997
	 * @param obj
	 * @return a true copy of obj
	 */
	protected T copy(T obj)throws IOException, PersistenceException{
		return marshaller.deserialize(marshaller.serialize(obj));
	}

	protected void createTables()throws PersistenceException, SQLException {
		List<String> createCmds = getSQLCreateTable();
		Connection conn=getConnection();
		synchronized (conn) {
			try(Statement s=conn.createStatement()){
				for(String sql: createCmds){
					try{
						s.execute(sql);
					}catch(SQLException e){
						logger.error(e);
					}
				}
			} finally{
				disposeConnection(conn);
			}
		}
	}


	public void dropTables()throws PersistenceException{
		Connection conn=getConnection();
		synchronized (conn) {
			try(Statement s=conn.createStatement()){
				s.execute(getSQLDropTable());
			}catch(Exception e){
				//OK, probably tables did not exist...
			}
			finally{
				disposeConnection(conn);
			}
		}
	}

	public void parametrizePSInsert(PreparedStatement psInsert, String id, T dao)throws IOException,SQLException,InvocationTargetException,IllegalAccessException,PersistenceException{
		psInsert.setString(1, id);
		String base64=marshaller.encode(dao);
		psInsert.setString(2, base64);

		int i=3;
		for(ColumnDescriptor c: pd.getColumns()){
			Object val=c.getMethod().invoke(dao, (Object[])null);
			psInsert.setString(i, val!=null?val.toString():null);
			i++;
		}
	}

	public void parametrizePSUpdate(PreparedStatement psUpdate, String id, T dao)
			throws IOException,SQLException,InvocationTargetException,IllegalAccessException, PersistenceException{
		String base64=marshaller.encode(dao);
		psUpdate.setString(1, base64);

		int i=2;
		for(ColumnDescriptor c: pd.getColumns()){
			Object val=c.getMethod().invoke(dao, (Object[])null);
			psUpdate.setString(i, val!=null?val.toString():null);
			i++;
		}
		psUpdate.setString(i, id);
	}

	protected abstract String createConnString();

	protected abstract ConnectionPoolDataSource getConnectionPoolDataSource();

	/**
	 * if {@link #getConnectionPoolDataSource()} returns null, this must return non-null
	 * @return
	 */
	protected abstract DataSource getDataSource();

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

	private Connection theConnection;

	/**
	 * @param ds - JDBC poolable data source
	 * @param max_connections - how many connections to keep in the pool
	 * @param timeout - time in seconds when to idle out connections
	 */
	protected void setupConnectionPool(ConnectionPoolDataSource ds, int max_connections, int timeout, DataSource dataSource)throws PersistenceException{
		if(ds==null && dataSource==null){
			throw new IllegalStateException("Must have either ConnectionPoolDataSource or normal DataSource!");
		}
		if(ds!=null){
			pool=new Pool(ds,max_connections,timeout);
			logger.info("Connection pooling enabled, maxConnections="+max_connections+" timeout="+timeout);
		}
		else{
			try{
				theConnection=dataSource.getConnection();
			}catch(SQLException e){
				throw new PersistenceException(e);
			}
		}
	}

	protected synchronized Connection getConnection()throws PersistenceException{
		if(theConnection!=null)return theConnection;

		try{
			return pool.getConnection();
		}catch(SQLException se){
			throw new PersistenceException(se);
		}
	}

	protected void disposeConnection(Connection conn) {
		if(theConnection!=null)return;

		try{
			if(conn!=null && !conn.isClosed()){
				conn.close();
			}
		}catch(SQLException se){
			logger.error("Error closing connection.",se);
		}
	}

	protected void shutdownPool()throws PersistenceException{
		if(theConnection!=null)return;

		try{
			pool.dispose();
		}catch(SQLException se){
			throw new PersistenceException(se);
		}
	}
	
	public int getActiveConnections() {
		return pool.getActiveConnections();
	}
	
	@Override
	public String getStatusMessage(){
		return createConnString()+" <"+getActiveConnections()+"> connections.";
	}
	
	protected boolean tableExists() throws PersistenceException, SQLException {
		String tb = pd.getTableName();
		Connection conn=getConnection();
		synchronized(conn){
			try{
				DatabaseMetaData md = conn.getMetaData();
				ResultSet rs = md.getTables(null, null, tb.toUpperCase(), null);
				return rs.next();
			}
			finally{
				disposeConnection(conn);
			}
		}
	}
	
	protected boolean columnExists(String column) throws PersistenceException, SQLException {
		String tb = pd.getTableName();
		Connection conn=getConnection();
		synchronized(conn){
			try{
				DatabaseMetaData md = conn.getMetaData();
				ResultSet rs = md.getColumns(null, null, tb.toUpperCase(), column.toUpperCase());
				return rs.next();
			}
			finally{
				disposeConnection(conn);
			}
		}
	}
}
