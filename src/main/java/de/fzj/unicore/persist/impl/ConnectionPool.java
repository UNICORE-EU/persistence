package de.fzj.unicore.persist.impl;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

import org.apache.logging.log4j.Logger;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.util.Pool;
import eu.unicore.util.Log;

/**
 * contains the logic for managing a connection pool. Based on work by 
 * Christian d'Heureuse 
 * @see Pool
 * @author schuller
 */
public class ConnectionPool {

	private static final Logger logger  = Log.getLogger("unicore.persistence", ConnectionPool.class);
	
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
}
