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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;
import eu.unicore.util.Log;


/**
 * MySQL based persistence
 * 
 * @author schuller
 */
public class MySQLPersist<T> extends PersistImpl<T>{

	private static final Logger logger=Logger.getLogger("unicore.persistence."+MySQLPersist.class.getSimpleName());

	private String sqlHost, sqlPort, sqlUser, sqlPass, sqlType;

	@Override
	public List<String> getSQLCreateTable() throws PersistenceException, SQLException {
		List<String> cmds = new ArrayList<>();
		String tb=pd.getTableName();
		if(sqlType==null)sqlType = config.getSubkeyValue(PersistenceProperties.MYSQL_TABLETYPE, tb);
		String type=getSQLStringType();
		cmds.add("CREATE TABLE IF NOT EXISTS "+tb+" (id VARCHAR(255) PRIMARY KEY, data "
				+type+") ENGINE="+sqlType);
		if(pd.getColumns().size()>0){
			boolean haveTable = tableExists();
			for(ColumnDescriptor c: pd.getColumns()){
				if(!haveTable || !columnExists(c.getColumn())){
					cmds.add("ALTER TABLE "+pd.getTableName()+" ADD COLUMN "+c.getColumn()+" "+type);
				}
			}
		}
		return cmds;
	}
	
	@Override
	protected String getSQLStringType(){
		return "LONGTEXT";
	}

	private String connectionURL;
	
	@Override
	protected synchronized String createConnString(){
		if(connectionURL!=null){
			return connectionURL;
		}
		String tb=pd.getTableName();
		if(sqlHost==null)sqlHost = config.getSubkeyValue(PersistenceProperties.DB_HOST, tb);
		if(sqlPort==null)sqlPort = config.getSubkeyValue(PersistenceProperties.DB_PORT, tb);
		connectionURL = "jdbc:mysql://"+sqlHost+":"+sqlPort+"/"+getDatabaseName()+"?autoReconnect=true";
		logger.info("Connecting to: "+connectionURL);
		return connectionURL;
	}

	@Override
	protected String getDriverName(){
		String driver=config!=null?config.getSubkeyValue(PersistenceProperties.DB_DRIVER, pd.getTableName()):"com.mysql.jdbc.Driver";
		if(driver==null){
			driver = "com.mysql.cj.jdbc.Driver";
		}
		return driver;
	}

	@Override
	protected String getUserName(){
		if(sqlUser==null){
			sqlUser = config.getSubkeyValue(PersistenceProperties.DB_USER, pd.getTableName());
		}
		return sqlUser;
	}

	@Override
	protected String getPassword(){
		if(sqlPass==null){
			sqlPass = config.getSubkeyValue(PersistenceProperties.DB_PASSWORD, pd.getTableName());
		}
		return sqlPass;
	}

	@Override
	protected ConnectionPoolDataSource getConnectionPoolDataSource(){
		MysqlConnectionPoolDataSource ds=new MysqlConnectionPoolDataSource();
		ds.setDatabaseName(getDatabaseName());
		sqlHost=config==null?"localhost":config.getSubkeyValue(PersistenceProperties.DB_HOST, pd.getTableName());
		sqlPort=config==null?"3306":config.getSubkeyValue(PersistenceProperties.DB_PORT, pd.getTableName());
		ds.setPort(Integer.parseInt(sqlPort));
		ds.setServerName(sqlHost);
		ds.setUser(getUserName());
		ds.setPassword(getPassword());
		String tz = config==null?"UTC":config.getSubkeyValue(PersistenceProperties.MYSQL_TIMEZONE, pd.getTableName());
		String sslModeS = config==null?"false":config.getSubkeyValue(PersistenceProperties.MYSQL_SSL, pd.getTableName());
		boolean sslMode = Boolean.parseBoolean(sslModeS);
		try{
			ds.setVerifyServerCertificate(false);
			ds.setUseSSL(sslMode);
			ds.setAutoReconnect(true);
			ds.setAutoReconnectForPools(true);
			ds.setServerTimezone(tz);
		}catch(SQLException se){
			logger.warn(Log.createFaultMessage("Error configuring MySQL driver auto-reconnect", se));
		}
		//for info purposes, create and log the connection string
		String conn = "jdbc:mysql://"+sqlHost+":"+sqlPort+"/"+getDatabaseName()+"?ssl="+sslMode+"&serverTimezone="+tz;
		logger.info("Connecting to: "+conn);
		return ds;
	}
	
	protected DataSource getDataSource(){
		return null;
	}
	
	@Override
	protected Connection getConnection()throws PersistenceException{
		Connection c=null;
		try{
			c=super.getConnection();	
			((com.mysql.cj.jdbc.JdbcConnection)c).ping();
		}catch(Exception se){
			logger.warn("Error when getting a MySQL connection: "+se.getMessage()+", trying to reconnect.");
			try{
				pool.cleanupPooledConnections();
			}catch(Exception ex){/*ignored*/}
			c=super.getConnection();
		}
		
		return c;
	}

	protected boolean columnExists(String column) throws PersistenceException, SQLException {
		String tb = pd.getTableName();
		String sql="SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE "
				+ "table_schema='"+getDatabaseName()+"'"
				+ " AND table_name='"+tb+
				"' AND column_name='"+column+"'";
		return runCheck(sql);
	}
	
	protected boolean tableExists() throws PersistenceException, SQLException {
		String tb = pd.getTableName();
		String sql="SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE "
				+ "table_schema='"+getDatabaseName()+"'"
				+ "AND table_name='"+tb+"'";
		return runCheck(sql);
	}
	
	private boolean runCheck(String sql) throws SQLException, PersistenceException {
		Connection conn=getConnection();
		synchronized(conn){
			try(Statement s=conn.createStatement()){
				return s.executeQuery(sql).next();
			}finally{
				disposeConnection(conn);
			}
		}
	}
	
}
