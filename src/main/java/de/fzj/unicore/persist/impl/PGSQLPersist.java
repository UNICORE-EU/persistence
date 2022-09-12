/*********************************************************************************
 * Copyright (c) 2022 Forschungszentrum Juelich GmbH 
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.ds.PGConnectionPoolDataSource;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;


/**
 * PostgreSQL implementation
 * 
 * @author schuller
 */
public class PGSQLPersist<T> extends PersistImpl<T>{

	private static final Logger logger = LogManager.getLogger("unicore.persistence.PGSQLPersist");

	@Override
	public List<String> getSQLCreateTable() throws PersistenceException, SQLException {
		List<String> cmds = new ArrayList<>();
		String tb=pd.getTableName();
		String type=getSQLStringType();
		cmds.add("CREATE TABLE IF NOT EXISTS "+tb+" (id VARCHAR(255) PRIMARY KEY, data "
				+type+")");
		boolean haveTable = tableExists();
		if(pd.getColumns().size()>0){
			for(ColumnDescriptor c: pd.getColumns()){
				if(!haveTable || !columnExists(c.getColumn())){
					cmds.add("ALTER TABLE "+pd.getTableName()+" ADD COLUMN IF NOT EXISTS "+c.getColumn()+" "+type);
				}
			}
		}
		if(!haveTable || !columnExists("CREATED")){
			cmds.add("ALTER TABLE "+pd.getTableName()+" ADD COLUMN IF NOT EXISTS created char(32) NOT NULL DEFAULT '"+getTimeStamp()+"'");
		}
		return cmds;
	}
	
	@Override
	protected String getSQLStringType(){
		return "TEXT";
	}

	@Override
	protected String getDefaultDriverName(){
		return "org.postgresql.Driver";
	}

	@Override
	protected int getDefaultPort() {
		return  5432;
	}

	@Override
	protected ConnectionPoolDataSource getConnectionPoolDataSource(){
		PGConnectionPoolDataSource ds = new PGConnectionPoolDataSource();
		ds.setDatabaseName(getDatabaseName());
		String sqlHost=config==null?"localhost":config.getSubkeyValue(PersistenceProperties.DB_HOST, pd.getTableName());
		int port = getDatabaseServerPort();
		ds.setPortNumbers(new int[] { port });
		ds.setServerNames(new String[] { sqlHost });
		ds.setUser(getUserName());
		ds.setPassword(getPassword());
		String sslModeS = config==null?"true":config.getSubkeyValue(PersistenceProperties.PGSQL_SSL, pd.getTableName());
		boolean sslMode = Boolean.parseBoolean(sslModeS);
		try{
			ds.setSsl(sslMode);
			if(sslMode) {
				ds.setSslmode("allow");
			}
		}catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		connectionURL = String.format("jdbc:postgresql://%s:%d/%s?ssl=%s", sqlHost, port, getDatabaseName(),sslMode);
		logger.info("Connecting to: {}", connectionURL);
		return ds;
	}

	@Override
	protected Connection getConnection() throws SQLException{
		Connection c=null;
		try{
			c=super.getConnection();
		}catch(Exception se){
			logger.warn("Error when getting a PGSQL connection: "+se.getMessage()+", trying to reconnect.");
			try{
				pool.cleanupPooledConnections();
			}catch(Exception ex){/*ignored*/}
			c=super.getConnection();
		}
		return c;
	}

	protected boolean columnExists(String column) throws PersistenceException, SQLException {
		String tb = pd.getTableName();
		String sql="SELECT 1 FROM information_schema.columns WHERE "
				+ "table_name='"+tb+"' AND column_name='"+column+"'";
		return runCheck(sql);
	}
	
	protected boolean tableExists() throws PersistenceException, SQLException {
		String tb = pd.getTableName();
		String sql="SELECT 1 FROM pg_tables WHERE "
				+ "schemaname='public' "
				+ "AND tablename='"+tb+"'";
		return runCheck(sql);
	}

	private boolean runCheck(String sql) throws SQLException, PersistenceException {
		try(Connection conn=getConnection()){
			synchronized(conn){
				try(Statement s=conn.createStatement()){
					return s.executeQuery(sql).next();
				}
			}
		}
	}
	
}
