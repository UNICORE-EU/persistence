package eu.unicore.persist.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.ConnectionPoolDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.ds.PGConnectionPoolDataSource;

import eu.unicore.persist.PersistenceException;
import eu.unicore.persist.PersistenceProperties;


/**
 * PostgreSQL implementation
 * 
 * @author schuller
 */
public class PGSQLPersist<T> extends PersistImpl<T>{

	private static final Logger logger = LogManager.getLogger("unicore.persistence.PGSQLPersist");

	public PGSQLPersist(Class<T> daoClass, String tableName) {
		super(daoClass, tableName);
	}

	@Override
	protected List<String> getSQLCreateTable() throws PersistenceException, SQLException {
		List<String> cmds = new ArrayList<>();
		String tb=pd.getTableName();
		String type=getSQLStringType();
		cmds.add(String.format("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(255) PRIMARY KEY, data %s)", tb, type));
		boolean haveTable = tableExists();
		if(pd.getColumns().size()>0){
			for(ColumnDescriptor c: pd.getColumns()){
				if(!haveTable || !columnExists(c.getColumn())){
					cmds.add(String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
							pd.getTableName(), c.getColumn(), type));
				}
			}
		}
		if(!haveTable || !columnExists("CREATED")){
			cmds.add(String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS created char(32) NOT NULL DEFAULT '%s'",
					pd.getTableName(), getTimeStamp()));
		}
		return cmds;
	}

	@Override
	protected String getSQLStringType(){
		return "TEXT";
	}

	@Override
	protected String getDefaultDriverName(){
		return org.postgresql.Driver.class.getName();
	}

	@Override
	protected int getDefaultPort() {
		return 5432;
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
		ds.setSsl(sslMode);
		if(sslMode) {
			ds.setSslmode("allow");
		}
		connectionURL = String.format("jdbc:postgresql://%s:%d/%s?ssl=%s", sqlHost, port, getDatabaseName(), sslMode);
		return ds;
	}

	@Override
	protected Connection getConnection() throws SQLException{
		Connection c=null;
		try{
			c=super.getConnection();
		}catch(Exception se){
			logger.warn("Error when getting a PGSQL connection: {}, trying to reconnect.",se.getMessage());
			try{
				pool.cleanupPooledConnections();
			}catch(Exception ex){/*ignored*/}
			c=super.getConnection();
		}
		return c;
	}

	@Override
	protected boolean columnExists(String column) throws PersistenceException, SQLException {
		return runCheck(String.format("SELECT 1 FROM pg_tables WHERE "
				+ "schemaname='public' AND tablename='%s' AND column_name='%s'",
				pd.getTableName(), column));
	}

	@Override
	protected boolean tableExists() throws PersistenceException, SQLException {
		return runCheck(String.format("SELECT 1 FROM pg_tables WHERE "
				+ "schemaname='public' AND tablename='%s'", pd.getTableName()));
	}

	protected boolean runCheck(String sql) throws SQLException, PersistenceException {
		try(Connection conn=getConnection()){
			synchronized(conn){
				try(Statement s=conn.createStatement()){
					return s.executeQuery(sql).next();
				}
			}
		}
	}

}
