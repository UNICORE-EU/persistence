package eu.unicore.persist.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.ConnectionPoolDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

import eu.unicore.persist.PersistenceException;
import eu.unicore.persist.PersistenceProperties;


/**
 * MySQL implementation
 *
 * @author schuller
 */
public class MySQLPersist<T> extends PersistImpl<T>{

	private static final Logger logger = LogManager.getLogger("unicore.persistence.MySQLPersist");

	public MySQLPersist(Class<T> daoClass, String tableName) {
		super(daoClass, tableName);
	}

	@Override
	protected List<String> getSQLCreateTable() throws PersistenceException, SQLException {
		List<String> cmds = new ArrayList<>();
		String tb = pd.getTableName();
		String engineType = config.getSubkeyValue(PersistenceProperties.MYSQL_TABLETYPE, tb);
		String stringType = getSQLStringType();
		cmds.add(String.format("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(240) PRIMARY KEY, data %s) ENGINE=%s",
				tb, stringType, engineType)
		);
		boolean haveTable = tableExists();
		if(pd.getColumns().size()>0){
			for(ColumnDescriptor c: pd.getColumns()){
				if(!haveTable || !columnExists(c.getColumn())){
					cmds.add(String.format("ALTER TABLE %s ADD COLUMN %s %s",
							tb, c.getColumn(), stringType));
				}
			}
		}
		if(!haveTable || !columnExists("CREATED")){
			cmds.add(String.format("ALTER TABLE %s ADD COLUMN created CHAR(32) NOT NULL DEFAULT '%s'",
					tb, getTimeStamp()));
		}
		return cmds;
	}

	@Override
	protected String getSQLStringType(){
		return "LONGTEXT";
	}

	@Override
	protected int getDefaultPort() {
		return 3306;
	}

	@Override
	protected String getDefaultDriverName(){
		return com.mysql.cj.jdbc.Driver.class.getName();
	}

	@Override
	protected ConnectionPoolDataSource getConnectionPoolDataSource() throws SQLException {
		MysqlConnectionPoolDataSource ds=new MysqlConnectionPoolDataSource();
		ds.setDatabaseName(getDatabaseName());
		String sqlHost=config==null?"localhost":config.getSubkeyValue(PersistenceProperties.DB_HOST, pd.getTableName());
		ds.setPort(getDatabaseServerPort());
		ds.setServerName(sqlHost);
		ds.setUser(getUserName());
		ds.setPassword(getPassword());
		String tz = config==null?"UTC":config.getSubkeyValue(PersistenceProperties.MYSQL_TIMEZONE, pd.getTableName());
		String sslModeS = config==null?"false":config.getSubkeyValue(PersistenceProperties.MYSQL_SSL, pd.getTableName());
		boolean sslMode = Boolean.parseBoolean(sslModeS);
		ds.setVerifyServerCertificate(false);
		ds.setUseSSL(sslMode);
		ds.setAutoReconnect(true);
		ds.setAutoReconnectForPools(true);
		ds.setServerTimezone(tz);
		connectionURL = String.format("jdbc:mysql://%s:%s/%s?ssl=%s&serverTimezone=%s",
				sqlHost, getDatabaseServerPort(), getDatabaseName(), sslMode, tz);
		return ds;
	}

	@Override
	protected Connection getConnection() throws SQLException {
		Connection c=null;
		try{
			c=super.getConnection();	
			((com.mysql.cj.jdbc.JdbcConnection)c).ping();
		}catch(Exception se){
			logger.warn("Error when getting a MySQL connection: {}, trying to reconnect.", se.getMessage());
			try{
				pool.cleanupPooledConnections();
			}catch(Exception ex){/*ignored*/}
			c=super.getConnection();
		}
		return c;
	}

	@Override
	protected boolean columnExists(String column) throws PersistenceException, SQLException {
		return runCheck( String.format("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE "
				+ "table_schema='%s' "
				+ "AND table_name='%s' "
				+ "AND column_name='%s'",
				getDatabaseName(), pd.getTableName(), column));
	}

	@Override
	protected boolean tableExists() throws PersistenceException, SQLException {
		return runCheck( String.format("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE "
				+ "table_schema='%s' "
				+ "AND table_name='%s'",
				getDatabaseName(), pd.getTableName()));
	}

	protected boolean runCheck(String sql) throws SQLException{
		try(Connection conn = getConnection()){
			synchronized(conn){
				try(Statement s=conn.createStatement()){
					return s.executeQuery(sql).next();
				}
			}
		}
	}

}
