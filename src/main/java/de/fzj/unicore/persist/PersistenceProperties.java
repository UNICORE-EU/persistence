package de.fzj.unicore.persist;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.fzj.unicore.persist.impl.H2Persist;
import eu.unicore.util.configuration.DocumentationReferenceMeta;
import eu.unicore.util.configuration.DocumentationReferencePrefix;
import eu.unicore.util.configuration.PropertiesHelper;
import eu.unicore.util.configuration.PropertyMD;

/**
 * properties for configuring the persistence library 
 * 
 * @author schuller
 */
public class PersistenceProperties extends PropertiesHelper{
	
	private static final Logger log = LogManager.getLogger("unicore.configuration.PersistenceProperties"); 
	
	@DocumentationReferencePrefix
	public static final String PREFIX="persistence.";
	
	/**
	 * configuration file to read
	 */
	public static final String FILE="config";
	
	/**
	 * the concrete persistence class to use
	 */
	public static final String DB_IMPL="class";
	
	/**
	 * the JDBC driver name (if not using the default for a given DB)
	 */
	public static final String DB_DRIVER="driver";
	
	/**
	 * the name of the database to connect to
	 */
	public static final String DB_DATABASE="database";
	
	/**
	 * the database user name
	 */
	public static final String DB_USER="user";
	
	/**
	 * the database password
	 */
	public static final String DB_PASSWORD="password";
	
	/**
	 * the host of the DB server
	 */
	public static final String DB_HOST="host";
	
	/**
	 * the port where the DB server is listening
	 */
	public static final String DB_PORT="port";
	
	/**
	 * maximum pooled connections (default: 1)
	 */
	public static final String DB_POOL_MAXSIZE="max_connections";
	
	/**
	 * maximum pooled connections (default: Integer.MAX_VALUE)
	 */
	public static final String DB_POOL_TIMEOUT="pool_timeout";
	
	/**
	 * enable caching
	 */
	public static final String DB_CACHE_ENABLE="cache.enable";
	
	/**
	 * maximum number of cache entries
	 */
	public static final String DB_CACHE_MAX_SIZE="cache.maxSize";
	
	/**
	 * enable clustering (use distributed locks)
	 */
	public static final String DB_LOCKS_DISTRIBUTED="cluster.enable";
	
	/**
	 * config file for the Hazelcast cluster
	 */
	public static final String DB_CLUSTER_CONFIG="cluster.config";
	
	/**
	 * the directory (if appropriate) where to store the data
	 */
	public static final String DB_DIRECTORY="directory";
	
	/**
	 * memory for caching in kilo bytes <br/> 
	 * default: 4096 kB 
	 */
	public static String H2_CACHESIZE="h2.cache_size";

	/**
	 * use the H2 server mode? <br/> 
	 * default: false
	 */
	public static String H2_SERVER_MODE="h2.server_mode";

	/**
	 * further options for H2 (separated by ";")
	 * default: null
	 */
	public static String H2_OPTIONS="h2.options";


	/**
	 * MySQL table type (default "MyISAM")
	 */
	public static final String MYSQL_TABLETYPE="mysql.tabletype";

	public static final String MYSQL_TIMEZONE = "mysql.timezone";
	
	public static final String MYSQL_SSL = "mysql.useSSL";

	/**
	 * Postgres
	 */
	public static final String PGSQL_SSL = "pgsql.useSSL";

	
	@DocumentationReferenceMeta
	public final static Map<String, PropertyMD> META = new HashMap<String, PropertyMD>();
	static
	{
		META.put(FILE, new PropertyMD().
				setPath().
				setDescription("Allows to specify a separate properties file containing the persistence configuration."));
		META.put(DB_IMPL, new PropertyMD(H2Persist.class.getName()).setCanHaveSubkeys().
				setDescription("The persistence implementation class, which controls with DB backend is used."));
		META.put(DB_DRIVER, new PropertyMD().setCanHaveSubkeys().
				setDescription("The database driver. If not set, the default one for the chosen DB backend is used."));
		META.put(DB_DATABASE, new PropertyMD().setCanHaveSubkeys().
				setDescription("The name of the database to connect to (e.g. when using MySQL)."));
		META.put(DB_USER, new PropertyMD("sa").setCanHaveSubkeys().
				setDescription("The database username."));
		META.put(DB_PASSWORD, new PropertyMD("").setCanHaveSubkeys().
				setDescription("The database password."));
		META.put(DB_HOST, new PropertyMD("localhost").setCanHaveSubkeys().
				setDescription("The database host."));
		META.put(DB_PORT, new PropertyMD().setInt().setCanHaveSubkeys().
				setDescription("The database port. If not set, the default port for the chosen DB backend is used."));
		META.put(DB_DIRECTORY, new PropertyMD().setCanHaveSubkeys().
				setDescription("The directory for storing data (embedded DBs)."));
		META.put(DB_LOCKS_DISTRIBUTED, new PropertyMD("false").setCanHaveSubkeys().setDeprecated().
				setDescription("(deprecated, no effect)"));
		META.put(DB_CLUSTER_CONFIG, new PropertyMD().setCanHaveSubkeys().
				setDescription("Clustering configuration file."));
		META.put(DB_POOL_MAXSIZE, new PropertyMD("1").setCanHaveSubkeys().
				setDescription("Connection pool maximum size."));
		META.put(DB_POOL_TIMEOUT, new PropertyMD("3600").setCanHaveSubkeys().
				setDescription("Connection pool timeout when trying to get a connection."));
		META.put(DB_CACHE_ENABLE, new PropertyMD("true").setCanHaveSubkeys().
				setDescription("Enable caching."));
		META.put(DB_CACHE_MAX_SIZE, new PropertyMD("10").setCanHaveSubkeys().
				setDescription("Maximum number of elements in the cache (default: 10)."));
		META.put(H2_CACHESIZE, new PropertyMD("1024").setCanHaveSubkeys().
				setDescription("(H2) Cache size."));
		META.put(H2_OPTIONS, new PropertyMD().setCanHaveSubkeys().
				setDescription("(H2) Further options separated by ';'."));
		META.put(H2_SERVER_MODE, new PropertyMD("false").setCanHaveSubkeys().
				setDescription("(H2) Connect to a H2 server."));		
		META.put(MYSQL_TABLETYPE, new PropertyMD("MyISAM").setCanHaveSubkeys().
						setDescription("(MySQL) Table type (engine) to use."));
		META.put(MYSQL_TIMEZONE, new PropertyMD("UTC").setCanHaveSubkeys().
				setDescription("(MySQL) Server timezone."));
		META.put(MYSQL_SSL, new PropertyMD("false").setCanHaveSubkeys().
				setDescription("(MySQL) Connect using SSL."));
		META.put(PGSQL_SSL, new PropertyMD("true").setCanHaveSubkeys().
				setDescription("(PostgreSQL) Connect using SSL."));
	}
	
	
	/**
	 * create empty persistence properties
	 */
	public PersistenceProperties(){
		this(new Properties());
	}
	
	/**
	 * create persistence properties from the given properties
	 */
	public PersistenceProperties(Properties properties){
		super(PREFIX, properties, META, log);
	}
	
	public void setDatabaseDirectory(String dir){
		if(!(new File(dir).isAbsolute())){
			dir="./"+dir;
		}
		setProperty(DB_DIRECTORY, dir);
	}

	public Properties getRawProperties(){
		return properties;
	}
}
