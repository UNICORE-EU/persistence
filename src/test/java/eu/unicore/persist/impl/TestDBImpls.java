package eu.unicore.persist.impl;

import org.junit.Test;

import eu.unicore.persist.PersistenceProperties;
import eu.unicore.persist.impl.MySQLPersist;
import eu.unicore.persist.impl.PGSQLPersist;
import eu.unicore.persist.impl.PersistenceDescriptor;

public class TestDBImpls {

	@Test
	public void testMySQL() throws Exception {
		System.out.println(" ** MYSQL");
		PersistenceProperties cf=new PersistenceProperties();
		MySQLPersist<Dao1> p = new MySQLPersist<>() {
			protected boolean runCheck(String sql) {
				return true;
			}
		};
		p.setPersistenceDescriptor(PersistenceDescriptor.get(Dao1.class));
		p.setConfigSource(cf);
		p.setDaoClass(Dao1.class);
		System.out.println(p.getSQLCreateTable());
		assert "LONGTEXT".equals(p.getSQLStringType());
		System.out.println(p.getSQLDelete("1234"));
		assert 3306==p.getDefaultPort();
		Class.forName(p.getDefaultDriverName());
		assert p.getConnectionPoolDataSource()!=null;
	}
	
	@Test
	public void testPGSQL() throws Exception {
		System.out.println(" ** PGSQL");
		PersistenceProperties cf=new PersistenceProperties();
		PGSQLPersist<Dao1> p = new PGSQLPersist<>() {
			protected boolean runCheck(String sql) {
				return true;
			}
		};
		p.setPersistenceDescriptor(PersistenceDescriptor.get(Dao1.class));
		p.setConfigSource(cf);
		p.setDaoClass(Dao1.class);
		System.out.println(p.getSQLCreateTable());
		assert "TEXT".equals(p.getSQLStringType());
		System.out.println(p.getSQLDelete("1234"));
		assert 5432==p.getDefaultPort();
		Class.forName(p.getDefaultDriverName());
		assert p.getConnectionPoolDataSource()!=null;
	}
}
