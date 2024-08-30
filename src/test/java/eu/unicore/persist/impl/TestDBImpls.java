package eu.unicore.persist.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import eu.unicore.persist.PersistenceProperties;

public class TestDBImpls {

	@Test
	public void testMySQL() throws Exception {
		System.out.println(" ** MYSQL");
		PersistenceProperties cf=new PersistenceProperties();
		MySQLPersist<Dao1> p = new MySQLPersist<>(Dao1.class) {
			protected boolean runCheck(String sql) {
				return true;
			}
		};
		p.setConfigSource(cf);
		System.out.println(p.getSQLCreateTable());
		assertEquals("LONGTEXT", p.getSQLStringType());
		System.out.println(p.getSQLDelete("1234"));
		assertEquals(3306, p.getDefaultPort());
		Class.forName(p.getDefaultDriverName());
		assertNotNull(p.getConnectionPoolDataSource());
	}
	
	@Test
	public void testPGSQL() throws Exception {
		System.out.println(" ** PGSQL");
		PersistenceProperties cf=new PersistenceProperties();
		PGSQLPersist<Dao1> p = new PGSQLPersist<>(Dao1.class) {
			protected boolean runCheck(String sql) {
				return true;
			}
		};
		p.setConfigSource(cf);
		System.out.println(p.getSQLCreateTable());
		assertEquals("TEXT", p.getSQLStringType());
		System.out.println(p.getSQLDelete("1234"));
		assertEquals(5432, p.getDefaultPort());
		Class.forName(p.getDefaultDriverName());
		assertNotNull(p.getConnectionPoolDataSource());
	}
}
