package eu.unicore.persist.integration;

import org.junit.Test;

import eu.unicore.persist.PersistenceProperties;
import eu.unicore.persist.impl.MySQLPersist;
import eu.unicore.persist.impl.Tester;

public class TestMySQL {

	@Test
	public void testMySQL() throws Exception {
		System.out.println(" ** MYSQL INTEGRATION TEST");
		PersistenceProperties cf = new PersistenceProperties();
		cf.setProperty(PersistenceProperties.DB_DATABASE, "test");
		cf.setProperty(PersistenceProperties.DB_USER, "root");
		cf.setProperty(PersistenceProperties.DB_PASSWORD, "root");
		cf.setProperty(PersistenceProperties.MYSQL_SSL, "true");
		new Tester(MySQLPersist.class, cf).run();
	}

}
