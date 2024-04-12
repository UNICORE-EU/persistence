package eu.unicore.persist.integration;

import org.junit.Test;

import eu.unicore.persist.PersistenceProperties;
import eu.unicore.persist.impl.PGSQLPersist;
import eu.unicore.persist.impl.Tester;

public class TestPGSQL {

	@Test
	public void testPGSQL() throws Exception {
		System.out.println(" ** POSTGRES INTEGRATION TEST");
		PersistenceProperties cf = new PersistenceProperties();
		cf.setProperty(PersistenceProperties.DB_DATABASE, "postgres");
		cf.setProperty(PersistenceProperties.DB_USER, "postgres");
		cf.setProperty(PersistenceProperties.DB_PASSWORD, "root");
		//cf.setProperty(PersistenceProperties.MYSQL_SSL, "true");
		new Tester(PGSQLPersist.class, cf).run();
	}

}
