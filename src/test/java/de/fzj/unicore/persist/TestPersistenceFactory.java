package de.fzj.unicore.persist;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import de.fzj.unicore.persist.impl.Dao1;
import de.fzj.unicore.persist.impl.H2Persist;
import de.fzj.unicore.persist.impl.InMemory;
import de.fzj.unicore.persist.impl.MySQLPersist;
import eu.unicore.util.configuration.ConfigurationException;

public class TestPersistenceFactory {

	@Test
	public void testGetPersistImplementation()throws Exception{
		PersistenceProperties p=new PersistenceProperties();
		p.setProperty(PersistenceProperties.DB_IMPL+".Dao1", InMemory.class.getName());
		Class<?>clazz=PersistenceFactory.get(p).getPersistClass(Dao1.class, null);
		assert clazz.isAssignableFrom(InMemory.class);
	}
	
	@Test
	public void testConfigureDirectory()throws Exception{
		File tmp=new File("target","data");
		FileUtils.deleteQuietly(tmp);
		PersistenceProperties p=new PersistenceProperties();
		p.setProperty(PersistenceProperties.DB_DIRECTORY, "./"+tmp.getPath());
		p.setProperty(PersistenceProperties.DB_IMPL+".Dao1", H2Persist.class.getName());
		Class<?>clazz=PersistenceFactory.get(p).getPersistClass(Dao1.class, null);
		assert clazz.isAssignableFrom(H2Persist.class);
		Persist<Dao1> persist=PersistenceFactory.get(p).getPersist(Dao1.class);
		persist.shutdown();
		assert tmp.listFiles().length>0;
		FileUtils.deleteQuietly(tmp);
	}
	
	@Test
	public void testConfigurePersistImplementation()throws Exception{
		PersistenceProperties p=new PersistenceProperties();
		p.setProperty(PersistenceProperties.FILE, "src/test/resources/persistence.properties");
		Class<?>clazz=PersistenceFactory.get(p).getPersistClass(Dao1.class, null);
		assert clazz.isAssignableFrom(MySQLPersist.class);
	}
	
	@Test(expected = ConfigurationException.class)
	public void testConfigurePersistImplementationInvalidFile()throws Exception{
		PersistenceProperties p=new PersistenceProperties();
		p.setProperty(PersistenceProperties.FILE, "src/test/resources/persistence.properties.DOESNOTEXIST");
		PersistenceFactory.get(p).getPersistClass(Dao1.class, null);
	}

}
