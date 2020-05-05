package de.fzj.unicore.persist.impl;

import java.util.concurrent.TimeoutException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import de.fzj.unicore.persist.Persist;
import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;


/**
 * runs some basic functionality tests for the available 
 * (embedded) implementations
 * 
 * @author schuller
 */
public class TestPersistImplementations {

	/**
	 * @return implementations of {@link Persist} for testing
	 */
	@DataProvider(name="data-provider")
	public Object[][] getSettings(){
		return new Object[][]{
				{H2Persist.class},
				{InMemory.class},
		};
	}	

	@Test(dataProvider="data-provider")
	public void testBasic(Class<? extends PersistImpl<Dao1>> persistClass) throws Exception {
		PersistenceProperties cf=new PersistenceProperties();
		cf.setDatabaseDirectory("./target/test_data");
		new Tester(persistClass, cf).run();
	}
	
	@Test
	public void testSerializableWrapper()throws PersistenceException, 
	InstantiationException, IllegalAccessException, TimeoutException, InterruptedException{
		H2Persist<Dao5>p=new H2Persist<Dao5>();
		PersistenceProperties config=new PersistenceProperties();
		config.setDatabaseDirectory("target/test_data");
		p.setConfigSource(config);
		p.setDaoClass(Dao5.class);
		p.setCaching(false);
		p.init();
		p.removeAll();

		Dao5 test=new Dao5();
		test.setId("1");
		test.setData(null);

		p.write(test);
		assert p.getIDs().size()==1;

		Dao5 test1 = p.read("1");
		assert test1!=null;
		assert test1.getId().equals("1");
		assert test1.getData()==null;
		
		test=new Dao5();
		test.setId("2");
		test.setData(123);

		p.write(test);
		assert p.getIDs().size()==2;

		test1 = p.read("2");
		assert test1!=null;
		assert test1.getId().equals("2");
		assert test1.getData().equals(Integer.valueOf(123));

		p.shutdown();
	}

}
