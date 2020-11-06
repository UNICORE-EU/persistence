package de.fzj.unicore.persist.impl;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;


/**
 * runs some basic functionality tests for the available 
 * (embedded) implementations
 * 
 * @author schuller
 */
@RunWith(Parameterized.class)
public class TestPersistImplementations {

	@Parameters
	public static Iterable<? extends Object> implementations(){
		return Arrays.asList(
				H2Persist.class,
				InMemory.class
		);
	}	

	@Parameter
	public Class<? extends PersistImpl<?>> persistClass;
	
	@Test
	public void testBasic() throws Exception {
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
