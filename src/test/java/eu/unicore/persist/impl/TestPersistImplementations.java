package eu.unicore.persist.impl;

import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import eu.unicore.persist.PersistenceProperties;


/**
 * runs some basic functionality tests for the available 
 * (embedded) implementations
 * 
 * @author schuller
 */
public class TestPersistImplementations {

	@ParameterizedTest
	@ValueSource(classes = {H2Persist.class, InMemory.class})
	public void testBasic(Class<?>persistClass) throws Exception {
		PersistenceProperties cf=new PersistenceProperties();
		cf.setDatabaseDirectory("./target/test_data");
		new Tester(persistClass, cf).run();
	}
	
	@Test
	public void testSerializableWrapper()throws Exception, 
	InstantiationException, IllegalAccessException, TimeoutException, InterruptedException{
		H2Persist<Dao5>p = new H2Persist<>(Dao5.class, null);
		PersistenceProperties config=new PersistenceProperties();
		config.setDatabaseDirectory("target/test_data");
		p.setConfigSource(config);
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
