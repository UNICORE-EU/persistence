package eu.unicore.persist.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.JsonParseException;

import eu.unicore.persist.PersistenceProperties;
import eu.unicore.persist.util.Wrapper;


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
		assertEquals(1, p.getIDs().size());

		Dao5 test1 = p.read("1");
		assertNotNull(test1);
		assertEquals("1", test1.getId());
		assertNull(test1.getData());
		
		test=new Dao5();
		test.setId("2");
		test.setData(123);

		p.write(test);
		assertEquals(2, p.getIDs().size());

		test1 = p.read("2");
		assertNotNull(test1);
		assertEquals("2", test1.getId());
		assertEquals(123, test1.getData());

		p.removeAll();
		
		test=new Dao5();
		test.setId("1");
		test.setData(new M1(123));
		p.write(test);

		Wrapper.updates.put(M1.class.getName(), M2.class.getName());
		Dao5 testRenamed = p.read("1");
		assertEquals(123, ((M2)testRenamed.getData()).getData());
		Wrapper.updates.clear();
		
		Wrapper.updates.put(M1.class.getName(), "nosuchclass_"+M2.class.getName());
		assertThrows(JsonParseException.class, ()->p.read("1"));
	}

	public static class M1 implements Serializable{
		public static final long serialVersionUID=1l;
		private int data;
		public M1(int data) {
			this.data = data;
		}
		public int getData() {
			return data;
		}
	}

	public static class M2 implements Serializable {
		public static final long serialVersionUID=1l;
		private int data;
		public M2(int data) {
			this.data = data;
		}
		public int getData() {
			return data;
		}
	}

}
