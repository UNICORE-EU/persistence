package eu.unicore.persist.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Disabled;

import eu.unicore.persist.Persist;
import eu.unicore.persist.PersistenceProperties;


/**
 * runs some basic functionality tests on the given Persist implementation
 * 
 * @author schuller
 */
@Disabled
public class Tester {

	@SuppressWarnings("rawtypes")
	final Class persistClass;
	final PersistenceProperties properties;
	
	public Tester(Class<?> persistClass){
		this(persistClass, null);
	}
	
	public Tester(Class<?> persistClass, PersistenceProperties properties){
		this.persistClass = persistClass;
		this.properties = properties;
	}
	
	public void run() throws Exception {
		testBasicCRUD(false);
		testBasicCRUD(true);
		testAdditionalColumns(false);
		testAdditionalColumns(true);
		testFindIDs(true);
		testLocking();
		testManualLocking();
	}
	
	@SuppressWarnings("unchecked")
	private void testBasicCRUD(boolean cache)throws Exception {
		Persist<Dao1>p = (Persist<Dao1>)persistClass.getConstructor(Class.class, String.class).
			newInstance(Dao1.class, null);
		p.setCaching(cache);
		if(properties!=null)p.setConfigSource(properties);
		p.init();
		p.removeAll();
		if(p instanceof PersistImpl) {
			((PersistImpl<?>)p).pool.cleanupPooledConnections();
		}
		
		Dao1 test=new Dao1();
		test.setId("1");
		test.setData("testdata");
		p.write(test);
		assertEquals(1, p.getIDs().size());

		Dao1 test1 = p.getForUpdate("1");
		assertNotNull(test1);
		assertEquals("testdata", test1.getData());
		assertEquals("1", test1.getId());

		test1.setData("testdata-changed");
		p.write(test1);
		assertEquals(1, p.getIDs().size());

		test1 = p.read("1");
		assertNotNull(test1);
		assertEquals("testdata-changed", test1.getData());
		assertEquals("1", test1.getId());

		test=new Dao1();
		test.setId("2");
		test.setData("testdata-2");
		p.write(test);
		assertEquals(2, p.getIDs().size());
		test=p.read("2");
		assertNotNull(test);
		assertEquals("testdata-2", test.getData());
		assertEquals("2", test.getId());
		assertEquals(2, p.getRowCount());
		p.remove("1");
		assertEquals(1, p.getIDs().size());
		assertEquals(1, p.getRowCount());
		p.remove("2");
		assertEquals(0, p.getIDs().size());
		assertEquals(0, p.getRowCount());
		test=p.read("1");
		assertNull(test);
		test=p.read("2");
		assertNull(test);
		p.shutdown();
	}
	
	@SuppressWarnings("unchecked")
	private void testAdditionalColumns(Boolean cache)throws Exception{
		System.out.println("Testing "+persistClass.getName());
		Persist<Dao2>p = (Persist<Dao2>)persistClass.getConstructor(Class.class, String.class).
				newInstance(Dao2.class, null);
		
		p.setCaching(cache);
		if(properties!=null)p.setConfigSource(properties);
		p.init();
		p.removeAll();

		Dao2 test=new Dao2();
		test.setId("1");
		test.setData("testdata");
		p.write(test);

		test=p.getForUpdate("1");
		test.setField("new-content");
		p.write(test);

		Dao2 test1=p.read("1");
		assertNotNull(test1);
		assertEquals("new-content", test1.getField());
		assertEquals(1, p.getIDs("foo", "new-content").size());
		assertEquals(1, p.getRowCount("foo", "new-content"));

		Dao2 test2=new Dao2();
		test2.setId("2");
		test2.setField("bar");
		p.write(test2);

		Map<String,String>values=p.getColumnValues("foo");
		assertNotNull(values);
		assertEquals(2, values.size());
		assertEquals("new-content", values.get("1"));
		assertEquals("bar", values.get("2"));

		p.shutdown();
	}

	@SuppressWarnings("unchecked")
	private void testFindIDs(Boolean cache)throws Exception{
		Persist<Dao2>p = (Persist<Dao2>)persistClass.getConstructor(Class.class, String.class).
				newInstance(Dao2.class, null);
		p.setCaching(cache);
		if(properties!=null)p.setConfigSource(properties);
		p.init();
		p.removeAll();

		Dao2 test1=new Dao2();
		test1.setId("1");
		test1.setData("testdata");
		test1.setField("tag1,tag2");
		p.write(test1);

		Dao2 test2=new Dao2();
		test2.setId("2");
		test2.setField("tag2");
		p.write(test2);

		Dao2 test3=new Dao2();
		test3.setId("3");
		test3.setField("tag1");
		p.write(test3);

		List<String> ids = p.findIDs("foo", "tag2");
		assertEquals(2, ids.size());
		
		ids = p.findIDs("foo", "tag2", "tag1");
		assertEquals(1, ids.size());

		p.shutdown();
	}
	
	@SuppressWarnings("unchecked")
	private void testLocking() throws Exception {
		final Persist<Dao1>p = (Persist<Dao1>)persistClass.getConstructor(Class.class, String.class).
				newInstance(Dao1.class, null);
		
		if(properties!=null)p.setConfigSource(properties);
		p.init();
		p.removeAll();

		Dao1 test=new Dao1();
		test.setId("1");
		test.setData("testdata");
		p.write(test);
		assertEquals(1, p.getIDs().size());
		
		test = p.getForUpdate("1");
		final AtomicBoolean OK = new AtomicBoolean(true);
		try{
			Runnable r = new Runnable(){
				public void run(){
					try{
						p.getForUpdate("1", 100, TimeUnit.MILLISECONDS);
						OK.set(false);
					}catch(TimeoutException ex){
					}
					catch(Exception ex){
						OK.set(false);
					}
					if(OK.get())try{
						p.read("1");
					}catch(Exception ex){
						OK.set(false);
					}
				}
			};
			Thread t = new Thread(r);
			t.start();
			t.join();
			assertTrue(OK.get());
		}finally{
			p.unlock(test);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void testManualLocking() throws Exception {
		final Persist<Dao1>p = (Persist<Dao1>)persistClass.getConstructor(Class.class, String.class).
				newInstance(Dao1.class, null);
		if(properties!=null)p.setConfigSource(properties);
		p.init();
		p.removeAll();
		Dao1 test=new Dao1();
		test.setId("1");
		test.setData("testdata");
		p.write(test);
		assertEquals(1, p.getIDs().size());
		test = p.read("1");
		p.lock("1", 100, TimeUnit.MILLISECONDS);
		final AtomicBoolean CAN_READ = new AtomicBoolean(false);
		Runnable r = () -> {
			try{
				Dao1 test2 = p.tryGetForUpdate("1");
				CAN_READ.set(test2!=null);
				if(test2!=null) {
					p.unlock(test2);
				}
			}catch(Exception ex) {}
		};
		Thread t = new Thread(r);
		t.start();
		t.join();
		assertFalse(CAN_READ.get());

		p.unlock(test);
		t = new Thread(r);
		t.start();
		t.join();
		assertTrue(CAN_READ.get());
	}

}
