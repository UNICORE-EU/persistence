package eu.unicore.persist.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Ignore;

import eu.unicore.persist.Persist;
import eu.unicore.persist.PersistenceProperties;
import eu.unicore.persist.impl.PersistImpl;


/**
 * runs some basic functionality tests on the given Persist implementation
 * 
 * @author schuller
 */
@Ignore
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
		Persist<Dao1>p=(Persist<Dao1>)persistClass.getConstructor().newInstance();
		p.setDaoClass(Dao1.class);
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
		assert p.getIDs().size()==1;

		Dao1 test1 = p.getForUpdate("1");
		assert test1!=null;
		assert test1.getData().equals("testdata");
		assert test1.getId().equals("1");

		test1.setData("testdata-changed");
		p.write(test1);
		assert p.getIDs().size()==1;

		test1 = p.read("1");
		assert test1!=null;
		assert test1.getData().equals("testdata-changed");
		assert test1.getId().equals("1");

		test=new Dao1();
		test.setId("2");
		test.setData("testdata-2");
		p.write(test);
		assert p.getIDs().size()==2;
		test=p.read("2");
		assert test!=null;
		assert test.getData().equals("testdata-2");
		assert test.getId().equals("2");
		assert p.getRowCount()==2;
		p.remove("1");
		assert p.getIDs().size()==1;
		assert p.getRowCount()==1;
		p.remove("2");
		assert p.getIDs().size()==0;
		assert p.getRowCount()==0;
		test=p.read("1");
		assert test==null;
		test=p.read("2");
		assert test==null;
		p.shutdown();
	}
	
	@SuppressWarnings("unchecked")
	private void testAdditionalColumns(Boolean cache)throws Exception{
		System.out.println("Testing "+persistClass.getName());
		Persist<Dao2>p=(Persist<Dao2>)persistClass.getConstructor().newInstance();
		p.setDaoClass(Dao2.class);
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
		assert test1!=null;
		assert test1.getField().equals("new-content");
		assert p.getIDs("foo", "new-content").size()==1;
		assert p.getRowCount("foo", "new-content")==1;


		Dao2 test2=new Dao2();
		test2.setId("2");
		test2.setField("bar");
		p.write(test2);

		Map<String,String>values=p.getColumnValues("foo");
		assert values!=null;
		assert 2==values.size();
		assert "new-content".equals(values.get("1"));
		assert "bar".equals(values.get("2"));

		p.shutdown();
	}

	@SuppressWarnings("unchecked")
	private void testFindIDs(Boolean cache)throws Exception{
		Persist<Dao2>p=(Persist<Dao2>)persistClass.getConstructor().newInstance();
		p.setDaoClass(Dao2.class);
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
		assert ids.size() == 2;
		
		ids = p.findIDs("foo", "tag2", "tag1");
		assert ids.size() == 1;
		
		
		p.shutdown();
	}
	
	@SuppressWarnings("unchecked")
	private void testLocking() throws Exception {
		final Persist<Dao1>p=(Persist<Dao1>)persistClass.getConstructor().newInstance();
		p.setDaoClass(Dao1.class);
		if(properties!=null)p.setConfigSource(properties);
		p.init();
		p.removeAll();

		Dao1 test=new Dao1();
		test.setId("1");
		test.setData("testdata");
		p.write(test);
		assert p.getIDs().size()==1;
		
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
			assert OK.get()==true;
		}finally{
			p.unlock(test);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void testManualLocking() throws Exception {
		final Persist<Dao1>p=(Persist<Dao1>)persistClass.getConstructor().newInstance();
		p.setDaoClass(Dao1.class);
		if(properties!=null)p.setConfigSource(properties);
		p.init();
		p.removeAll();

		Dao1 test=new Dao1();
		test.setId("1");
		test.setData("testdata");
		p.write(test);
		assert p.getIDs().size()==1;
		
		test = p.read("1");
		p.lock("1", 100, TimeUnit.MILLISECONDS);
		final AtomicBoolean CAN_READ = new AtomicBoolean(false);
		Runnable r = new Runnable(){
			public void run() {
				try{
					Dao1 test2 = p.tryGetForUpdate("1");
					CAN_READ.set(test2!=null);
					if(test2!=null) {
						p.unlock(test2);
					}
				}catch(Exception ex) {}
			}
		};
		Thread t = new Thread(r);
		t.start();
		t.join();
		assert !CAN_READ.get();
		
		p.unlock(test);
		t = new Thread(r);
		t.start();
		t.join();
		assert CAN_READ.get();
	}

}
