package de.fzj.unicore.persist.impl;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import de.fzj.unicore.persist.Persist;
import de.fzj.unicore.persist.PersistenceProperties;


public class TestDataIntegrity {
	
	Persist<Dao3>p;
	long total;
	
	String[] ids={"1","2","3"};
	
	@BeforeMethod
	public void init()throws Exception{
		total=0;
		p=new H2Persist<Dao3>();
		PersistenceProperties props = new PersistenceProperties();
		props.setDatabaseDirectory("target/test_data");
		p.setConfigSource(props);
		p.setDaoClass(Dao3.class);
		p.setCaching(true);
		p.init();
		for(String id: ids){
			Dao3 test=new Dao3();
			test.setId(id);
			test.setData(0);
			p.write(test);
			p.flush();
		}
	}
	
	@Test
	public void testIsolation1()throws Exception{
		Dao3 orig=null;
		Dao3 test=null;
		for(String id: ids){
			orig=p.getForUpdate(id);
			int original=orig.getData();
			orig.setData(original+1);
			
			//check that we get a new copy, i.e. the original value of the data field
			test=p.read(id);
			assert(test.getData().intValue()==0);
			p.write(orig);
			//check that a read now returns the new value
			test=p.read(id);
			assert(test.getData().intValue()==1);
			
		}	
	}
	
	@Test
	public void testIsolation2()throws Exception{
		Dao3 orig=null;
		Dao3 test=null;
		for(String id: ids){
			orig=p.getForUpdate(id);
			int original=orig.getData();
			orig.setData(original+1);
			//assume that we abort the transaction
			p.unlock(orig);

			//check that a read still returns the old value
			test=p.read(id);
			assert(test!=null);
			assert(test.getData().intValue()==0);
		}	
	}
	
}
