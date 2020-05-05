package de.fzj.unicore.persist.impl;

import java.util.Collection;
import java.util.Random;

import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;


/**
 * runs a set of tests with different data sizes and instance numbers
 */
public class PerftestH2Persist {

	private PersistImpl<Dao1>p;

	private Random rand=new Random();

	@AfterClass
	protected void cleanUp()throws Exception{
		p.dropTables();
	}

	protected void createPersist(){
		p=new H2Persist<Dao1>();
		PersistenceProperties props = new PersistenceProperties();
		props.setDatabaseDirectory("target/test_data");
		p.setConfigSource(props);
	}
	
	@Test(dataProvider="data-provider-json")
	public void perfTestJSONvsBinary(int numberOfInstances, int size, Boolean cache)throws PersistenceException{
		doTest(numberOfInstances, size, cache);
	}
	
	private void doTest(int numberOfInstances, int size, Boolean cache)throws PersistenceException{
		System.out.println("\n\n**** Running test with numInstances="+numberOfInstances
				+" data_size="+size
				+" cache="+cache);
		createPersist();
		p.setDaoClass(Dao1.class);
		p.setCaching(cache);
		long start=System.currentTimeMillis();
		p.init();
		p.removeAll();
		System.out.println("Init took: "+(System.currentTimeMillis()-start)+" ms.");
		System.out.println("Testing n="+numberOfInstances+" instances, size="+size);
		start=System.currentTimeMillis();
		Boolean other=Boolean.TRUE; 
		for(int i=0;i<numberOfInstances;i++){
			byte[] b=new byte[size];
			rand.nextBytes(b);
			String s=new String(b);
			Dao1 d=new Dao1();
			d.setData(s);
			d.setOther(other.toString());
			other=!other;
			d.setId(""+i);
			p.write(d);
		}
		System.out.println("Time for writing: "+(System.currentTimeMillis()-start));
		
		System.out.println("Entries in DB:    "+p.getIDs().size());
		start=System.currentTimeMillis();
		readSomeRandomEntries(numberOfInstances);
		System.out.println("Time for reading 50 random entries: "+(System.currentTimeMillis()-start));
		//select subset woth other="true"
		start=System.currentTimeMillis();
		Collection<String> ids=p.getIDs("other", Boolean.TRUE);
		assert ids!=null;
		assert ids.size()>0;
		System.out.println("Time for selecting subset: "+(System.currentTimeMillis()-start));
		System.out.println("Read cache hits: "+p.getCacheHits());
	}

	protected void readSomeRandomEntries(int numberOfInstances)throws PersistenceException{
		for(int i=0;i<50;i++){
			Dao1 e=p.read(String.valueOf(rand.nextInt(numberOfInstances)));
			assert e!=null;
		}
	}

	@DataProvider(name="data-provider-json")
	protected Object[][]getDataForJSONTest(){
		return new Object[][]{
				// numberOfInstances, size, cache
				{10000,5000,Boolean.FALSE},
				{10000,5000,Boolean.FALSE},
		};
	}
}
