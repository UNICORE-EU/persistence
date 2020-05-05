package de.fzj.unicore.persist.impl;

import java.util.Random;

import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;


/**
 * runs a set of tests with different data sizes and instance numbers
 */
public class TestH2PersistUpdateMany {

	private PersistImpl<Dao1>p;

	private Random rand=new Random();

	@AfterClass
	protected void cleanUp()throws Exception{
		p.dropTables();
	}

	protected void createPersist(){
		p=new H2Persist<Dao1>();
		PersistenceProperties cf=new PersistenceProperties();
		cf.setDatabaseDirectory("./target/test_data");
		p.setConfigSource(cf);
	}
	
	@Test(dataProvider="data-provider")
	public void perfTest(int numberOfUpdates, int size)throws PersistenceException{
		System.out.println("\n\n**** Running test with numUpdates="+numberOfUpdates+", data size="+size);
		createPersist();
		p.setDaoClass(Dao1.class);
		p.setCaching(true);
		long start=System.currentTimeMillis();
		p.init();
		p.removeAll();
		System.out.println("Init took: "+(System.currentTimeMillis()-start)+" ms.");
		System.out.println("Testing n="+numberOfUpdates+" instances, size="+size);
		start=System.currentTimeMillis();
		Dao1 d=new Dao1();
		d.setId("test");
		byte[] buf=new byte[size];
		rand.nextBytes(buf);
		d.setData(new String(buf));
		p.write(d);
		Dao1 d2=new Dao1();
		d2.setId("test1");
		d2.setData(new String(buf));
		p.write(d2);
		for(int i=0;i<numberOfUpdates;i++){
			p.write(d);
			p.write(d2);
		}
		System.out.println("Time for updating: "+(System.currentTimeMillis()-start));
	}

	@DataProvider(name="data-provider")
	protected Object[][]getData(){
		return new Object[][]{
				{1000,50000},
				{5000,10000},
				
		};
	}
	
}
