package de.fzj.unicore.persist.externaldb;

import java.util.Collection;
import java.util.Random;

import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;
import de.fzj.unicore.persist.impl.Dao1;
import de.fzj.unicore.persist.impl.MySQLPersist;


/**
 * runs a set of tests with different data sizes and instance numbers
 */
public class PerftestMySQLPersist {

	private MySQLPersist<Dao1>p;

	private Random rand=new Random();

	@AfterClass
	protected void cleanUp()throws Exception{
		p.dropTables();
	}

	@Test(dataProvider="data-provider")
	public void perfTest(int numberOfInstances, int size, Boolean cache)throws PersistenceException{
		System.out.println("Running test with numInstances="+numberOfInstances+", data size="+size+", cache="+cache);
		p=new MySQLPersist<Dao1>();
		PersistenceProperties pcs=new PersistenceProperties();
		p.setConfigSource(pcs);
		
		pcs.setProperty(PersistenceProperties.DB_DATABASE, "unicore_test");
		pcs.setProperty(PersistenceProperties.DB_HOST, "zam025c17");
		pcs.setProperty(PersistenceProperties.DB_PORT, "3306");
		pcs.setProperty(PersistenceProperties.DB_USER, "unicore");
		pcs.setProperty(PersistenceProperties.DB_PASSWORD, "uN1C0R3!");
		pcs.setProperty(PersistenceProperties.DB_POOL_MAXSIZE, "5");
		
		p.setDaoClass(Dao1.class);
		p.setCaching(cache);
		long start=System.currentTimeMillis();
		p.init();
		try{
			p.removeAll();
		}catch(PersistenceException pe){
			System.out.println("removing: "+pe.getMessage());
		}
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
		System.out.println("Time for reading: "+(System.currentTimeMillis()-start));
		//select subset with other="true"
		start=System.currentTimeMillis();
		Collection<String> ids=p.getIDs("other", Boolean.TRUE);
		assert ids!=null;
		assert ids.size()>0;
		System.out.println("Time for selecting subset: "+(System.currentTimeMillis()-start));	
	}

	protected void readSomeRandomEntries(int numberOfInstances)throws PersistenceException{
		for(int i=0;i<50;i++){
			Dao1 e=p.read(""+rand.nextInt(numberOfInstances));
			assert e!=null;
		}
	}

	@DataProvider(name="data-provider")
	protected Object[][]getData(){
		return new Object[][]{
//				{99,100,Boolean.TRUE},
//				{99,100,Boolean.FALSE},
//				{300,1000,Boolean.FALSE},
//				{300,1000,Boolean.TRUE},
//				{300,100000,Boolean.TRUE},
//				{300,100000,Boolean.FALSE},
//				{99, 100000,Boolean.TRUE},
				{99, 100000,Boolean.FALSE},
//				{10, 400000,Boolean.FALSE},
		};
	}
}
