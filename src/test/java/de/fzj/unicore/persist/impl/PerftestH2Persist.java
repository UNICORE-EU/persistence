package de.fzj.unicore.persist.impl;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;


/**
 * runs a set of tests with different data sizes and instance numbers
 */
@RunWith(Parameterized.class)
public class PerftestH2Persist {

	private static PersistImpl<Dao1>p;

	private Random rand=new Random();

	@Parameters
	public static Collection<Object[]>getDataForJSONTest(){
		return Arrays.asList(new Object[][]{
				// numberOfInstances, size, cache
				{10000,5000,Boolean.FALSE},
				{10000,5000,Boolean.FALSE}
		});
	}
	
	protected void createPersist(){
		FileUtils.deleteQuietly(new File("target/test_data"));
		p=new H2Persist<Dao1>();
		PersistenceProperties props = new PersistenceProperties();
		props.setDatabaseDirectory("target/test_data");
		p.setConfigSource(props);
	}
	
	@Parameter
	public int numberOfInstances; 
	@Parameter(1)
	public int size;
	@Parameter(2)
	public Boolean cache;
	
	@Test
	public void perfTestJSONvsBinary()throws PersistenceException{
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
		p.shutdown();
	}

	protected void readSomeRandomEntries(int numberOfInstances)throws PersistenceException{
		for(int i=0;i<50;i++){
			Dao1 e=p.read(String.valueOf(rand.nextInt(numberOfInstances)));
			assert e!=null;
		}
	}


}
