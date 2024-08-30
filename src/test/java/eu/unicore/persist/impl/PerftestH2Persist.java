package eu.unicore.persist.impl;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.MethodSource;

import eu.unicore.persist.PersistenceProperties;


/**
 * runs a set of tests with different data sizes and instance numbers
 */
public class PerftestH2Persist {

	private static PersistImpl<Dao1>p;

	private Random rand=new Random();

	public static Collection<Object[]>getData(){
		return Arrays.asList(new Object[][]{
				// numberOfInstances, size, cache
				{10000,5000,Boolean.FALSE},
				{10000,5000,Boolean.FALSE}
		});
	}

	protected void createPersist() throws SQLException {
		FileUtils.deleteQuietly(new File("target/test_data"));
		p = new H2Persist<>(Dao1.class);
		PersistenceProperties props = new PersistenceProperties();
		props.setDatabaseDirectory("target/test_data");
		p.setConfigSource(props);
	}

	@ParameterizedTest
	@MethodSource("getData")
	public void perfTestJSONvsBinary(ArgumentsAccessor args)throws Exception{
		int numberOfInstances = args.getInteger(0);
		int size = args.getInteger(1);
		Boolean cache = args.getBoolean(2);
		System.out.println("\n\n**** Running test with numInstances="+numberOfInstances
				+" data_size="+size
				+" cache="+cache);
		createPersist();
		p.setCaching(cache);
		long start=System.currentTimeMillis();
		p.init();
		p.pool.cleanupPooledConnections();
		assert 0==p.getActiveConnections();
		System.out.println(p.getStatusMessage());
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
		//select subset with other="true"
		start=System.currentTimeMillis();
		Collection<String> ids=p.getIDs("other", Boolean.TRUE);
		assert ids!=null;
		assert ids.size()>0;
		System.out.println("Time for selecting subset: "+(System.currentTimeMillis()-start));
		System.out.println("Read cache hits: "+p.getCacheHits());
		p.shutdown();
	}

	protected void readSomeRandomEntries(int numberOfInstances)throws Exception{
		for(int i=0;i<50;i++){
			Dao1 e=p.read(String.valueOf(rand.nextInt(numberOfInstances)));
			assert e!=null;
		}
	}
}
