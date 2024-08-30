package eu.unicore.persist.impl;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Random;

import org.h2.tools.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import eu.unicore.persist.PersistenceProperties;


/**
 * tests the h2 server mode
 */
public class TestH2PersistServerMode {

	private static H2Persist<Dao1>p;

	private Random rand=new Random();

	private static final String port="65530";

	@BeforeAll
	public static void setUp()throws Exception{
		String[]args=new String[]{"-tcp", "-tcpPort", port, "-tcpPassword", "test", "-ifNotExists"};
		Server.main(args);
	}

	@AfterAll
	public static void cleanUp()throws Exception{
		p.dropTables();
		Server.shutdownTcpServer("tcp://localhost:65530","test",true,true);
	}

	protected void createPersist(){
		p = new H2Persist<>(Dao1.class, null);
		p.setServerMode(true);
		PersistenceProperties cf=new PersistenceProperties();
		cf.setProperty(PersistenceProperties.DB_PORT, port);
		cf.setProperty(PersistenceProperties.DB_POOL_MAXSIZE, "5");
		cf.setDatabaseDirectory("target/test_data");
		p.setConfigSource(cf);
	}

	int numberOfInstances = 100;
	int size = 100;

	@Test
	public void perfTest()throws Exception{
		System.out.println("\n\n**** Running test with numInstances="+numberOfInstances+", data size="+size);
		createPersist();
		p.setCaching(true);
		long start=System.currentTimeMillis();
		p.init();
		
		assertTrue(p.connectionURL.contains("tcp"));
		assertTrue(p.connectionURL.contains("AUTO_RECONNECT=TRUE"));
		
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
		System.out.println("Time for reading: "+(System.currentTimeMillis()-start));
		//select subset woth other="true"
		start=System.currentTimeMillis();
		Collection<String> ids=p.getIDs("other", Boolean.TRUE);
		assertNotNull(ids);
		assertTrue(ids.size()>0);
		System.out.println("Time for selecting subset: "+(System.currentTimeMillis()-start));
		System.out.println("Read cache hits: "+p.getCacheHits());
		
	}

	protected void readSomeRandomEntries(int numberOfInstances)throws Exception{
		for(int i=0;i<50;i++){
			Dao1 e=p.read(String.valueOf(rand.nextInt(numberOfInstances)));
			assert e!=null;
		}
	}

}
