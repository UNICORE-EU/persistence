package de.fzj.unicore.persist.impl;

import java.util.Collection;
import java.util.Random;

import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fzj.unicore.persist.PersistenceProperties;


/**
 * tests the h2 server mode
 */
public class TestH2PersistServerMode {

	private static H2Persist<Dao1>p;

	private Random rand=new Random();

	private static final String port="65530";
	
	@BeforeClass
	public static void setUp()throws Exception{
		String[]args=new String[]{"-tcp", "-tcpPort", port, "-tcpPassword", "test", "-ifNotExists"};
		Server.main(args);
	}
	
	@AfterClass
	public static void cleanUp()throws Exception{
		p.dropTables();
		Server.shutdownTcpServer("tcp://localhost:65530","test",true,true);
	}

	protected void createPersist(){
		p=new H2Persist<Dao1>();
		p.setServerMode(true);
		
		PersistenceProperties cf=new PersistenceProperties();
		cf.setProperty(PersistenceProperties.DB_PORT, port);
		cf.setProperty(PersistenceProperties.DB_POOL_MAXSIZE, "5");
		cf.setDatabaseDirectory("target/test_data");
		p.setConfigSource(cf);
		p.setPersistenceDescriptor(PersistenceDescriptor.get(Dao1.class));
	}
	
	int numberOfInstances = 100;
	int size = 100;
	
	@Test
	public void perfTest()throws Exception{
		System.out.println("\n\n**** Running test with numInstances="+numberOfInstances+", data size="+size);
		createPersist();
		p.setDaoClass(Dao1.class);
		p.setCaching(true);
		long start=System.currentTimeMillis();
		p.init();
		
		assert p.connectionURL.contains("tcp");
		assert p.connectionURL.contains("AUTO_RECONNECT=TRUE");
		
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
		assert ids!=null;
		assert ids.size()>0;
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
