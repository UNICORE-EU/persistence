package de.fzj.unicore.persist.cluster;

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.AfterClass;
import org.junit.Test;

import de.fzj.unicore.persist.impl.LockSupport;

public class TestLockSupport {
	
	@AfterClass
	public static void cleanup(){
		Cluster.shutdownAll();
	}


	@Test	
	public void testBasic()throws Exception{
		
		String table="test";
		LockSupport ls=new LockSupport(false,table);
		
		Lock l1=ls.getOrCreateLock("1234");
		assert ls.getLockIfExists("1234")==l1;
		ls.cleanup();
		assert ls.getLockIfExists("1234")==null;
		
		ls.getOrCreateLock("1234");
		ls.cleanup("1234");
		assert ls.getLockIfExists("1234")==null;
		
	}
	

	@Test
	public void testDistributed()throws Exception{
		String table="test";
		LockSupport ls=new LockSupport(true,table);
		ls.setCluster(new Cluster(new File("src/test/resources/cluster1.yaml")));
		Lock l1=ls.getOrCreateLock("1234");
		assert(l1!=null);
		assert(l1.getClass().getName().contains("com.hazelcast"));
		assert(ls.getLockIfExists("1234")!=null);
		l1.lock();
		
		//setup a second instance
		Cluster i2=new Cluster(new File("src/test/resources/cluster1.yaml"));
		Lock l2=i2.getLock(table+"1234");
		boolean aquired=l2.tryLock();
		assert(aquired == false);
		
		l1.unlock();
		
		aquired=l2.tryLock();
		assert(aquired == true);

	}
	
	@Test
	public void testNonDistributed()throws Exception{
		LockSupport ls=new LockSupport(false,"test");
		Lock l1=ls.getOrCreateLock("1234");
		assert(l1!=null);
		assert(ls.getLockIfExists("1234")!=null);
		assert(l1 instanceof ReentrantLock);
	}
}
