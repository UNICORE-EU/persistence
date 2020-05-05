package de.fzj.unicore.persist.cluster;

import java.io.File;
import java.io.FileNotFoundException;

import org.testng.annotations.Test;

import de.fzj.unicore.persist.PersistenceProperties;

@Test
public class TestCluster {

	public void testCacheClusterInstances()throws FileNotFoundException{
		String config="src/test/resources/cluster1.xml";
		Cluster c1=Cluster.getInstance(config);
		File f=new File(config);
		Cluster c2=Cluster.getInstance(f.getAbsolutePath());
		assert(c1==c2);
		
		Cluster.shutdownAll();
	}
	
	public void testGetDefaultInstance()throws FileNotFoundException{
		Cluster c=Cluster.getDefaultInstance();
		assert(c==null);
		System.setProperty(PersistenceProperties.DB_CLUSTER_CONFIG, "src/test/resources/cluster1.xml");
		c=Cluster.getDefaultInstance();
		assert(c!=null);
		
		Cluster.shutdownAll();
	}
	
}
