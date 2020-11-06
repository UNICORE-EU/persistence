package de.fzj.unicore.persist.util;

import java.io.FileInputStream;
import java.util.Properties;

import org.junit.Test;

import de.fzj.unicore.persist.Persist;
import de.fzj.unicore.persist.PersistenceFactory;
import de.fzj.unicore.persist.PersistenceProperties;
import de.fzj.unicore.persist.impl.Dao1;
import de.fzj.unicore.persist.impl.H2Persist;

public class TestConvert {

	@Test
	@SuppressWarnings("unchecked")
	public void test1() throws Exception{
		Properties in=new Properties();
		in.setProperty("class", Dao1.class.getName());
		in.setProperty("persistence.class", H2Persist.class.getName());
		in.setProperty("persistence."+PersistenceProperties.DB_DIRECTORY, "./target/testdata");
		
		Properties out=new Properties();
		out.setProperty("class", Dao1.class.getName());
		out.setProperty("persistence.class", H2Persist.class.getName());
		out.setProperty("persistence."+PersistenceProperties.DB_DIRECTORY, "./target/testdata2");
		
		Convert conv=new Convert(in,out);
		assert conv!=null;
		
		Dao1 x=new Dao1();
		x.setId("1");
		x.setData("test");
		
		Persist<Dao1>inDB=(Persist<Dao1>)conv.getInput();
		inDB.write(x);
		conv.convert();
		
		Persist<Dao1>outDB=(Persist<Dao1>)conv.getOutput();
		
		Dao1 y=(Dao1)outDB.read("1");
		
		assert y!=null;
		assert "test".equals(y.getData());
		
		conv.shutDown();
	}
	
	@Test
	public void testMain() throws Exception{
		String inProps="src/test/resources/convert1.properties";
		String outProps="src/test/resources/convert2.properties";
		
		Properties in=new Properties();
		in.load(new FileInputStream(inProps));
		
		Properties out=new Properties();
		out.load(new FileInputStream(outProps));
		out.setProperty("class", Dao1.class.getName());
		out.setProperty("persistence.class", H2Persist.class.getName());
		
		Convert conv=new Convert(in,out);
		assert conv!=null;
		
		Dao1 x=new Dao1();
		x.setId("1");
		x.setData("test");
		
		Persist<Dao1>inDB=PersistenceFactory.get(new PersistenceProperties(in)).getPersist(Dao1.class);
		inDB.write(x);
		
		
		String[]args={inProps,outProps};
		Convert.main(args);
		
		Persist<Dao1>outDB=PersistenceFactory.get(new PersistenceProperties(out)).getPersist(Dao1.class);
		Dao1 d=outDB.read("1");
		assert(d!=null);
		assert("test".equals(d.getData()));
	}
}
