package eu.unicore.persist.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.FileInputStream;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import eu.unicore.persist.Persist;
import eu.unicore.persist.PersistenceFactory;
import eu.unicore.persist.PersistenceProperties;
import eu.unicore.persist.impl.Dao1;
import eu.unicore.persist.impl.H2Persist;

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
		assertNotNull(conv);
		
		Dao1 x=new Dao1();
		x.setId("1");
		x.setData("test");
		
		Persist<Dao1>inDB=(Persist<Dao1>)conv.getInput();
		inDB.write(x);
		conv.convert();
		
		Persist<Dao1>outDB=(Persist<Dao1>)conv.getOutput();
		
		Dao1 y=(Dao1)outDB.read("1");
		assertNotNull(y);
		assertEquals("test", y.getData());
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
		assertNotNull(conv);
		
		Dao1 x=new Dao1();
		x.setId("1");
		x.setData("test");
		
		Persist<Dao1>inDB=PersistenceFactory.get(new PersistenceProperties(in)).getPersist(Dao1.class, null);
		inDB.write(x);
		
		
		String[]args={inProps,outProps};
		Convert.main(args);
		
		Persist<Dao1>outDB=PersistenceFactory.get(new PersistenceProperties(out)).getPersist(Dao1.class, null);
		Dao1 d=outDB.read("1");
		assertNotNull(d);
		assertEquals("test", d.getData());
	}
}
