package eu.unicore.persist.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import eu.unicore.persist.Persist;
import eu.unicore.persist.PersistenceFactory;
import eu.unicore.persist.PersistenceProperties;
import eu.unicore.persist.impl.Dao1;
import eu.unicore.persist.impl.H2Persist;

public class TestExportImport {

	@Test
	@SuppressWarnings("unchecked")
	public void testExport() throws Exception{
		Properties in=new Properties();
		in.setProperty("class", Dao1.class.getName());
		in.setProperty("persistence.class", H2Persist.class.getName());
		in.setProperty("persistence."+PersistenceProperties.DB_DIRECTORY, "target/testdata");
		String json = "target/exported.json";
		Export export=new Export(in, json);
		assertNotNull(export);
		Dao1 x=new Dao1();
		x.setId("1");
		x.setData("test");
		Dao1 x1=new Dao1();
		x1.setId("2");
		x1.setData("another test");
		Persist<Dao1>inDB=(Persist<Dao1>)export.getInput();
		inDB.write(x);
		inDB.write(x1);
		export.doExport();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testImport() throws Exception{
		Properties out=new Properties();
		out.setProperty("class", Dao1.class.getName());
		out.setProperty("persistence.class", H2Persist.class.getName());
		out.setProperty("persistence."+PersistenceProperties.DB_DIRECTORY, "target/testdata");
		String json="[{\"data\":\"import test\",\"other\":\"\",\"id\":\"1\"},\n"+
		  "{\"data\":\"another test\",\"other\":\"foo\",\"id\":\"2\"}\n]";
		System.out.println("Reading from "+json);
		Import importer=new Import(json,out);
		assertNotNull(importer);
		importer.doImport();
		Persist<Dao1>inDB=(Persist<Dao1>)importer.getOutput();
		Dao1 x = inDB.read("1");
		assertNotNull(x);
		Dao1 x2 = inDB.read("2");
		assertNotNull(x2);
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testExportImport() throws Exception{
		Properties in=new Properties();
		in.setProperty("class", Dao1.class.getName());
		in.setProperty("persistence.class", H2Persist.class.getName());
		in.setProperty("persistence."+PersistenceProperties.DB_DIRECTORY, "target/testdata");
		String json = "target/exported.json";
		Class<Dao1> daoClass = (Class<Dao1>)Class.forName((String)in.remove("class"));
		Class<Persist<Dao1>> inPersistImpl = (Class<Persist<Dao1>>)Class.forName(in.getProperty("persistence.class"));
		String inTableName=(String)in.remove("tableName");
		Persist<Dao1> inDB = PersistenceFactory.get(new PersistenceProperties(in)).configurePersist(daoClass, inPersistImpl, inTableName);
		Dao1 x=new Dao1();
		x.setId("1");
		x.setData("test");
		Dao1 x1=new Dao1();
		x1.setId("2");
		x1.setData("another test");
		inDB.write(x);
		inDB.write(x1);
		String[] args = new String[]{"src/test/resources/export.properties", json};
		Export.main(args);
		args = new String[]{json, "src/test/resources/import.properties"};
		Import.main(args);
	}
}