package de.fzj.unicore.persist.externaldb;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import de.fzj.unicore.persist.Persist;
import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceProperties;
import de.fzj.unicore.persist.impl.Dao1;
import de.fzj.unicore.persist.impl.Dao2;
import de.fzj.unicore.persist.impl.MySQLPersist;
import de.fzj.unicore.persist.impl.PersistImpl;


/**
 * runs some basic functionality tests for the available 
 * (embedded) implementations
 * 
 * @author schuller
 */
public class TestExternalDBs {

	/**
	 * @return implementations of {@link Persist} for testing
	 */
	@DataProvider(name="data-provider")
	public Object[][] getImplementation(){
		return new Object[][]{
				{MySQLPersist.class, Boolean.FALSE},
		};
	}	

	
	@Test(dataProvider="data-provider")
	public void testBasicCRUD(Class<? extends PersistImpl<Dao1>> persistClass, Boolean cache)throws PersistenceException, InstantiationException, IllegalAccessException{
		PersistImpl<Dao1>p=persistClass.newInstance();
		PersistenceProperties props=new PersistenceProperties();
		setParams(props);
		p.setConfigSource(props);
		p.setDaoClass(Dao1.class);
		p.setCaching(cache);
		p.setDatabaseName("unicore");
		p.init();
		p.removeAll();
		
		Dao1 test=new Dao1();
		test.setId("1");
		test.setData("testdata");
		test.setOther("other-1");
		
		p.write(test);
		assert p.getIDs().size()==1;
		
		Dao1 test1 = p.read("1");
		assert test1!=null;
		assert test1.getData().equals("testdata");
		assert test1.getId().equals("1");
		
		test1.setData("testdata-changed");
		p.write(test1);
		assert p.getIDs().size()==1;
		test1 = p.read("1");
		assert test1!=null;
		assert test1.getData().equals("testdata-changed");
		assert test1.getId().equals("1");
		
		test=new Dao1();
		test.setId("2");
		test.setData("testdata-2");
		test.setOther("other-2");
		
		p.write(test);
		assert p.getIDs().size()==2;
		test=p.read("2");
		assert test!=null;
		assert test.getData().equals("testdata-2");
		assert test.getId().equals("2");
		
		p.remove("1");
		assert p.getIDs().size()==1;
		p.remove("2");
		assert p.getIDs().size()==0;
		
		test=p.read("1");
		assert test==null;
		test=p.read("2");
		assert test==null;
		
	}

	
	@Test(dataProvider="data-provider") 
	public void testAdditionalColumns(Class<? extends PersistImpl<Dao2>> persistClass, Boolean cache)throws Exception{
		PersistImpl<Dao2>p=persistClass.newInstance();
		PersistenceProperties props=new PersistenceProperties();
		setParams(props);
		p.setConfigSource(props);
		p.setDaoClass(Dao2.class);
		p.setCaching(cache);
		p.setDatabaseName("unicore");
		p.init();
		p.removeAll();
		Dao2 test=new Dao2();
		test.setId("1");
		test.setData("testdata");
		p.write(test);
		test.setField("new-content");
		p.write(test);
		Dao2 test1=p.read("1");
		assert test1!=null;
		assert test1.getField().equals("new-content");
		assert p.getIDs("foo", "new-content").size()==1;
	}
	
	private void setParams(PersistenceProperties props){
		props.setProperty("user", "unicore");
		props.setProperty("password", "unicore");
	}
	
}
