package eu.unicore.persist.impl;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Test;


public class TestClassScanner {

	@Test
	public void testGetIDByMethodAnnotation(){
		Method m=ClassScanner.getGetIdMethod(Dao1.class);
		assert(m.getName().equals("getId"));
		try{
			Object res=m.invoke(new Dao1(), (Object[])null);
			assert(res.toString().equals("the id"));
		}catch(Exception e){}
	}
	
	@Test
	public void testGetIDByFieldAnnotation(){
		Method m=ClassScanner.getGetIdMethod(Dao2.class);
		assert(m.getName().equals("getId"));
		try{
			Object res=m.invoke(new Dao2(), (Object[])null);
			assert(res.toString().equals("the id"));
		}catch(Exception e){}
	}
	
	@Test
	public void testColumnFieldAnnotation(){
		List<ColumnDescriptor>list=ClassScanner.getColumns(Dao2.class);
		assert list.size()==1;
		ColumnDescriptor cd=list.get(0);
		assert cd.getColumn().equalsIgnoreCase("foo");
	}
	
	
	@Test(expected = IllegalArgumentException.class)
	public void testFaultyDAO1(){
		ClassScanner.getGetIdMethod(FaultyDao1.class);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testFaultyDAO2(){
		ClassScanner.getGetIdMethod(FaultyDao2.class);
	}
	
	
	
}
