package eu.unicore.persist.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Test;


public class TestClassScanner {

	@Test
	public void testGetIDByMethodAnnotation() throws Exception {
		Method m=ClassScanner.getGetIdMethod(Dao1.class);
		assertEquals("getId", m.getName());
		Object res=m.invoke(new Dao1(), (Object[])null);
		assertEquals("the id", res.toString());
	}

	@Test
	public void testGetIDByFieldAnnotation() throws Exception {
		Method m=ClassScanner.getGetIdMethod(Dao2.class);
		assertEquals("getId", m.getName());
		Object res = m.invoke(new Dao2(), (Object[])null);
		assertEquals("the id", res.toString());
	}

	@Test
	public void testColumnFieldAnnotation() throws Exception {
		List<ColumnDescriptor>list=ClassScanner.getColumns(Dao2.class);
		assertEquals(1, list.size());
		ColumnDescriptor cd=list.get(0);
		assertEquals("foo", cd.getColumn());
	}

	@Test
	public void testFaultyDAO1(){
		assertThrows(IllegalArgumentException.class,()->{
			ClassScanner.getGetIdMethod(FaultyDao1.class);
		});
	}

	@Test
	public void testFaultyDAO2(){
		assertThrows(IllegalArgumentException.class,()->{
			ClassScanner.getGetIdMethod(FaultyDao2.class);
		});
	}

}
