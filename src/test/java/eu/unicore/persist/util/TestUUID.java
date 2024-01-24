package eu.unicore.persist.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;


public class TestUUID {

	@Test
	public void testNewUniqueID() {
		List<String>ids = new ArrayList<>();
		for(int i=0; i<10000; i++) {
			String s = UUID.newUniqueID();
			assertTrue(s.length()>8);
			assertFalse(ids.contains(s));
			ids.add(s);
		}
	}

}
