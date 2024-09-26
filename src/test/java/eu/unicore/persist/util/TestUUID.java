package eu.unicore.persist.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;


public class TestUUID {

	@Test
	public void testNewUniqueID() {
		List<String>ids = new ArrayList<>();
		for(int i=0; i<10000; i++) {
			String s = UUID.newUniqueID();
			assertTrue(s.length()>=7);
			assertFalse(ids.contains(s));
			ids.add(s);
		}
	}
}
