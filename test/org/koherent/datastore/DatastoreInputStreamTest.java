package org.koherent.datastore;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class DatastoreInputStreamTest {
	private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
			new LocalDatastoreServiceTestConfig());

	@Before
	public void setUp() {
		helper.setUp();
	}

	@After
	public void tearDown() {
		helper.tearDown();
	}

	@Test
	public void testConstructor() {
		try {
			new DatastoreInputStream(null);
			fail("Must throw an exception.");
		} catch (IllegalArgumentException e) {
		}
	}

	@Test
	public void testClose() {
		DatastoreInputStream in = new DatastoreInputStream(
				KeyFactory.createKey("kind", "name"));

		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			in.read();
			fail("Must throw an exception.");
		} catch (IOException e) {
		}

		try {
			in.close();
			fail("Must throw an exception.");
		} catch (IOException e) {
		}
	}

	@Test
	public void testUnexisting() throws IOException {
		DatastoreInputStream in = new DatastoreInputStream(
				KeyFactory.createKey("kind", "unexisting"));
		assertEquals(-1, in.read());
		in.close();
	}
}
