package org.koherent.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class DatastoreOutputStreamTest {
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
			new DatastoreOutputStream(null);
			fail("Must throw an exception.");
		} catch (IllegalArgumentException e) {
		}
	}

	@Test
	public void testRewrite() {
		Key key = KeyFactory.createKey("kind", "name");

		{
			byte[] bytes = { 2, 3, 5, 7, 11, 13 };

			try (DatastoreOutputStream out = new DatastoreOutputStream(key)) {
				out.write(bytes);
			} catch (IllegalArgumentException | IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			Query query = new Query(key);
			List<Entity> entities = DatastoreServiceFactory
					.getDatastoreService().prepare(query)
					.asList(FetchOptions.Builder.withLimit(100));
			assertEquals(1, entities.size());
			DatastoreStreamCombinationTest.assertEqualsByteArrays(bytes,
					((Blob) entities.get(0).getProperty("b")).getBytes());
		}

		{
			byte[] bytes = { 2, 3, 5, 7 };

			try (DatastoreOutputStream out = new DatastoreOutputStream(key)) {
				out.write(bytes);
			} catch (IllegalArgumentException | IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			Query query = new Query(key);
			List<Entity> entities = DatastoreServiceFactory
					.getDatastoreService().prepare(query)
					.asList(FetchOptions.Builder.withLimit(100));
			assertEquals(1, entities.size());
			DatastoreStreamCombinationTest.assertEqualsByteArrays(bytes,
					((Blob) entities.get(0).getProperty("b")).getBytes());
		}
	}

	@Test
	public void testClose() {
		Key key = KeyFactory.createKey("kind", "name");

		{
			DatastoreOutputStream out = new DatastoreOutputStream(key);

			try {
				out.write(new byte[DatastoreOutputStream.BLOB_SIZE * 10]);
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			try {
				out.close();
				fail("Must throw an exception.");
			} catch (IOException e) {
			}

			DatastoreService datastore = DatastoreServiceFactory
					.getDatastoreService();
			assertEquals(
					10,
					datastore.prepare(new Query(key))
							.asList(FetchOptions.Builder.withOffset(0)).size());
		}

		{
			DatastoreOutputStream out = new DatastoreOutputStream(key);

			try {
				out.write(0x0);
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			DatastoreService datastore = DatastoreServiceFactory
					.getDatastoreService();
			assertEquals(
					1,
					datastore.prepare(new Query(key))
							.asList(FetchOptions.Builder.withOffset(0)).size());
		}

	}
}
