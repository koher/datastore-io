package org.koherent.datastore;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class DatastoreStreamCombinationTest {
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
	public void testWriteAndRead() {
		Key key = KeyFactory.createKey("kind", "name");

		byte[] bytes = new byte[DatastoreOutputStream.BLOB_SIZE * 10];
		Random random = new Random();
		random.nextBytes(bytes);

		{
			int offset = Math.abs(random.nextInt() % 10000);
			int length = bytes.length
					- (offset + Math.abs(random.nextInt() % 10000));

			try (BufferedOutputStream out = new BufferedOutputStream(
					new DatastoreOutputStream(key),
					DatastoreOutputStream.BUFFER_SIZE)) {
				out.write(bytes, offset, length);
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			try (BufferedInputStream in = new BufferedInputStream(
					new DatastoreInputStream(key),
					DatastoreInputStream.BUFFER_SIZE)) {
				byte[] readBytes = new byte[bytes.length];
				System.arraycopy(bytes, 0, readBytes, 0, offset);
				System.arraycopy(bytes, offset + length, readBytes, offset
						+ length, bytes.length - (offset + length));
				in.read(readBytes, offset, length);

				assertEqualsByteArrays(bytes, readBytes);
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}

		{
			try (BufferedOutputStream out = new BufferedOutputStream(
					new DatastoreOutputStream(key),
					DatastoreOutputStream.BUFFER_SIZE)) {
				for (byte b : bytes) {
					out.write(b & 0xff);
				}
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			try (BufferedInputStream in = new BufferedInputStream(
					new DatastoreInputStream(key),
					DatastoreInputStream.BUFFER_SIZE)) {
				byte[] readBytes = new byte[bytes.length];
				int readByte;
				for (int i = 0; (readByte = in.read()) >= 0; i++) {
					readBytes[i] = (byte) readByte;
				}

				assertEqualsByteArrays(bytes, readBytes);
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}

		{
			final int length = 7; // length by which BUFFER_SIZE cannot be divided

			try (BufferedOutputStream out = new BufferedOutputStream(
					new DatastoreOutputStream(key),
					DatastoreOutputStream.BUFFER_SIZE)) {
				for (int offset = 0; offset < bytes.length; offset += length) {
					out.write(bytes, offset,
							offset + length < bytes.length ? length
									: bytes.length - offset);
				}
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			try (BufferedInputStream in = new BufferedInputStream(
					new DatastoreInputStream(key),
					DatastoreInputStream.BUFFER_SIZE)) {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				byte[] readBytes = new byte[length];
				int readLength;
				while ((readLength = in.read(readBytes)) >= 0) {
					out.write(readBytes, 0, readLength);
				}

				assertEqualsByteArrays(bytes, out.toByteArray());
			} catch (IOException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
	}

	private static void assertEqualsByteArrays(byte[] expected, byte[] actual) {
		int length = expected.length;
		if (length != actual.length) {
			fail("expected.length: " + length + ", actual.length: "
					+ actual.length);
		}

		for (int i = 0; i < length; i++) {
			if (expected[i] != actual[i]) {
				StringBuilder expectedBuilder = new StringBuilder();
				StringBuilder actualBuilder = new StringBuilder();
				for (int offset = -10; i <= 10; i++) {
					int j = i + offset;
					if (0 <= j && j < length) {
						if (j == i) {
							expectedBuilder.append("[");
							actualBuilder.append("[");
						}
						expectedBuilder.append(String.format("%02x",
								expected[j]));
						actualBuilder.append(String.format("%02x", actual[j]));
						if (j == i) {
							expectedBuilder.append("]");
							actualBuilder.append("]");
						}
					}
				}

				fail("expected[" + i + "]: " + expectedBuilder.toString()
						+ ", actual[" + i + "]: " + actualBuilder.toString());
			}
		}
	}
}
