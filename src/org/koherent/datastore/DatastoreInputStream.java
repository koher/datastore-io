package org.koherent.datastore;

import java.io.IOException;
import java.io.InputStream;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;

public class DatastoreInputStream extends InputStream {
	public static final int BUFFER_SIZE = DatastoreOutputStream.BLOB_SIZE;

	private Key key;

	private int entityIndex;
	private Key currentKey;
	private int position;

	public DatastoreInputStream(Key key) {
		if (key == null) {
			throw new IllegalArgumentException("'key' cannot be null.");
		}
		this.key = key;

		entityIndex = 0;
		currentKey = KeyFactory.createKey(this.key, this.key.getKind(),
				Integer.toString(entityIndex));
		position = 0;
	}

	@Override
	public int read() throws IOException {
		byte[] aByte = new byte[1];
		int length;
		while ((length = read(aByte)) == 0) {
		}

		if (length < 0) {
			return -1;
		}

		return aByte[0] & 0xff;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		throwExceptionIfClosed();

		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		try {
			Transaction transaction = datastore.beginTransaction();
			try {
				return read(datastore, transaction, b, off, len);
			} catch (DatastoreFailureException e) {
				transaction.rollback();
				throw new IOException(e);
			} catch (RuntimeException e) {
				transaction.rollback();
				throw e;
			}
		} catch (DatastoreFailureException e) {
			throw new IOException(e);
		}
	}

	private int read(DatastoreService datastore, Transaction transaction,
			byte[] b, int off, int len) {
		if (position >= BUFFER_SIZE) {
			entityIndex++;
			currentKey = KeyFactory.createKey(this.key, this.key.getKind(),
					Integer.toString(entityIndex));
			position = 0;
		}

		int requiredLength = position + len;
		if (requiredLength > BUFFER_SIZE) {
			int firstLength = BUFFER_SIZE - position;
			int readLength = read(datastore, transaction, b, off, firstLength);
			if (readLength < firstLength) {
				return readLength;
			}

			int otherLength = read(datastore, transaction, b,
					off + firstLength, len - firstLength);
			if (otherLength < 0) {
				return firstLength;
			} else {
				return firstLength + otherLength;
			}
		}

		Entity entity;
		byte[] bytes;
		try {
			entity = datastore.get(transaction, currentKey);
			bytes = ((Blob) entity
					.getProperty(DatastoreOutputStream.PROPERTY_NAME))
					.getBytes();
			int readLength = Math.min(len, bytes.length - position);
			System.arraycopy(bytes, position, b, off, readLength);

			position += readLength;

			return readLength;
		} catch (EntityNotFoundException e) {
			return -1;
		}
	}

	@Override
	public void close() throws IOException {
		throwExceptionIfClosed();
		currentKey = null;
	}

	private void throwExceptionIfClosed() throws IOException {
		if (currentKey == null) {
			throw new IOException("This stream has been already closed.");
		}
	}
}
