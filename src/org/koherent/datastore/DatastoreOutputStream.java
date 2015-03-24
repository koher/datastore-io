package org.koherent.datastore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.Query.FilterOperator;

public class DatastoreOutputStream extends OutputStream {
	public static final int BUFFER_SIZE = 1000000 - 10000;

	static final String PROPERTY_NAME = "b";

	private Key key;

	private int entityIndex;
	private Key currentKey;
	private int position;

	public DatastoreOutputStream(Key key) throws IllegalArgumentException {
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
	public void write(int b) throws IOException {
		write(new byte[(byte) b]);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		throwExceptionIfClosed();

		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		try {
			Transaction transaction = datastore.beginTransaction();
			try {
				write(datastore, transaction, b, off, len);
				transaction.commit();
			} catch (DatastoreFailureException
					| ConcurrentModificationException e) {
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

	private void write(DatastoreService datastore, Transaction transaction,
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
			write(datastore, transaction, b, off, firstLength);
			write(datastore, transaction, b, off + firstLength, len
					- firstLength);
			return;
		}

		Entity entity;
		byte[] bytes;
		try {
			entity = datastore.get(transaction, currentKey);
			bytes = ((Blob) entity.getProperty(PROPERTY_NAME)).getBytes();
			if (requiredLength != bytes.length) {
				byte[] newBytes = new byte[requiredLength];
				System.arraycopy(bytes, 0, newBytes, 0, position);
				bytes = newBytes;
			}
			System.arraycopy(b, off, bytes, position, len);
		} catch (EntityNotFoundException e) {
			entity = new Entity(currentKey);
			if (off == 0 && b.length == len) {
				bytes = b;
			} else {
				bytes = new byte[requiredLength];
				System.arraycopy(b, off, bytes, 0, len);
			}
		}
		entity.setUnindexedProperty(PROPERTY_NAME, new Blob(bytes));

		position += len;

		datastore.put(entity);
	}

	@Override
	public void close() throws IOException {
		throwExceptionIfClosed();

		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		try {
			Transaction transaction = datastore.beginTransaction();

			try {
				Query query = new Query(key); // Ancestor queries have strong consistency
				query.setFilter(new Query.FilterPredicate(
						Entity.KEY_RESERVED_PROPERTY,
						FilterOperator.GREATER_THAN, currentKey));
				query.setKeysOnly();
				List<Key> keys = new ArrayList<Key>();
				for (Entity entity : datastore.prepare(query).asIterable()) {
					keys.add(entity.getKey());
				}
				datastore.delete(transaction, keys);

				transaction.commit();
			} catch (DatastoreFailureException
					| ConcurrentModificationException e) {
				transaction.rollback();
				throw new IOException(e);
			}
		} catch (DatastoreFailureException e) {
			throw new IOException(e);
		}

		currentKey = null;
	}

	private void throwExceptionIfClosed() throws IOException {
		if (currentKey == null) {
			throw new IOException("This stream has been already closed.");
		}
	}
}
