datastore-io
========================

_datastore-io_ provides the output and input streams to/from the Datastore for [GAE/J](https://cloud.google.com/appengine/docs/java/), `DatastoreOutputStream` and `DatastoreInputStream`, which can be used instead of `FileOutputStream` and `FileInputStream`.

```java
// OutputStream to the Datastore
try (BufferedOutputStream out = new BufferedOutputStream(
            new DatastoreOutputStream(key),
            DatastoreOutputStream.BUFFER_SIZE)) {
    out.write(bytes);
    out.flush();
} catch (IOException e) {
    e.printStackTrace();
}
```

```java
// InputStream from the Datastore
try (BufferedInputStream in = new BufferedInputStream(
            new DatastoreInputStream(key),
            DatastoreInputStream.BUFFER_SIZE)) {
    in.read(bytes);
} catch (IOException e) {
    e.printStackTrace();
}
```

License
------------------------

[The MIT License](LICENSE)
