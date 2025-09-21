package com.can.codec;

import java.io.*;

public final class JavaSerializerCodec<T extends Serializable> implements Codec<T> {

    @Override
    public byte[] encode(T obj) {
        if (obj == null) return new byte[0];
        try (var baos = new ByteArrayOutputStream();
             var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) { throw new RuntimeException(e); }
    }
    @SuppressWarnings("unchecked")
    @Override
    public T decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        try (var bais = new ByteArrayInputStream(bytes);
             var ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) { throw new RuntimeException(e); }
    }
}