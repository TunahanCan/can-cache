package com.can.codec;

public interface Codec<T>
{
    byte[] encode(T obj);
    T decode(byte[] bytes);
}