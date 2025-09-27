package com.can.codec;

/**
 * Anahtar ve değer nesnelerinin ağ, disk veya başka bileşenler arasında taşınması
 * için bayt dizisine dönüştürülmesini ve tekrar nesne haline getirilmesini
 * sağlayan kodlayıcı sözleşmesidir. Farklı türler için farklı implementasyonlar
 * yazılarak önbellek motorunun genellenmiş şekilde çalışması sağlanır.
 */
public interface Codec<T>
{
    byte[] encode(T obj);
    T decode(byte[] bytes);
}