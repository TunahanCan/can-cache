package com.can.cluster;

import java.time.Duration;

/**
 * Kümedeki her fiziksel veya mantıksal düğümün sunması gereken temel önbellek
 * operasyonlarını tarif eder. İstemci katmanı bu arayüzü kullanarak değer
 * yazma, okuma, silme ve düğüm kimliğini öğrenme işlemlerini soyutlar.
 */
public interface Node<K,V>
{
    void set(K key, V value, Duration ttl);
    V get(K key);
    boolean delete(K key);
    String id();
}