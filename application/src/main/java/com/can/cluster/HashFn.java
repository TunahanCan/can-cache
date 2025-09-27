package com.can.cluster;

/**
 * Tutarlı hash halkasında ve diğer dağıtım algoritmalarında kullanılmak üzere
 * anahtar baytlarını imzaya dönüştüren fonksiyonların sözleşmesini tanımlar.
 * Uygulamalar tek tip bir hashing stratejisi sağlayarak halka üzerinde düğüm ve
 * anahtar yerleşiminin belirlenmesine imkan verir.
 */
public interface HashFn {
    int hash(byte[] keyBytes);
}