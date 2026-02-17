package com.cancache.agent.core.model;

/**
 * CAS operasyonunun sonucunu ve oluşturulan değeri temsil eder.
 */
public record CasResult(boolean success, CacheValue newValue) {
}
