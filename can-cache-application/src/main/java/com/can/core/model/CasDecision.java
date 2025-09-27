package com.can.core.model;

/**
 * Segment içindeki CAS kararının nasıl uygulanacağını tanımlar.
 */
public record CasDecision(boolean success,
                          CacheValue newValue,
                          boolean removeExisting,
                          boolean notifyRemoval,
                          boolean recordAccess)
{
    public static CasDecision success(CacheValue newValue) {
        return new CasDecision(true, newValue, false, false, true);
    }

    public static CasDecision fail() {
        return
                new CasDecision(false, null, false, false, false);
    }

    public static CasDecision expired() {
        return
                new CasDecision(false, null, true, true, false);
    }
}
