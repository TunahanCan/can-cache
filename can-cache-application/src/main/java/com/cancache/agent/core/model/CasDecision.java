package com.cancache.agent.core.model;

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

    public static CasDecision removeSuccess() {
        return new CasDecision(true, null, true, true, true);
    }

    public static CasDecision replaceExpired(CacheValue newValue) {
        return new CasDecision(true, newValue, true, true, false);
    }

    public static CasDecision noValueSuccess() {
        return new CasDecision(true, null, false, false, false);
    }
}
