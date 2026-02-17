package com.cancache.agent.cluster.coordination;

public record JoinHandshakeResult(long epoch, boolean accepted) {}