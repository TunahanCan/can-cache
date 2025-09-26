package com.can.cluster.coordination;

public record JoinHandshakeResult(long epoch, boolean accepted) {}