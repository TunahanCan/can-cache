package com.cancache.agent.service;

import com.cancache.agent.model.ConnectionRecord;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@ApplicationScoped
public class ConnectionTracker {

    private static final int MAX = 10;
    private final Deque<ConnectionRecord> records = new ConcurrentLinkedDeque<>();

    public void add(ConnectionRecord record)
    {
        records.addFirst(record);
        while (records.size() > MAX) {
            records.pollLast();
        }
    }

    public List<ConnectionRecord> latest() {
        return new ArrayList<>(records);
    }
}
