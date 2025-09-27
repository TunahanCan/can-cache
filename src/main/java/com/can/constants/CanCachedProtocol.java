package com.can.constants;

public interface CanCachedProtocol
{
    // Protocol identification used for logging and diagnostics.
    public static final String PROTOCOL = "cancached";

    // Storage commands that either create or mutate values.
    public static final String SET = "set";
    public static final String ADD = "add";
    public static final String APPEND = "append";
    public static final String PREPEND = "prepend";
    public static final String REPLACE = "replace";
    public static final String CAS = "cas";

    // Retrieval commands.
    public static final String GET = "get";
    public static final String GETS = "gets";

    // Mutation commands that operate on numeric values.
    public static final String INCR = "incr";
    public static final String DECR = "decr";

    // Key management commands.
    public static final String DELETE = "delete";
    public static final String TOUCH = "touch";

    // Server administration commands.
    public static final String FLUSH_ALL = "flush_all";
    public static final String STATS = "stats";
    public static final String VERSION = "version";
    public static final String QUIT = "quit";

    // Generic error response used when a command cannot be processed.
    public static final String ERROR = "ERROR";
}
