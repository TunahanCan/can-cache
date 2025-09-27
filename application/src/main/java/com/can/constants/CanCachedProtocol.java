package com.can.constants;

public interface CanCachedProtocol
{
    // Protocol identification used for logging and diagnostics.
    String PROTOCOL = "cancached";

    // Storage commands that either create or mutate values.
    String SET = "set";
    String ADD = "add";
    String APPEND = "append";
    String PREPEND = "prepend";
    String REPLACE = "replace";
    String CAS = "cas";

    // Retrieval commands.
    String GET = "get";
    String GETS = "gets";

    // Mutation commands that operate on numeric values.
    String INCR = "incr";
    String DECR = "decr";

    // Key management commands.
    String DELETE = "delete";
    String TOUCH = "touch";

    // Server administration commands.
    String FLUSH_ALL = "flush_all";
    String STATS = "stats";
    String VERSION = "version";
    String QUIT = "quit";

    // Generic error response used when a command cannot be processed.
    String ERROR = "ERROR";
}
