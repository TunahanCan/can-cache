package com.can.net.protocol;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Hemen yürütülebilecek komutları kapsüller.
 */
public final class ImmediateCommand implements CommandAction
{
    private final Supplier<CommandResult> executor;

    public ImmediateCommand(Supplier<CommandResult> executor)
    {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public Supplier<CommandResult> executor()
    {
        return executor;
    }
}
