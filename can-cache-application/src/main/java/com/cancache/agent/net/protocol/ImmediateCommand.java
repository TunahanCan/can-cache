package com.cancache.agent.net.protocol;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Hemen yürütülebilecek komutları kapsüller.
 */
public record ImmediateCommand(Supplier<CommandResult> executor) implements CommandAction
{
    public ImmediateCommand
    {
        Objects.requireNonNull(executor, "executor");
    }
}
