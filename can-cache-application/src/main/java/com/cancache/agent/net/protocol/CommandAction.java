package com.cancache.agent.net.protocol;

/**
 * Bir istemci komutunun nasıl işleneceğini temsil eden işaretçi arayüz.
 */
public sealed interface CommandAction permits ImmediateCommand, StorageCommand
{
}
