package com.can.constants;

public final class NodeProtocol
{
    public static final byte CMD_SET = 'S';
    public static final byte CMD_CAS = 'X';
    public static final byte CMD_GET = 'G';
    public static final byte CMD_DELETE = 'D';
    public static final byte CMD_CLEAR = 'C';
    public static final byte RESP_OK = 'O';
    public static final byte RESP_HIT = 'H';
    public static final byte RESP_MISS = 'M';
    public static final byte RESP_TRUE = 'T';
    public static final byte RESP_FALSE = 'F';
}
