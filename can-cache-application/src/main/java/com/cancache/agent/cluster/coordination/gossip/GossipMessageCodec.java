package com.cancache.agent.cluster.coordination.gossip;

import com.cancache.agent.cluster.coordination.NodeInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded binary codec for UDP gossip messages.
 *
 * <p>The wire format is deliberately independent from Java serialization so a
 * datagram cannot cause arbitrary classes to be instantiated. Every
 * variable-sized field is validated before allocation and the complete packet
 * must fit within a single gossip datagram.</p>
 */
final class GossipMessageCodec
{
    static final int MAX_PACKET_BYTES = 1500;
    static final int MAX_MEMBERS = 64;
    static final int MAX_SENDER_ID_BYTES = 256;
    static final int MAX_NODE_ID_BYTES = 256;
    static final int MAX_HOST_BYTES = 255;
    static final int MAX_MAP_KEY_BYTES = 256;

    private static final int MAGIC = 0x43434750; // CCGP
    private static final int VERSION = 1;

    private GossipMessageCodec()
    {
    }

    static byte[] encode(GossipMessage message) throws IOException
    {
        Objects.requireNonNull(message, "message");

        GossipMessageType type = Objects.requireNonNull(message.getType(), "message.type");
        byte[] senderId = encodeRequiredString(
                message.getSenderId(), MAX_SENDER_ID_BYTES, "senderId");
        Map<String, GossipMember> members = Objects.requireNonNull(
                message.getMembers(), "message.members");
        if (members.size() > MAX_MEMBERS) {
            throw new IOException("gossip member count exceeds limit: " + members.size());
        }

        List<EncodedMember> memberSnapshot = new ArrayList<>(members.size());
        int encodedSize = 4 + 1 + 1 + 2 + senderId.length + 2;
        for (Map.Entry<String, GossipMember> entry : members.entrySet()) {
            if (memberSnapshot.size() >= MAX_MEMBERS) {
                throw new IOException("gossip member count exceeds limit");
            }
            GossipMember member = Objects.requireNonNull(entry.getValue(), "gossip member");
            NodeInfo nodeInfo = Objects.requireNonNull(member.getNodeInfo(), "gossip member nodeInfo");
            if (!Objects.equals(entry.getKey(), nodeInfo.nodeId())) {
                throw new IOException("gossip member map key does not match nodeId");
            }
            if (nodeInfo.port() < 1 || nodeInfo.port() > 65_535) {
                throw new IOException("gossip member port is out of range: " + nodeInfo.port());
            }
            if (member.getEpoch() < 0L) {
                throw new IOException("gossip member epoch cannot be negative");
            }
            if (member.getLastSeen() < 0L) {
                throw new IOException("gossip member lastSeen cannot be negative");
            }

            EncodedMember encodedMember = new EncodedMember(
                    encodeRequiredString(entry.getKey(), MAX_MAP_KEY_BYTES, "member map key"),
                    encodeRequiredString(nodeInfo.nodeId(), MAX_NODE_ID_BYTES, "member nodeId"),
                    encodeRequiredString(nodeInfo.host(), MAX_HOST_BYTES, "member host"),
                    nodeInfo.port(),
                    member.getEpoch(),
                    member.getLastSeen(),
                    Objects.requireNonNull(member.getStatus(), "gossip member status"));
            memberSnapshot.add(encodedMember);
            encodedSize += encodedMember.encodedSize();
            ensurePacketSize(encodedSize);
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream(encodedSize);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(encodeType(type));
            writeBytes(out, senderId);
            out.writeShort(memberSnapshot.size());
            for (EncodedMember member : memberSnapshot) {
                writeBytes(out, member.mapKey());
                writeBytes(out, member.nodeId());
                writeBytes(out, member.host());
                out.writeInt(member.port());
                out.writeLong(member.epoch());
                out.writeLong(member.lastSeen());
                out.writeByte(encodeStatus(member.status()));
            }
        }
        byte[] packet = bytes.toByteArray();
        ensurePacketSize(packet.length);
        return packet;
    }

    static GossipMessage decode(byte[] packet) throws IOException
    {
        if (packet == null || packet.length == 0) {
            throw new IOException("gossip packet is empty");
        }
        ensurePacketSize(packet.length);

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet))) {
            if (in.readInt() != MAGIC) {
                throw new IOException("invalid gossip packet magic");
            }
            int version = in.readUnsignedByte();
            if (version != VERSION) {
                throw new IOException("unsupported gossip packet version: " + version);
            }

            GossipMessageType type = decodeType(in.readUnsignedByte());
            String senderId = readRequiredString(in, MAX_SENDER_ID_BYTES, "senderId");
            int memberCount = in.readUnsignedShort();
            if (memberCount > MAX_MEMBERS) {
                throw new IOException("gossip member count exceeds limit: " + memberCount);
            }

            Map<String, GossipMember> members = new LinkedHashMap<>(memberCount);
            for (int i = 0; i < memberCount; i++) {
                String mapKey = readRequiredString(in, MAX_MAP_KEY_BYTES, "member map key");
                String nodeId = readRequiredString(in, MAX_NODE_ID_BYTES, "member nodeId");
                String host = readRequiredString(in, MAX_HOST_BYTES, "member host");
                int port = in.readInt();
                long epoch = in.readLong();
                long lastSeen = in.readLong();
                MemberStatus status = decodeStatus(in.readUnsignedByte());

                if (!mapKey.equals(nodeId)) {
                    throw new IOException("gossip member map key does not match nodeId");
                }
                if (port < 1 || port > 65_535) {
                    throw new IOException("gossip member port is out of range: " + port);
                }
                if (epoch < 0L) {
                    throw new IOException("gossip member epoch cannot be negative");
                }
                if (lastSeen < 0L) {
                    throw new IOException("gossip member lastSeen cannot be negative");
                }

                GossipMember previous = members.put(mapKey, new GossipMember(
                        new NodeInfo(nodeId, host, port), epoch, lastSeen, status));
                if (previous != null) {
                    throw new IOException("duplicate gossip member: " + mapKey);
                }
            }
            if (in.available() != 0) {
                throw new IOException("unexpected trailing bytes in gossip packet");
            }
            return new GossipMessage(type, senderId, members);
        } catch (EOFException e) {
            throw new IOException("truncated gossip packet", e);
        }
    }

    private static byte[] encodeRequiredString(String value, int maxBytes, String field) throws IOException
    {
        if (value == null || value.isBlank()) {
            throw new IOException(field + " cannot be blank");
        }
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length > maxBytes) {
            throw new IOException(field + " exceeds byte limit: " + encoded.length);
        }
        return encoded;
    }

    private static String readRequiredString(DataInputStream in, int maxBytes, String field) throws IOException
    {
        int length = in.readUnsignedShort();
        if (length == 0 || length > maxBytes) {
            throw new IOException(field + " has invalid byte length: " + length);
        }
        byte[] encoded = in.readNBytes(length);
        if (encoded.length != length) {
            throw new EOFException("truncated " + field);
        }
        try {
            String decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded))
                    .toString();
            if (decoded.isBlank()) {
                throw new IOException(field + " cannot be blank");
            }
            return decoded;
        } catch (CharacterCodingException e) {
            throw new IOException(field + " contains invalid UTF-8", e);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException
    {
        out.writeShort(value.length);
        out.write(value);
    }

    private static void ensurePacketSize(int size) throws IOException
    {
        if (size > MAX_PACKET_BYTES) {
            throw new IOException("gossip packet exceeds byte limit: " + size);
        }
    }

    private static int encodeType(GossipMessageType type)
    {
        return switch (type) {
            case GOSSIP -> 1;
            case PING -> 2;
            case ACK -> 3;
            case PING_REQ -> 4;
        };
    }

    private static GossipMessageType decodeType(int value) throws IOException
    {
        return switch (value) {
            case 1 -> GossipMessageType.GOSSIP;
            case 2 -> GossipMessageType.PING;
            case 3 -> GossipMessageType.ACK;
            case 4 -> GossipMessageType.PING_REQ;
            default -> throw new IOException("unknown gossip message type: " + value);
        };
    }

    private static int encodeStatus(MemberStatus status)
    {
        return switch (status) {
            case ALIVE -> 1;
            case SUSPECT -> 2;
            case DEAD -> 3;
        };
    }

    private static MemberStatus decodeStatus(int value) throws IOException
    {
        return switch (value) {
            case 1 -> MemberStatus.ALIVE;
            case 2 -> MemberStatus.SUSPECT;
            case 3 -> MemberStatus.DEAD;
            default -> throw new IOException("unknown gossip member status: " + value);
        };
    }

    private record EncodedMember(
            byte[] mapKey,
            byte[] nodeId,
            byte[] host,
            int port,
            long epoch,
            long lastSeen,
            MemberStatus status)
    {
        private int encodedSize()
        {
            return 2 + mapKey.length
                    + 2 + nodeId.length
                    + 2 + host.length
                    + 4 + 8 + 8 + 1;
        }
    }
}
