package com.cancache.agent.cluster.coordination.gossip;

import com.cancache.agent.cluster.coordination.NodeInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GossipMessageCodecTest
{
    @Test
    void shouldRoundTripPing()
            throws IOException
    {
        assertRoundTrip(message(GossipMessageType.PING));
    }

    @Test
    void shouldRoundTripAck()
            throws IOException
    {
        assertRoundTrip(new GossipMessage(GossipMessageType.ACK, "node-a", Map.of()));
    }

    @Test
    void shouldRoundTripGossip()
            throws IOException
    {
        assertRoundTrip(message(GossipMessageType.GOSSIP));
    }

    @Test
    void shouldRoundTripPingRequest()
            throws IOException
    {
        assertRoundTrip(message(GossipMessageType.PING_REQ));
    }

    @Test
    void shouldRejectOversizedPacket()
    {
        byte[] packet = new byte[GossipMessageCodec.MAX_PACKET_BYTES + 1];

        assertThrows(IOException.class, () -> GossipMessageCodec.decode(packet));
    }

    @Test
    void shouldRejectOversizedStringBeforeEncoding()
    {
        String oversizedSender = "x".repeat(GossipMessageCodec.MAX_SENDER_ID_BYTES + 1);
        GossipMessage message = new GossipMessage(GossipMessageType.ACK, oversizedSender, Map.of());

        assertThrows(IOException.class, () -> GossipMessageCodec.encode(message));
    }

    @Test
    void shouldRejectAbusiveMemberCount()
            throws IOException
    {
        byte[] packet = GossipMessageCodec.encode(
                new GossipMessage(GossipMessageType.ACK, "n", Map.of()));
        int count = GossipMessageCodec.MAX_MEMBERS + 1;
        packet[packet.length - 2] = (byte) (count >>> 8);
        packet[packet.length - 1] = (byte) count;

        assertThrows(IOException.class, () -> GossipMessageCodec.decode(packet));
    }

    @Test
    void shouldRejectOversizedEncodedMemberCount()
    {
        Map<String, GossipMember> members = new LinkedHashMap<>();
        for (int i = 0; i <= GossipMessageCodec.MAX_MEMBERS; i++) {
            String nodeId = "n" + i;
            members.put(nodeId, member(nodeId, MemberStatus.ALIVE, i));
        }
        GossipMessage message = new GossipMessage(GossipMessageType.GOSSIP, "sender", members);

        assertThrows(IOException.class, () -> GossipMessageCodec.encode(message));
    }

    @Test
    void shouldRejectTruncatedPacket()
            throws IOException
    {
        byte[] packet = GossipMessageCodec.encode(message(GossipMessageType.PING));
        byte[] truncated = Arrays.copyOf(packet, packet.length - 1);

        assertThrows(IOException.class, () -> GossipMessageCodec.decode(truncated));
    }

    @Test
    void shouldRejectTrailingBytes()
            throws IOException
    {
        byte[] packet = GossipMessageCodec.encode(
                new GossipMessage(GossipMessageType.ACK, "node-a", Map.of()));
        byte[] withTrailingByte = Arrays.copyOf(packet, packet.length + 1);

        assertThrows(IOException.class, () -> GossipMessageCodec.decode(withTrailingByte));
    }

    @Test
    void shouldRejectUnknownMessageType()
            throws IOException
    {
        byte[] packet = GossipMessageCodec.encode(
                new GossipMessage(GossipMessageType.ACK, "node-a", Map.of()));
        packet[5] = (byte) 0xff;

        assertThrows(IOException.class, () -> GossipMessageCodec.decode(packet));
    }

    @Test
    void shouldRejectMemberMapKeyMismatch()
    {
        GossipMember member = member("node-a", MemberStatus.ALIVE, 1L);
        GossipMessage message = new GossipMessage(
                GossipMessageType.GOSSIP, "sender", Map.of("alias", member));

        assertThrows(IOException.class, () -> GossipMessageCodec.encode(message));
    }

    private static GossipMessage message(GossipMessageType type)
    {
        Map<String, GossipMember> members = new LinkedHashMap<>();
        members.put("node-a", member("node-a", MemberStatus.ALIVE, 11L));
        members.put("node-b", member("node-b", MemberStatus.SUSPECT, 12L));
        members.put("node-c", member("node-c", MemberStatus.DEAD, 13L));
        return new GossipMessage(type, "sender-node", members);
    }

    private static GossipMember member(String nodeId, MemberStatus status, long epoch)
    {
        return new GossipMember(
                new NodeInfo(nodeId, "127.0.0." + epoch, 18_000 + (int) epoch),
                epoch,
                1_000_000L + epoch,
                status);
    }

    private static void assertRoundTrip(GossipMessage expected)
            throws IOException
    {
        GossipMessage actual = GossipMessageCodec.decode(GossipMessageCodec.encode(expected));

        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getSenderId(), actual.getSenderId());
        assertEquals(expected.getMembers().keySet(), actual.getMembers().keySet());
        for (Map.Entry<String, GossipMember> entry : expected.getMembers().entrySet()) {
            GossipMember expectedMember = entry.getValue();
            GossipMember actualMember = actual.getMembers().get(entry.getKey());
            assertEquals(expectedMember.getNodeInfo(), actualMember.getNodeInfo());
            assertEquals(expectedMember.getEpoch(), actualMember.getEpoch());
            assertEquals(expectedMember.getLastSeen(), actualMember.getLastSeen());
            assertEquals(expectedMember.getStatus(), actualMember.getStatus());
        }
    }
}
