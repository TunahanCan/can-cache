package com.can.constants;

public final class NodeProtocol
{
    // 'S' komutu, remote node'a gelen veriyi SET işlemi ile saklamak için gönderilir.
    public static final byte CMD_SET = 'S';

    // 'X' komutu, Compare-And-Swap (CAS) operasyonu için kullanılır.
    public static final byte CMD_CAS = 'X';

    // 'G' komutu, istenen anahtardaki veriyi GET isteği olarak talep eder.
    public static final byte CMD_GET = 'G';

    // 'D' komutu, belirtilen anahtarı DELETE isteği ile siler.
    public static final byte CMD_DELETE = 'D';

    // 'C' komutu, node üzerindeki tüm veriyi CLEAR işlemi ile temizler.
    public static final byte CMD_CLEAR = 'C';

    // 'J' komutu, yeni bir node'un kümeye JOIN el sıkışmasını başlatmasını sağlar.
    public static final byte CMD_JOIN = 'J';

    // 'R' komutu, bir node'dan tam veri akışı (STREAM) talep eder.
    public static final byte CMD_STREAM = 'R';

    // 'H' komutu, uzak nodun anti-entropy Digest değerini istemek için gönderilir.
    public static final byte CMD_DIGEST = 'H';

    // 'O' yanıtı, isteğin başarılı olduğunu (OK) belirtir.
    public static final byte RESP_OK = 'O';

    // 'H' yanıtı, GET sonucunda verinin bulunduğunu (HIT) bildirir.
    public static final byte RESP_HIT = 'H';

    // 'M' yanıtı, GET sonucunda verinin bulunamadığını (MISS) gösterir.
    public static final byte RESP_MISS = 'M';

    // 'T' yanıtı, boolean dönen işlemlerde TRUE sonucunu ifade eder.
    public static final byte RESP_TRUE = 'T';

    // 'F' yanıtı, boolean dönen işlemlerde FALSE sonucunu ifade eder.
    public static final byte RESP_FALSE = 'F';

    // 'A' yanıtı, join el sıkışmasının ACCEPT edildiğini bildirir.
    public static final byte RESP_ACCEPT = 'A';

    // 'R' yanıtı, join isteğinin REDDEDİLDİĞİNİ/yeniden yönlendirilmesi gerektiğini belirtir.
    public static final byte RESP_REJECT = 'R';

    // 1 baytlık chunk işaretleyicisi, STREAM yanıtında yeni bir kaydın geldiğini gösterir.
    public static final byte STREAM_CHUNK_MARKER = 1;

    // 0 baytlık işaretleyici, STREAM yanıtında aktarımın sona erdiğini gösterir.
    public static final byte STREAM_END_MARKER = 0;
}
