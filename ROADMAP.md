# can-cache Roadmap

<div align="center">
  <a href="#en">🇬🇧 English</a>
  &nbsp;|&nbsp;
  <a href="#tr">🇹🇷 Türkçe</a>
</div>

---

<a id="tr"></a>
## 🇹🇷 Türkçe

Bu roadmap, `can-cache` projesini production-ready bir dağıtık önbellek sistemine dönüştürmek için önceliklendirilmiş görevleri içerir.

### 📊 Öncelik Matrisi

| Öncelik | Etki | Efor | Kategori |
|---------|------|------|----------|
| 🔴 Kritik | Veri kaybı/kesinti riski | - | Hemen yapılmalı |
| 🟠 Yüksek | Güvenilirlik/tutarlılık | - | Sprint 1 |
| 🟡 Orta | Operasyonel iyileştirme | - | Sprint 2 |
| 🟢 Düşük | Nice-to-have | - | Backlog |

---

## 🔴 PHASE 1: Kritik Düzeltmeler (Hafta 1)

### 1.1 ClusterClient Read Path Exception Handling
**Sorun:** `ClusterClient.get()` ilk replica exception atarsa sonraki denenmiyor.  
**Etki:** Tek node geçici hatası tüm read'i kırıyor.  
**Çözüm:**
```java
// ClusterClient.java:88-98
public String get(String key) {
    List<Node<String, String>> nodes = replicas(key);
    for (Node<String, String> node : nodes) {
        try {
            String value = node.get(key);
            if (value != null) return value;
        } catch (RuntimeException e) {
            LOG.debugf(e, "Failed to read key %s from node %s, trying next", key, node.id());
            continue;
        }
    }
    return null;
}
```
**Dosya:** `can-cache-application/.../cluster/ClusterClient.java`  
**Test:** `ClusterClientTest` - replica failure senaryosu ekle

---

### 1.2 Replication Factor Default Değişikliği
**Sorun:** `app.cluster.replication-factor=1` ile quorum/replication pratikte devre dışı.  
**Etki:** Tek node kaybında veri kaybı.  
**Çözüm:**
```properties
# application.properties
app.cluster.replication-factor=3
```
**Dosya:** `can-cache-application/src/main/resources/application.properties`

---

### 1.3 Touch Command Response Düzeltmesi
**Sorun:** `handleTouch` immediate-expire durumunda yanlış `NOT_FOUND` dönüyor.  
**Etki:** Protokol uyumsuzluğu, client-side karışıklık.  
**Çözüm:** TTL=0 ile silme başarılıysa `TOUCHED` dön.  
**Dosya:** `can-cache-application/.../net/CanCachedServer.java:506`

---

## 🟠 PHASE 2: Veri Güvenilirliği (Hafta 2-3)

### 2.1 Hinted Handoff Genişletme
**Sorun:** Sadece exception'da hint kaydediliyor; `false` dönen write'lar kaçıyor.  
**Etki:** Replica drift, tutarsız veri.  
**Çözüm:**
```java
// ClusterClient.java - set/delete/cas metodlarında
if (!ok) {
    hintedHandoffService.recordSet(node.id(), key, value, ttl);
}
```
**Dosyalar:**
- `ClusterClient.java:72-76`
- `ClusterClient.java:111-114`
- `ClusterClient.java:147-149`

---

### 2.2 Agent High Availability
**Sorun:** Tek agent instance SPOF oluşturuyor.  
**Etki:** Agent down = tüm trafik kesilir.  
**Çözüm:**
```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
┌──────▼──────┐
│  L4 LB /    │
│  DNS RR     │
└──────┬──────┘
       │
┌──────┴──────┬─────────────┐
▼             ▼             ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│ Agent-1 │ │ Agent-2 │ │ Agent-3 │
└─────────┘ └─────────┘ └─────────┘
```
**Görevler:**
- [ ] Agent stateless olduğunu doğrula (zaten öyle)
- [ ] K8s Deployment replica=3 örneği ekle
- [ ] Agent health endpoint ekle (`/health/live`, `/health/ready`)
- [ ] Dokümantasyonu güncelle

---

### 2.3 RDB/Snapshot Kararı
**Sorun:** `help.md` snapshot anlatıyor ama kod wired değil.  
**Karar gerekli:**
- [ ] **Opsiyon A:** RDB'yi tamamen kaldır (config + doc + kod)
- [ ] **Opsiyon B:** RDB'yi tam implement et (WAL/snapshot/recovery)

**Eğer kaldırılacaksa temizlenecekler:**
```
- app.rdb.* config
- AppProperties.Rdb interface
- data.rdb dosyaları
- help.md snapshot referansları
```

---

## 🟡 PHASE 3: Operasyonel İyileştirmeler (Hafta 4-5)

### 3.1 Tunable Consistency
**Amaç:** Farklı use-case'ler için consistency/latency trade-off.  
**Çözüm:**
```java
public enum ConsistencyLevel {
    ONE,      // Tek node yeterli (en hızlı)
    QUORUM,   // Çoğunluk gerekli (dengeli)
    ALL       // Tüm replica (en güvenli)
}

// Kullanım
clusterClient.set(key, value, ttl, ConsistencyLevel.QUORUM);
```

---

### 3.2 Anti-Entropy Aktivasyonu
**Sorun:** Kod var ama çağrılmıyor.  
**Çözüm:**
- [ ] `CoordinationService`'te periyodik digest karşılaştırma
- [ ] Uyuşmazlıkta repair tetikleme
- [ ] Metric ekleme (`anti_entropy_repairs_total`)

---

### 3.3 Metrics & Observability Genişletme
**Yeni metrikler:**
```
# Cluster
cluster_replication_lag_ms
cluster_quorum_failures_total
cluster_hint_queue_size

# Agent
agent_upstream_latency_ms
agent_selection_decisions_total{policy="RR|LEAST_CONN"}
```

---

### 3.4 CacheSegment.clear() Listener Bildirimi
**Sorun:** `clear()` çağrısında removal listener tetiklenmiyor.  
**Etki:** Metric/subscriber tutarsızlığı.  
**Çözüm:**
```java
void clear() {
    lock.lock();
    try {
        for (K key : map.keySet()) {
            policy.onRemove(key);
            notifyRemoval(key);  // ← Eksik olan bu
        }
        map.clear();
    } finally {
        lock.unlock();
    }
}
```

---

## 🟢 PHASE 4: Gelecek Özellikler (Backlog)

### 4.1 Smart Routing (Agent)
**Amaç:** Agent'ın consistent hash ile doğru node'a yönlendirmesi.  
**Fayda:** Gereksiz hop azaltma, latency iyileştirme.

### 4.2 Read Repair
**Amaç:** Read sırasında replica tutarsızlığı tespit ve düzeltme.  
**Fayda:** Eventual consistency güçlendirme.

### 4.3 Connection Pooling (Agent → Upstream)
**Amaç:** Her request için yeni TCP bağlantısı yerine pool.  
**Fayda:** Latency azaltma, kaynak verimliliği.

### 4.4 Binary Protocol Support
**Amaç:** Memcached binary protocol desteği.  
**Fayda:** Performans, bazı client uyumluluğu.

### 4.5 Persistence Layer (Opsiyonel)
**Eğer istenirse:**
- Write-Ahead Log (WAL)
- Periodic snapshot
- Startup recovery

---

## 📅 Zaman Çizelgesi

```
Hafta 1  │ PHASE 1: Kritik Düzeltmeler
         │ ├── 1.1 Read path fix
         │ ├── 1.2 RF=3 default
         │ └── 1.3 Touch response fix
         │
Hafta 2  │ PHASE 2: Veri Güvenilirliği (Başlangıç)
         │ ├── 2.1 Hinted handoff genişletme
         │ └── 2.3 RDB kararı + temizlik
         │
Hafta 3  │ PHASE 2: Veri Güvenilirliği (Devam)
         │ └── 2.2 Agent HA dokümantasyonu
         │
Hafta 4  │ PHASE 3: Operasyonel (Başlangıç)
         │ ├── 3.1 Tunable consistency
         │ └── 3.4 clear() listener fix
         │
Hafta 5  │ PHASE 3: Operasyonel (Devam)
         │ ├── 3.2 Anti-entropy aktivasyonu
         │ └── 3.3 Metrics genişletme
         │
Hafta 6+ │ PHASE 4: Backlog (önceliğe göre)
```

---

## ✅ Definition of Done

Her görev için:
- [ ] Kod değişikliği tamamlandı
- [ ] Unit test eklendi/güncellendi
- [ ] Integration test (varsa) güncellendi
- [ ] Dokümantasyon (`help.md`, `README.md`) güncellendi
- [ ] Code review yapıldı
- [ ] CI/CD başarılı

---

## 📝 Açık Sorular

1. **RDB kararı:** Tamamen kaldırılacak mı, yoksa implement edilecek mi?
2. **Consistency default:** Varsayılan `QUORUM` mu, `ONE` mi olsun?
3. **Agent HA:** K8s-only mi, bare-metal için de çözüm mü?
4. **Performans hedefi:** Latency/throughput SLA var mı?

---

<a id="en"></a>
## 🇬🇧 English

This roadmap contains prioritized tasks to transform `can-cache` into a production-ready distributed cache system.

### Priority Matrix

| Priority | Impact | Category |
|----------|--------|----------|
| 🔴 Critical | Data loss/outage risk | Immediate |
| 🟠 High | Reliability/consistency | Sprint 1 |
| 🟡 Medium | Operational improvement | Sprint 2 |
| 🟢 Low | Nice-to-have | Backlog |

### Summary

**PHASE 1 (Week 1):** Critical fixes - read path exception handling, RF=3 default, touch response fix  
**PHASE 2 (Week 2-3):** Data reliability - hinted handoff expansion, agent HA, RDB decision  
**PHASE 3 (Week 4-5):** Operational - tunable consistency, anti-entropy, metrics  
**PHASE 4 (Backlog):** Future features - smart routing, read repair, connection pooling, binary protocol

See Turkish section above for detailed implementation guidance.

