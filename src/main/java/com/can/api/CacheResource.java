package com.can.api;

import com.can.cluster.ClusterClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.Duration;
import java.util.Objects;

/**
 * Dış istemcilerin HTTP üzerinden küme önbelleğini okuması ve güncellemesi için
 * sağlanan REST kaynağı. Bu katman ClusterClient bean'ini kullanarak küme
 * içerisindeki tüm replikalara istekleri yönlendirir.
 */
@Path("/cache")
@ApplicationScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class CacheResource {

    private final ClusterClient<String, String> clusterClient;

    @Inject
    public CacheResource(ClusterClient<String, String> clusterClient) {
        this.clusterClient = clusterClient;
    }

    @GET
    @Path("{key}")
    public Response get(@PathParam("key") String key) {
        String value = clusterClient.get(key);
        if (value == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Key not found"))
                    .build();
        }
        return Response.ok(new CacheEntry(key, value)).build();
    }

    @PUT
    @Path("{key}")
    public Response put(@PathParam("key") String key, CacheWriteRequest request) {
        if (request == null || request.value() == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("value must be provided"))
                    .build();
        }

        clusterClient.set(key, request.value(), request.ttlDuration());
        return Response.noContent().build();
    }

    @DELETE
    @Path("{key}")
    public Response delete(@PathParam("key") String key) {
        boolean removed = clusterClient.delete(key);
        if (!removed) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Key not found"))
                    .build();
        }
        return Response.noContent().build();
    }

    public record CacheEntry(String key, String value) {}

    public record ErrorResponse(String message) {}

    public static final class CacheWriteRequest {
        private String value;
        private Long ttlSeconds;

        public CacheWriteRequest() {
        }

        public CacheWriteRequest(String value, Long ttlSeconds) {
            this.value = value;
            this.ttlSeconds = ttlSeconds;
        }

        public String value() {
            return value;
        }

        public Long ttlSeconds() {
            return ttlSeconds;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public void setTtlSeconds(Long ttlSeconds) {
            this.ttlSeconds = ttlSeconds;
        }

        Duration ttlDuration() {
            if (ttlSeconds == null) {
                return null;
            }
            long secs = ttlSeconds;
            if (secs <= 0) {
                return null;
            }
            return Duration.ofSeconds(secs);
        }

        @Override
        public String toString() {
            return "CacheWriteRequest{" +
                    "value='" + value + '\'' +
                    ", ttlSeconds=" + ttlSeconds +
                    '}';
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, ttlSeconds);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheWriteRequest that = (CacheWriteRequest) o;
            return Objects.equals(value, that.value) && Objects.equals(ttlSeconds, that.ttlSeconds);
        }
    }
}
