package com.can.api;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
class CacheResourceTest {

    @Test
    void putThenGetEntry() {
        given()
                .contentType(ContentType.JSON)
                .body("{" +
                        "\"value\":\"external\"," +
                        "\"ttlSeconds\":5" +
                        "}")
                .when()
                .put("/cache/sample-key")
                .then()
                .statusCode(204);

        given()
                .when()
                .get("/cache/sample-key")
                .then()
                .statusCode(200)
                .body("key", equalTo("sample-key"))
                .body("value", equalTo("external"));
    }

    @Test
    void getMissingKeyReturns404() {
        given()
                .when()
                .get("/cache/missing-key")
                .then()
                .statusCode(404)
                .body("message", equalTo("Key not found"));
    }

    @Test
    void deleteRemovesEntry() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"value\":\"to-delete\"}")
                .when()
                .put("/cache/delete-key")
                .then()
                .statusCode(204);

        given()
                .when()
                .delete("/cache/delete-key")
                .then()
                .statusCode(204);

        given()
                .when()
                .get("/cache/delete-key")
                .then()
                .statusCode(404);
    }

    @Test
    void putWithoutValueFails() {
        given()
                .contentType(ContentType.JSON)
                .body("{}")
                .when()
                .put("/cache/no-value")
                .then()
                .statusCode(400)
                .body("message", equalTo("value must be provided"));
    }
}
