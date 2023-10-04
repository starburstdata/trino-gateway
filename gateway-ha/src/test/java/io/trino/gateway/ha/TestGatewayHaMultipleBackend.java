package io.trino.gateway.ha;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.base.Strings;
import java.io.IOException;
import okhttp3.Cookie;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestGatewayHaMultipleBackend {
  public static final String EXPECTED_RESPONSE1 = "{\"id\":\"testId1\"}";
  public static final String EXPECTED_RESPONSE2 = "{\"id\":\"testId2\"}";
  public static final String CUSTOM_RESPONSE = "123";
  public static final String CUSTOM_PATH = "/v1/custom/extra";

  public static String OAUTH_INITIATE_PATH = "/oauth2";
  public static String OAUTH_CALLBACK_PATH = OAUTH_INITIATE_PATH + "/callback";
  public static String OAUTH_INITIAL_RESPONSE = "TODO"; //TODO
  public static String OAUTH_CALLBACK_RESPONSE = "TODO"; //TODO


  final int routerPort = 20000 + (int) (Math.random() * 1000);
  final int backend1Port = 21000 + (int) (Math.random() * 1000);
  final int backend2Port = 21000 + (int) (Math.random() * 1000);
  final int customBackendPort = 21000 + (int) (Math.random() * 1000);

  private final WireMockServer adhocBackend =
      new WireMockServer(WireMockConfiguration.options().port(backend1Port));
  private final WireMockServer scheduledBackend =
      new WireMockServer(WireMockConfiguration.options().port(backend2Port));

  private final WireMockServer customBackend =
          new WireMockServer(WireMockConfiguration.options().port(customBackendPort));

  @BeforeClass(alwaysRun = true)
  public void setup() throws Exception {
    HaGatewayTestUtils.prepareMockBackend(adhocBackend, "/v1/statement", EXPECTED_RESPONSE1);
    HaGatewayTestUtils.prepareMockBackend(scheduledBackend, "/v1/statement", EXPECTED_RESPONSE2);
    HaGatewayTestUtils.prepareMockBackend(customBackend, CUSTOM_PATH, CUSTOM_RESPONSE);

    // seed database
    HaGatewayTestUtils.TestConfig testConfig =
        HaGatewayTestUtils.buildGatewayConfigAndSeedDb(routerPort, "test-config-template.yml");

    // Start Gateway
    String[] args = {"server", testConfig.getConfigFilePath()};
    HaGatewayLauncher.main(args);
    // Now populate the backend
    HaGatewayTestUtils.setUpBackend(
        "trino1", "http://localhost:" + backend1Port, "externalUrl", true, "adhoc", routerPort);
    HaGatewayTestUtils.setUpBackend(
        "trino2", "http://localhost:" + backend2Port, "externalUrl", true, "scheduled",
        routerPort);
    HaGatewayTestUtils.setUpBackend(
            "custom", "http://localhost:" + customBackendPort, "externalUrl", true, "custom",
            routerPort);

  }

  @Test
  public void testCustomPath() throws Exception {
    OkHttpClient httpClient = new OkHttpClient();
    RequestBody requestBody =
            RequestBody.create(MediaType.parse("application/json; charset=utf-8"), "abc");
    Request request1 =
            new Request.Builder()
                    .url("http://localhost:" + routerPort + CUSTOM_PATH)
                    .post(requestBody)
                    .addHeader("X-Trino-Routing-Group", "custom")
                    .build();
    Response response1 = httpClient.newCall(request1).execute();
    Assert.assertEquals(response1.body().string(), CUSTOM_RESPONSE);

    Request request2 =
            new Request.Builder()
                    .url("http://localhost:" + routerPort + "/invalid")
                    .post(requestBody)
                    .addHeader("X-Trino-Routing-Group", "custom")
                    .build();
    Response response2 = httpClient.newCall(request2).execute();
    Assert.assertEquals(response2.code(), 404);

    HaGatewayTestUtils.setUpBackend(
            "trino_oauth1", "http://localhost:" + backend1Port, "externalUrl", true, "adhoc", routerPort);
    HaGatewayTestUtils.setUpBackend(
            "trino_oauth2", "http://localhost:" + backend2Port, "externalUrl", true, "scheduled",
            routerPort);

  }

  @Test
  public void testQueryDeliveryToMultipleRoutingGroups() throws Exception {
    // Default request should be routed to adhoc backend
    OkHttpClient httpClient = new OkHttpClient();
    RequestBody requestBody =
        RequestBody.create(MediaType.parse("application/json; charset=utf-8"), "SELECT 1");
    Request request1 =
        new Request.Builder()
            .url("http://localhost:" + routerPort + "/v1/statement")
            .post(requestBody)
            .build();
    Response response1 = httpClient.newCall(request1).execute();
    Assert.assertEquals(response1.body().string(), EXPECTED_RESPONSE1);

    // When X-Trino-Routing-Group is set in header, query should be routed to cluster under the
    // routing group
    Request request4 =
        new Request.Builder()
            .url("http://localhost:" + routerPort + "/v1/statement")
            .post(requestBody)
            .addHeader("X-Trino-Routing-Group", "scheduled")
            .build();
    Response response4 = httpClient.newCall(request4).execute();
    Assert.assertEquals(response4.body().string(), EXPECTED_RESPONSE2);
  }

  @Test
  public void testCookieBasedRouting()
          throws IOException {
    // This simulates the Trino oauth handshake
    HaGatewayTestUtils.addEndpoint(scheduledBackend, OAUTH_INITIATE_PATH, OAUTH_INITIAL_RESPONSE);
    HaGatewayTestUtils.addEndpoint(scheduledBackend, OAUTH_CALLBACK_PATH, OAUTH_CALLBACK_RESPONSE);


    OkHttpClient httpClient = new OkHttpClient(); /*.newBuilder()
            .cookieJar(new UiApiCookieJar()) //borrowing for convenience
            .build();;*/
    String oauthInitiateBody = "todo";
    RequestBody requestBody =
            RequestBody.create(
                    MediaType.parse("application/json; charset=utf-8"), oauthInitiateBody);
    // Scheduled is the non-default RG. We send our request there to ensure
    // the follow-up request will only be routed correctly if the cookie routing logic is used
    Request initiateRequest =
            new Request.Builder()
                    .url("http://localhost:" + routerPort + OAUTH_INITIATE_PATH)
                    .post(requestBody)
                    .addHeader("X-Trino-Routing-Group", "scheduled")
                    .build();
    Response initiateResponse = httpClient.newCall(initiateRequest).execute();
    Assert.assertTrue(!Strings.isNullOrEmpty(initiateResponse.header("set-cookie")));

    Request callbackRequest =
            new Request.Builder()
                    .url("http://localhost:" + routerPort + OAUTH_CALLBACK_PATH)
                    .post(requestBody)
                    .addHeader("Cookie", initiateResponse.header("set-cookie"))
                    .build();
    Response callbackResponse = httpClient.newCall(callbackRequest).execute();
    Assert.assertEquals(callbackResponse.body().string(), OAUTH_CALLBACK_RESPONSE);
  }

  public static Cookie createNonPersistentCookie() {
    return new Cookie.Builder()
            .domain("publicobject.com")
            .path("/")
            .name("cookie-name")
            .value("cookie-value")
            .httpOnly()
            .secure()
            .build();
  }

  @AfterClass(alwaysRun = true)
  public void cleanup() {
    adhocBackend.stop();
    scheduledBackend.stop();
  }
}
