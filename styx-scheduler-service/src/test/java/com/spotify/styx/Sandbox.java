package com.spotify.styx;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.auth.ApiKeyAuth;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import java.io.IOException;
import org.junit.Test;

public class Sandbox {

  @Test
  public void testFoo() {
    try {
      // .withMasterUrl("https://" + cluster.getEndpoint())
      // .withCaCertData(cluster.getMasterAuth().getClusterCaCertificate())
      // .withClientCertData(cluster.getMasterAuth().getClientCertificate())
      // .withClientKeyData(cluster.getMasterAuth().getClientKey())
      // .withNamespace(config.getString(GKE_CLUSTER_NAMESPACE))
      final KubeConfig config = KubeConfig.loadDefaultKubeConfig();
      final String rsa = String.format("%s %s %s %s %s %s %s %s",
                                       config.getClientCertificateData(),
                                       config.getClientCertificateFile(),
                                       config.getClientKeyData(),
                                       config.getClientKeyFile(),
                                       "RSA", "",
                                       null, null);
      System.out.println("rsa = " + rsa);

      ApiClient defaultClient = Configuration.getDefaultApiClient();

      // Configure API key authorization: BearerToken
      ApiKeyAuth bearerToken = (ApiKeyAuth) defaultClient.getAuthentication("BearerToken");
      bearerToken.setApiKey("YOUR API KEY");
      // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
      //BearerToken.setApiKeyPrefix("Token");

      final ApiClient client = Config.defaultClient();
      // Configuration.setDefaultApiClient(client);
      final CoreV1Api api = new CoreV1Api(client);
      // final V1Pod pod = new V1Pod().spec(new V1PodSpec());
      api.listNode("true", null, null, null, null, null, null, null, null)
          .getItems()
          .forEach(node -> System.out.println("node = " + node));
      api.listNamespace("true", null, null, null, null, null, null, null, null)
          .getItems()
          .forEach(namespace -> System.out.println("namespace = " + namespace));
      //api.createNamespacedPod("default", pod, null);
    } catch (IOException | ApiException e) {
      throw new RuntimeException(e);
    }
  }
}
