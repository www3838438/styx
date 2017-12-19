package com.spotify.styx;

import com.google.common.reflect.TypeToken;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.authenticators.GCPAuthenticator;
import java.io.IOException;
import java.util.Arrays;
import org.junit.Test;

public class Sandbox {

  @Test
  public void testFoo() {
    try {
KubeConfig.registerAuthenticator(new GCPAuthenticator());

final CoreV1Api api = new CoreV1Api(Config.defaultClient());

final int numberOfPods =
    api.listNamespacedPod("default", null, null, null, null, null, null, null, null, null)
        .getItems()
        .size();

System.out.println("numberOfPods = " + numberOfPods);

      final Thread watcherThread = new Thread(
          () -> {
            System.out.println("hello from watcher thread");
            final Watch<V1Pod> watch;
            try {
              watch = Watch.createWatch(api.getApiClient(),
                                        api.listNamespacedPodCall("default", null,
                                                                  null, null, null,
                                                                  null, null, null,
                                                                  null, true, null,
                                                                  null),
                                        new TypeToken<Watch.Response<V1Pod>>() {
                                                           }.getType());
            } catch (ApiException e) {
              throw new RuntimeException(e);
            }
            for (Watch.Response<V1Pod> item : watch) {
              // item.object.getSpec().getContainers().get(0).getArgs();
              System.out.printf("%-10s %s %s\n", item.type, item.object.getMetadata().getName(), item.object.getStatus().getPhase());
            }
          });

      watcherThread.start();

      watcherThread.join();

    } catch (IOException | ApiException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void createPod(CoreV1Api api) throws ApiException {
    final V1Pod pod = new V1Pod()
        .apiVersion("v1")
        .kind("Pod")
        .spec(new V1PodSpec()
                  .restartPolicy("Never")
                  .containers(Arrays.asList(new V1Container()
                                                .name("jockecontainer")
                                                .command(Arrays.asList("echo"))
                                                .args(Arrays.asList("hello", "world"))
                                                .image("busybox:latest"))))
        .metadata(new V1ObjectMeta()
                      .clusterName("styx-regional")
                      .namespace("default")
                      .name("jocketest" + System.currentTimeMillis()));

    System.out.println("podToBeCreated = " + pod);

    final V1Pod createdPod = api.createNamespacedPod("default", pod, null);

    System.out.println("createdPod = " + createdPod);
  }
}
