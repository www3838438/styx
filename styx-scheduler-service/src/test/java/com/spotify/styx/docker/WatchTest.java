/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx.docker;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.container.Container;
import com.google.api.services.container.ContainerScopes;
import com.google.api.services.container.model.Cluster;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Ignore;
import org.junit.Test;

public class WatchTest {

  private static Container createGkeClient() {
    try {
      final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
      final GoogleCredential credential =
          GoogleCredential.getApplicationDefault(httpTransport, jsonFactory)
              .createScoped(ContainerScopes.all());
      return new Container.Builder(httpTransport, jsonFactory, credential)
          .setApplicationName("dano-test")
          .build();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void foo() throws Exception {
    KubernetesClient client = createClient();

//    PodList list = client.pods().list();

//    System.err.println("list resourceVersion: " + list.getMetadata().getResourceVersion());

    startWatch(client, "a");
//    startWatch(client, "b");

    while (true) {
      Thread.sleep(1000);
    }
  }

  void startWatch(KubernetesClient client, String name) {
    System.err.println("thread=" + Thread.currentThread().getId() + ": " + name + ": start");
    final CompletableFuture<Watch> watch = new CompletableFuture<>();

    PodList list = client.pods()
        .withLabel("foobar")
        .list();

    System.err.println("foobar: " +  list.getMetadata().getResourceVersion());

    watch.complete(client.pods()
//        .withResourceVersion("0")
//        .withResourceVersion(list.getMetadata().getResourceVersion())
        .watch(new Watcher<Pod>() {
          volatile int i = 0;

          @Override
          public void eventReceived(Action action, Pod resource) {
            i++;
            System.err.println("thread=" + Thread.currentThread().getId() + ": " + i + ": " + name + ": " +
                resource.getMetadata().getResourceVersion() + ": " + action + " " + resource);
//            try {
//              watch.get().close();
//            } catch (InterruptedException | ExecutionException e) {
//              throw new RuntimeException(e);
//            }
          }

          @Override
          public void onClose(KubernetesClientException cause) {
            System.err.println(name + ": " + "close: " + cause);
          }
        }));
  }

  private KubernetesClient createClient() throws IOException {

    Container gke = createGkeClient();

    final Cluster cluster = gke.projects().zones().clusters()
        .get("steel-ridge-91615",
            "europe-west1-d",
//            "styx-production"
            "styx-staging"
        ).execute();

    final io.fabric8.kubernetes.client.Config kubeConfig = new ConfigBuilder()
        .withMasterUrl("https://" + cluster.getEndpoint())
        .withCaCertData(cluster.getMasterAuth().getClusterCaCertificate())
        .withClientCertData(cluster.getMasterAuth().getClientCertificate())
        .withClientKeyData(cluster.getMasterAuth().getClientKey())
        .withNamespace("default")
        .build();

    return new DefaultKubernetesClient(kubeConfig);
  }
}
