/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 Spotify AB
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

import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.testdata.TestData;

public class KubernetesDockerRunnerPodResourceTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04");

  private final static KubernetesSecretSpec EMPTY_SECRET_SPEC = KubernetesSecretSpec.builder().build();

  private static final String TEST_EXECUTION_ID = "execution_1";

  // @Test
  // public void shouldAddLatestTag() throws Exception {
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);
  //
  //   List<Container> containers = pod.getSpec().getContainers();
  //   assertThat(containers.size(), is(1));
  //
  //   Container container = containers.get(0);
  //   assertThat(container.getImage(), is("busybox:latest"));
  // }
  //
  // @Test
  // public void shouldUseConfiguredTag() throws Exception {
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.simple("eid", "busybox:v7"), EMPTY_SECRET_SPEC);
  //
  //   List<Container> containers = pod.getSpec().getContainers();
  //   assertThat(containers.size(), is(1));
  //
  //   Container container = containers.get(0);
  //   assertThat(container.getImage(), is("busybox:v7"));
  // }
  //
  // @Test
  // public void shouldAddArgs() throws Exception {
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.simple("eid", "busybox", "echo", "foo", "bar"), EMPTY_SECRET_SPEC);
  //
  //   List<Container> containers = pod.getSpec().getContainers();
  //   assertThat(containers.size(), is(1));
  //
  //   Container container = containers.get(0);
  //   assertThat(container.getArgs(), contains("echo", "foo", "bar"));
  // }
  //
  // @Test
  // public void shouldAddWorkflowInstanceAnnotation() throws Exception {
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);
  //
  //   Map<String, String> annotations = pod.getMetadata().getAnnotations();
  //   assertThat(annotations, hasEntry(STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey()));
  //
  //   WorkflowInstance workflowInstance =
  //       WorkflowInstance.parseKey(annotations.get(STYX_WORKFLOW_INSTANCE_ANNOTATION));
  //   assertThat(workflowInstance, is(WORKFLOW_INSTANCE));
  // }
  //
  // @Test
  // public void shouldDisableTerminationLoggingWhenFalse() throws Exception {
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);
  //
  //   Map<String, String> annotations = pod.getMetadata().getAnnotations();
  //   assertThat(annotations.get(DOCKER_TERMINATION_LOGGING_ANNOTATION), is("false"));
  //
  //   List<Container> containers = pod.getSpec().getContainers();
  //   Optional<EnvVar> terminationLogVar = containers.get(0).getEnv().stream()
  //       .filter(e -> TERMINATION_LOG.equals(e.getName())).findAny();
  //   assertThat(terminationLogVar.get().getValue(), is("/dev/termination-log"));
  // }
  //
  // @Test
  // public void shouldEnableTerminationLoggingWhenTrue() throws Exception {
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.builder()
  //           .executionId("eid")
  //           .imageName("busybox")
  //           .terminationLogging(true)
  //           .build(),
  //       EMPTY_SECRET_SPEC);
  //
  //   Map<String, String> annotations = pod.getMetadata().getAnnotations();
  //   assertThat(annotations.get(DOCKER_TERMINATION_LOGGING_ANNOTATION), is("true"));
  //
  //   List<Container> containers = pod.getSpec().getContainers();
  //   Optional<EnvVar> terminationLogVar = containers.get(0).getEnv().stream()
  //       .filter(e -> TERMINATION_LOG.equals(e.getName())).findAny();
  //   assertThat(terminationLogVar.get().getValue(), is("/dev/termination-log"));
  // }
  //
  // @Test
  // public void shouldHaveRestartPolicyNever() throws Exception {
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);
  //
  //   assertThat(pod.getSpec().getRestartPolicy(), is("Never"));
  // }
  //
  // @Test
  // public void shouldNotHaveSecretsMountIfNoSecret() throws Exception {
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);
  //
  //   List<Volume> volumes = pod.getSpec().getVolumes();
  //   List<Container> containers = pod.getSpec().getContainers();
  //   assertThat(volumes.size(), is(0));
  //   assertThat(containers.size(), is(1));
  //
  //   Container container = containers.get(0);
  //   List<VolumeMount> volumeMounts = container.getVolumeMounts();
  //   assertThat(volumeMounts.size(), is(0));
  // }
  //
  // @Test
  // public void shouldConfigureSecretsMount() throws Exception {
  //   WorkflowConfiguration.Secret secret = WorkflowConfiguration.Secret.create("my-secret", "/etc/secrets");
  //   KubernetesSecretSpec secretSpec = KubernetesSecretSpec.builder()
  //       .customSecret(secret)
  //       .build();
  //   Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.builder()
  //           .executionId("eid")
  //           .imageName("busybox")
  //           .secret(secret)
  //           .build(),
  //       secretSpec);
  //
  //   List<Volume> volumes = pod.getSpec().getVolumes();
  //   List<Container> containers = pod.getSpec().getContainers();
  //   assertThat(volumes.size(), is(1));
  //   assertThat(containers.size(), is(1));
  //
  //   Volume volume = volumes.get(0);
  //   assertThat(volume.getName(), is("my-secret"));
  //   assertThat(volume.getSecret().getSecretName(), is("my-secret"));
  //
  //   Container container = containers.get(0);
  //   List<VolumeMount> volumeMounts = container.getVolumeMounts();
  //   assertThat(volumeMounts.size(), is(1));
  //
  //   VolumeMount volumeMount = volumeMounts.get(0);
  //   assertThat(volumeMount.getName(), is("my-secret"));
  //   assertThat(volumeMount.getMountPath(), is("/etc/secrets"));
  //   assertThat(volumeMount.getReadOnly(), is(true));
  // }
  //
  // @Test
  // public void shouldConfigureEnvironmentVariables() throws Exception {
  //   final Pod pod = KubernetesDockerRunner.createPod(
  //       WORKFLOW_INSTANCE,
  //       DockerRunner.RunSpec.builder()
  //           .executionId(TEST_EXECUTION_ID)
  //           .imageName("busybox")
  //           .trigger(Trigger.unknown("trigger-id"))
  //           .commitSha("abc123")
  //           .build(),
  //       EMPTY_SECRET_SPEC);
  //
  //   final List<EnvVar> envVars = pod.getSpec().getContainers().get(0).getEnv();
  //
  //   assertThat(envVars, hasItem(envVar(COMPONENT_ID, WORKFLOW_INSTANCE.workflowId().componentId())));
  //   assertThat(envVars, hasItem(envVar(WORKFLOW_ID, WORKFLOW_INSTANCE.workflowId().id())));
  //   assertThat(envVars, hasItem(envVar(PARAMETER, WORKFLOW_INSTANCE.parameter())));
  //   assertThat(envVars, hasItem(envVar(SERVICE_ACCOUNT, "")));
  //   assertThat(envVars, hasItem(envVar(DOCKER_IMAGE, "busybox")));
  //   assertThat(envVars, hasItem(envVar(TRIGGER_ID, "trigger-id")));
  //   assertThat(envVars, hasItem(envVar(TRIGGER_TYPE, "unknown")));
  //   assertThat(envVars, hasItem(envVar(COMMIT_SHA, "abc123")));
  //   assertThat(envVars, hasItem(envVar(EXECUTION_ID, TEST_EXECUTION_ID)));
  // }
}
