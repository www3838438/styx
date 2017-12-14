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

import static com.spotify.styx.docker.KubernetesDockerRunner.STYX_RUN;
import static com.spotify.styx.docker.KubernetesPodEventTranslator.translate;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.testdata.TestData;
import io.kubernetes.client.models.V1ContainerState;
import io.kubernetes.client.models.V1ContainerStateRunning;
import io.kubernetes.client.models.V1ContainerStateTerminated;
import io.kubernetes.client.models.V1ContainerStateWaiting;
import io.kubernetes.client.models.V1ContainerStatus;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodStatus;
import java.util.List;
import java.util.Optional;
import org.joda.time.DateTime;
import org.junit.Test;

public class KubernetesPodEventTranslatorTest {

  private static final WorkflowInstance WFI =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final DockerRunner.RunSpec RUN_SPEC =
      DockerRunner.RunSpec.simple("eid", "busybox");
  private static final String MESSAGE_FORMAT = "{\"rfu\":{\"dum\":\"my\"},\"component_id\":\"dummy\",\"workflow_id\":\"dummy\",\"parameter\":\"dummy\",\"execution_id\":\"dummy\",\"event\":\"dummy\",\"exit_code\":%d}\n";
  private static final KubernetesSecretSpec SECRET_SPEC = KubernetesSecretSpec.builder().build();

  V1Pod pod = KubernetesDockerRunner.createPod(WFI, RUN_SPEC, SECRET_SPEC);

  @Test
  public void terminateOnSuccessfulTermination() throws Exception {
    pod.setStatus(terminated("Succeeded", 20, null));

    assertGeneratesEventsAndTransitions(
        RunState.State.RUNNING, pod,
        Event.terminate(WFI, Optional.of(20)));
  }

  @Test
  public void startedAndTerminatedOnFromSubmitted() throws Exception {
    pod.setStatus(terminated("Succeeded", 0, null));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(0)));
  }

  @Test
  public void shouldNotGenerateStartedWhenContainerIsNotReady() throws Exception {
    pod.setStatus(running(/* ready= */ false));

    assertGeneratesNoEvents(
        RunState.State.SUBMITTED, pod);
  }

  @Test
  public void shouldGenerateStartedWhenContainerIsReady() throws Exception {
    pod.setStatus(running(/* ready= */ true));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI));
  }

  @Test
  public void runErrorOnErrImagePull() throws Exception {
    pod.setStatus(waiting("Pending", "ErrImagePull"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "One or more containers failed to pull their image"));
  }

  @Test
  public void runErrorOnUnknownPhaseEntered() throws Exception {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Pod entered Unknown phase"));
  }

  @Test
  public void runErrorOnMissingContainer() throws Exception {
    pod.setStatus(podStatusNoContainer("Succeeded"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Could not find our container in pod"));
  }

  @Test
  public void runErrorOnUnexpectedTerminatedStatus() throws Exception {
    pod.setStatus(waiting("Failed", ""));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Unexpected null terminated status"));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButNoMessage() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Succeeded", 0, null));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButInvalidJson() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Succeeded", 0, "SUCCESS"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButPartialJson() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Succeeded", 0, "{\"workflow_id\":\"dummy\"}"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButK8sFallback() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Failed", 17, "{\"workflow_id\":\"dummy\"}"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(17)));
  }

  @Test
  public void errorContainerExitCodeAndUnparsableTerminationLog() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Failed", 17, "{\"workf"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(17)));
  }

  @Test
  public void zeroContainerExitCodeAndInvalidTerminationLog() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Failed", 0, "{\"workflow_id\":\"dummy\"}"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void zeroContainerExitCodeAndUnparsableTerminationLog() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Failed", 0, "{\"workflo"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void exitCodeFromMessageOnTerminationLoggingAndZeroExitCode() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Succeeded", 0, String.format(MESSAGE_FORMAT, 1)));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(1)));
  }

  @Test
  public void noExitCodeFromEitherMessageOnTerminationLoggingNorDocker() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Succeeded", null, null));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void exitCodeFromMessageOnTerminationLoggingAndNonzeroExitCode() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Failed", 2, String.format(MESSAGE_FORMAT, 3)));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(3)));
  }

  @Test
  public void zeroExitCodeFromTerminationLogAndNonZeroContainerExitCode() throws Exception {
    V1Pod pod = podWithTerminationLogging();
    pod.setStatus(terminated("Failed", 2, String.format(MESSAGE_FORMAT, 0)));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(2)));
  }

  @Test
  public void noEventsWhenStateInTerminated() throws Exception {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesNoEvents(RunState.State.TERMINATED, pod);
  }

  @Test
  public void noEventsWhenStateInFailed() throws Exception {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesNoEvents(RunState.State.FAILED, pod);
  }

  @Test
  public void shouldIgnoreDeletedEvents() throws Exception {
    pod.setStatus(terminated("Succeeded", 0, null));
    RunState state = RunState.create(WFI, RunState.State.TERMINATED);

    List<Event> events = translate(WFI, state, "DELETE", pod, Stats.NOOP);
    assertThat(events, empty());
  }

  private void assertGeneratesEventsAndTransitions(
      RunState.State initialState,
      V1Pod pod,
      Event... expectedEvents) {

    RunState state = RunState.create(WFI, initialState);
    List<Event> events = translate(WFI, state, "MODIFIED", pod, Stats.NOOP);
    assertThat(events, contains(expectedEvents));

    // ensure no exceptions are thrown when transitioning
    for (Event event : events) {
      state = state.transition(event);
    }
  }

  private void assertGeneratesNoEvents(
      RunState.State initialState,
      V1Pod pod) {

    RunState state = RunState.create(WFI, initialState);
    List<Event> events = translate(WFI, state, "MODIFIED", pod, Stats.NOOP);

    assertThat(events, empty());
  }

  static V1PodStatus running(boolean ready) {
    return podStatus("Running", ready, new V1ContainerState().running(
        new V1ContainerStateRunning().startedAt(DateTime.parse("2016-05-30T09:46:48Z"))));
  }

  static V1PodStatus terminated(String phase, Integer exitCode, String message) {
    return podStatus(phase, true, new V1ContainerState().terminated(
        new V1ContainerStateTerminated().exitCode(exitCode).message(message).signal(0)
    ));
  }

  static V1PodStatus waiting(String phase, String reason) {
    return podStatus(phase, true, new V1ContainerState().waiting(new V1ContainerStateWaiting().reason(reason)));
  }

  static V1PodStatus podStatus(String phase, boolean ready, V1ContainerState containerState) {
    V1PodStatus podStatus = podStatusNoContainer(phase);
    podStatus.getContainerStatuses()
        .add(new V1ContainerStatus().containerID(STYX_RUN).name(STYX_RUN).state(containerState)
             .ready(ready).restartCount(0).lastState(containerState));
    return podStatus;
  }

  static V1PodStatus podStatusNoContainer(String phase) {
    return new V1PodStatus().phase(phase);
  }

  private V1Pod podWithTerminationLogging() {
    return KubernetesDockerRunner.createPod(
        WFI,
        DockerRunner.RunSpec.builder()
            .executionId("eid")
            .imageName("busybox")
            .terminationLogging(true).build(),
        SECRET_SPEC);
  }
}
