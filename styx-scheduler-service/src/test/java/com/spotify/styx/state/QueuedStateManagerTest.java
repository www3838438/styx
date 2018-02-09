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

package com.spotify.styx.state;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.spotify.styx.RepeatRule;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionException;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueuedStateManagerTest {

  private static final Instant NOW = Instant.parse("2017-01-02T01:02:03Z");
  private static final WorkflowInstance INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "2016-05-01");
  private static final RunState INSTANCE_NEW_STATE =
      RunState.create(INSTANCE, State.NEW, StateData.zero(), NOW);
  private static final Workflow WORKFLOW =
      Workflow.create("foo", TestData.FULL_WORKFLOW_CONFIGURATION);
  private static final Trigger TRIGGER1 = Trigger.unknown("trig1");
  private static final BiConsumer<SequenceEvent, RunState> eventConsumer = (e, s) -> {};

  private final ExecutorService eventTransitionExecutor = Executors.newFixedThreadPool(16);
  private final ExecutorService eventConsumerExecutor = Executors.newSingleThreadExecutor();


  private QueuedStateManager stateManager;

  @Captor ArgumentCaptor<RunState> runStateCaptor;

  @Rule public RepeatRule repeatRule = new RepeatRule();

  @Mock Storage storage;
  @Mock StorageTransaction transaction;
  @Mock OutputHandler outputHandler;
  @Mock Time time;

  @Before
  public void setUp() throws Exception {
    when(time.get()).thenReturn(NOW);
    when(storage.runInTransaction(any())).thenAnswer(
        a -> a.getArgumentAt(0, TransactionFunction.class).apply(transaction));
    doNothing().when(outputHandler).transitionInto(runStateCaptor.capture());
    stateManager = new QueuedStateManager(
        time, eventTransitionExecutor, storage, eventConsumer,
        eventConsumerExecutor, OutputHandler.fanOutput(outputHandler));
  }

  @After
  public void tearDown() throws Exception {
    if (stateManager != null) {
      stateManager.close();
    }
  }

  @Test
  public void shouldInitializeAndTriggerWFInstance() throws Exception {
    final RunState instanceStateFresh =
        RunState.create(INSTANCE, State.NEW, StateData.zero(), NOW, -1);
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(instanceStateFresh));

    stateManager.trigger(INSTANCE, TRIGGER1)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).writeActiveState(INSTANCE, instanceStateFresh);
    verify(storage).writeEvent(SequenceEvent.create(
        Event.triggerExecution(INSTANCE, TRIGGER1), 0, NOW.toEpochMilli()));
  }

  @Test
  public void shouldReInitializeWFInstanceFromNextCounter() throws Exception {
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE.counter()));
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE));

    stateManager.trigger(INSTANCE, TRIGGER1)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).writeActiveState(INSTANCE, INSTANCE_NEW_STATE);
    verify(storage).writeEvent(SequenceEvent.create(
        Event.triggerExecution(INSTANCE, TRIGGER1), INSTANCE_NEW_STATE.counter() + 1, NOW.toEpochMilli()));
  }

  @Test
  public void shouldNotBeActiveAfterHalt() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(
        Optional.of(RunState.create(INSTANCE, State.PREPARE, StateData.zero(),
            NOW, 17)));

    Event event = Event.halt(INSTANCE);
    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).deleteActiveState(INSTANCE);
    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldFailTriggerWFIfAlreadyActive() throws Exception {
    reset(storage);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    final Exception rootCause = new IllegalStateException("Already exists!");
    final TransactionException transactionException = spy(new TransactionException(false, rootCause));
    when(transactionException.isAlreadyExists()).thenReturn(true);
    when(storage.runInTransaction(any())).thenAnswer(a -> {
      a.getArgumentAt(0, TransactionFunction.class)
          .apply(transaction);
      throw transactionException;
    });

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IllegalStateException.class)));
    }
  }

  @Test
  public void shouldFailTriggerWFIfOnConflict() throws Exception {
    reset(storage);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    final TransactionException transactionException = spy(new TransactionException(true, null));
    when(storage.runInTransaction(any())).thenAnswer(a -> {
      a.getArgumentAt(0, TransactionFunction.class)
          .apply(transaction);
      throw transactionException;
    });

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(TransactionException.class)));
      TransactionException cause = (TransactionException) Throwables.getRootCause(e);
      assertTrue(cause.isConflict());
    }
  }

  @Test
  public void shouldFailTriggerWFIfOnConflict9() throws Exception {
    reset(storage);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));

    final Exception rootCause = new FooException();
    final TransactionException transactionException = spy(new TransactionException(false, rootCause));
    when(transactionException.isAlreadyExists()).thenReturn(false);
    when(storage.runInTransaction(any())).thenAnswer(a -> {
      a.getArgumentAt(0, TransactionFunction.class)
          .apply(transaction);
      throw transactionException;
    });
    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(FooException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfGetLatestCounterFails() throws Exception {
    when(storage.getLatestStoredCounter(any())).thenThrow(new IOException());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IOException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfWorkflowNotFound() throws Exception {
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IllegalArgumentException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfIOExceptionFromTransaction() throws Exception {
    reset(storage);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());
    when(storage.runInTransaction(any())).thenThrow(new IOException());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IOException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfIsClosedOnReceive() throws Exception {
    reset(storage);
    stateManager = spy(new QueuedStateManager(
        time, eventTransitionExecutor, storage, eventConsumer,
        eventConsumerExecutor, outputHandler));
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());
    when(stateManager.receive(any())).thenThrow(new IsClosedException());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IsClosedException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfIsClosedOnReceiveAndFailDeleteActiveState() throws Exception {
    reset(storage);
    stateManager = spy(new QueuedStateManager(
        time, eventTransitionExecutor, storage, eventConsumer,
        eventConsumerExecutor, outputHandler));
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    doThrow(new IOException()).when(storage).deleteActiveState(any());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());
    when(stateManager.receive(any())).thenThrow(new IsClosedException());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IsClosedException.class)));
    }
  }

  @Test(expected = IsClosedException.class)
  public void shouldRejectEventIfClosed() throws Exception {
    stateManager.close();
    stateManager.receive(Event.timeTrigger(INSTANCE));
  }

  @Test
  public void shouldCloseGracefully() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(
        Optional.of(RunState.create(INSTANCE, State.QUEUED, StateData.zero(),
            NOW.minusMillis(1), 17)));

    CompletableFuture<Void> barrier = new CompletableFuture<>();

    reset(storage);

    when(storage.runInTransaction(any())).thenAnswer(a -> {
      barrier.get();
      return a.getArgumentAt(0, TransactionFunction.class).apply(transaction);
    });

    CompletableFuture<Void> f = stateManager.receive(Event.dequeue(INSTANCE))
        .toCompletableFuture();

    CompletableFuture.runAsync(() -> {
      try {
        stateManager.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    verify(storage, timeout(30_000)).runInTransaction(any());

    barrier.complete(null);

    f.get(1, MINUTES);

    verify(outputHandler).transitionInto(any());
    assertThat(runStateCaptor.getValue().state(), is(State.PREPARE));
  }

  @Test
  public void shouldWriteEvents() throws Exception {
    Event event = Event.started(INSTANCE);

    when(transaction.readActiveState(INSTANCE))
        .then(a -> Optional.of(RunState.create(INSTANCE, State.SUBMITTED, StateData.zero(),
            NOW, 17)));

    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldRemoveStateIfTerminal() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(
        Optional.of(RunState.create(INSTANCE, State.TERMINATED, StateData.zero(),
            NOW, 17)));

    Event event = Event.success(INSTANCE);
    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).deleteActiveState(INSTANCE);
    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }


  @Test
  public void shouldWriteActiveStateOnEvent() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(RunState.create(INSTANCE,
        State.QUEUED, StateData.zero(), NOW, 17)));

    stateManager.receive(Event.dequeue(INSTANCE))
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).updateActiveState(INSTANCE, RunState.create(INSTANCE, State.PREPARE,
        StateData.zero(), NOW, 18));
  }

  @Test
  public void shouldPreventIllegalStateTransition() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(RunState.create(INSTANCE,
        State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17)));

    CompletableFuture<Void> f = stateManager.receive(Event.terminate(INSTANCE, Optional.empty()))
        .toCompletableFuture();

    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IllegalStateException.class));
    }

    verify(transaction, never()).updateActiveState(any(), any());
  }

  @Test
  public void shouldFailReceiveForUnknownActiveWFInstance() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.empty());

    CompletableFuture<Void> f = stateManager.receive(Event.terminate(INSTANCE, Optional.empty()))
        .toCompletableFuture();

    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
    }

    verify(transaction, never()).updateActiveState(any(), any());
  }

  @Test
  public void shouldGetRunState() throws Exception {
    RunState runState = RunState.create(
        INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17);
    when(storage.readActiveState(INSTANCE)).thenReturn(Optional.of(runState));

    RunState returnedRunState = stateManager.get(INSTANCE);

    assertThat(runState, equalTo(returnedRunState));
  }

  @Test
  public void shouldGetRunStates() throws Exception {
    RunState runState = RunState.create(
        INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17);
    Map<WorkflowInstance, RunState> states = Maps.newConcurrentMap();
    states.put(INSTANCE, runState);
    when(storage.readActiveStates()).thenReturn(states);

    Map<WorkflowInstance, RunState> returnedRunStates = stateManager.activeStates();

    assertThat(returnedRunStates.get(INSTANCE), is(runState));
    assertThat(returnedRunStates.size(), is(1));
  }


  @Test
  public void triggerShouldHandleThrowingOutputHandler() throws Exception {
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE));
    final RuntimeException rootCause = new RuntimeException("foo!");
    doThrow(rootCause).when(outputHandler).transitionInto(any());
    CompletableFuture<Void> f = stateManager.trigger(INSTANCE, TRIGGER1).toCompletableFuture();
    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(rootCause));
    }
  }

  @Test
  public void receiveShouldHandleThrowingOutputHandler() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(RunState.create(INSTANCE,
        State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17)));

    final RuntimeException rootCause = new RuntimeException("foo!");
    doThrow(rootCause).when(outputHandler).transitionInto(any());
    CompletableFuture<Void> f = stateManager.receive(Event.dequeue(INSTANCE)).toCompletableFuture();
    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(rootCause));
    }
  }

  class FooException extends Exception {
  }
}
