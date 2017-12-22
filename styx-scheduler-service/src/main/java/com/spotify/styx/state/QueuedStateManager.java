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

import static java.util.stream.Collectors.toMap;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import javaslang.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link StateManager} that has an internal queue for all the incoming
 * {@link Event}s that are sent to {@link #receive(Event)}.
 *
 * <p>The events are all processed on an injected {@link Executor}, but sequentially per
 * {@link WorkflowInstance}. This allows event processing to scale across many separate workflow
 * instances while guaranteeing that each state machine progresses sequentially.
 *
 * <p>All {@link RunState#outputHandler()} transitions are also executed on the injected
 * {@link Executor}.
 */
public class QueuedStateManager implements StateManager {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedStateManager.class);

  private final Executor outputHandlerExecutor;
  private final Storage storage;
  private final BiConsumer<SequenceEvent, RunState> eventConsumer;
  private final Executor eventConsumerExecutor;
  private final OutputHandler outputHandler;

  private volatile boolean running = true;

  public QueuedStateManager(
      Executor outputHandlerExecutor,
      Storage storage,
      BiConsumer<SequenceEvent, RunState> eventConsumer,
      Executor eventConsumerExecutor,
      OutputHandler outputHandler) {
    this.outputHandlerExecutor = Objects.requireNonNull(outputHandlerExecutor);
    this.storage = Objects.requireNonNull(storage);
    this.eventConsumer = Objects.requireNonNull(eventConsumer);
    this.eventConsumerExecutor = Objects.requireNonNull(eventConsumerExecutor);
    this.outputHandler = outputHandler;
  }

  @Override
  public void initialize(RunState runState) throws IsClosedException {
    ensureRunning();

    try {
      storage.writeActiveState(runState.workflowInstance(), runState, 0L);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void restore(RunState runState, long count) {
    // not applicable
  }

  @Override
  public CompletionStage<Void> receive(Event event) throws IsClosedException {
    ensureRunning();
    return process(event);
  }

  @Override
  public Map<WorkflowInstance, RunState> activeStates() {
    try {
      return storage.readActiveWorkflowInstances().entrySet().stream()
          .collect(toMap(Entry::getKey, e -> e.getValue()._2));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getActiveStatesCount() {
    // unused method
    throw new UnsupportedOperationException();
  }

  @Override
  public long getQueuedEventsCount() {
    // not applicable ?
    return 0;
  }

  @Override
  public long getActiveStatesCount(WorkflowId workflowId) {
    return activeStates()
        .keySet()
        .stream()
        .filter(workflowInstance -> workflowInstance.workflowId().equals(workflowId))
        .count();
  }

  @Override
  public boolean isActiveWorkflowInstance(WorkflowInstance workflowInstance) {
    try {
      // TODO: look up single wfi instead
      return storage.readActiveWorkflowInstances(workflowInstance.workflowId().componentId())
          .keySet()
          .contains(workflowInstance);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RunState get(WorkflowInstance workflowInstance) {
    try {
      // TODO: look up single wfi instead
      final Tuple2<Long, RunState> t = storage
          .readActiveWorkflowInstances(workflowInstance.workflowId().componentId())
          .get(workflowInstance);
      return t == null
          ? null
          : t._2;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (!running) {
      return;
    }
    running = false;
  }

  private void ensureRunning() throws IsClosedException {
    if (!running) {
      throw new IsClosedException();
    }
  }

  /**
   * Process a state at a given counter position.
   *
   * <p>Processing a state mean that the event and counter positions that caused the transition
   * will be persisted, and the {@link OutputHandler} of the state is called.
   * @param event            The event that transitioned the state
   */
  private CompletionStage<Void> process(Event event) {

    CompletableFuture<Void> processed = new CompletableFuture<>();

    final Tuple2<Long, RunState> nextState;
    try {
      nextState = storage.updateActiveState(event.workflowInstance(), (RunState prevRunState) -> {
        try {
          return prevRunState.transition(event);
        } catch (IllegalStateException e) {
          LOG.warn("Illegal state transition", e);
          throw e;
        }
      });
    } catch (IOException e) {
      processed.completeExceptionally(e);
      return processed;
    }

    SequenceEvent sequenceEvent = SequenceEvent.create(
        event, nextState._1, nextState._2.timestamp());

    try {
      eventConsumerExecutor.execute(() -> eventConsumer.accept(sequenceEvent, nextState._2));
    } catch (Exception e) {
      LOG.warn("Error while consuming event {}", sequenceEvent, e);
    }

    outputHandlerExecutor.execute(() -> {
      try {
        outputHandler.transitionInto(nextState._2);
        processed.complete(null);
      } catch (Throwable e) {
        LOG.warn("Output handler threw", e);
        processed.completeExceptionally(e);
      }
    });

    return processed;
  }

  @VisibleForTesting
  boolean awaitIdle(long timeoutMillis) {
    // not applicable ?
    return true;
  }
}
