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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.TransactionException;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import javaslang.Tuple;
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
 * <p>All {@link #outputHandler} transitions are also executed on the injected
 * {@link Executor}.
 */
public class QueuedStateManager implements StateManager {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedStateManager.class);

  private static final long NO_EVENTS_PROCESSED = -1L;

  private static final Duration SHUTDOWN_GRACE_PERIOD = Duration.ofSeconds(5);

  private final Time time;

  private final ExecutorService eventTransitionExecutor;
  private final Storage storage;
  private final BiConsumer<SequenceEvent, RunState> eventConsumer;
  private final Executor eventConsumerExecutor;
  private final OutputHandler outputHandler;

  private volatile boolean running = true;

  public QueuedStateManager(
      Time time,
      ExecutorService eventTransitionExecutor,
      Storage storage,
      BiConsumer<SequenceEvent, RunState> eventConsumer,
      Executor eventConsumerExecutor,
      OutputHandler outputHandler) {
    this.time = Objects.requireNonNull(time);
    this.storage = Objects.requireNonNull(storage);
    this.eventConsumer = Objects.requireNonNull(eventConsumer);
    this.eventConsumerExecutor = Objects.requireNonNull(eventConsumerExecutor);
    this.eventTransitionExecutor = Objects.requireNonNull(eventTransitionExecutor);
    this.outputHandler = Objects.requireNonNull(outputHandler);
  }

  @Override
  public CompletionStage<Void> trigger(WorkflowInstance workflowInstance, Trigger trigger) {
    final long counter;
    try {
      counter = storage.getLatestStoredCounter(workflowInstance).orElse(NO_EVENTS_PROCESSED);
    } catch (IOException e) {
      return CompletableFutures.exceptionallyCompletedFuture(e);
    }

    // TODO: optional retry on transaction conflict

    return CompletableFuture.supplyAsync(() -> {

      // Write active state to datastore
      final RunState runState = RunState.create(workflowInstance, State.NEW, StateData.zero(), time.get(), counter);
      try {
        storage.runInTransaction(tx -> {
          final Optional<Workflow> workflow = tx.workflow(workflowInstance.workflowId());
          if (!workflow.isPresent()) {
            throw new IllegalArgumentException("Workflow not found: " + workflowInstance.workflowId().toKey());
          }
          return tx.writeActiveState(workflowInstance, runState);
        });
      } catch (TransactionException e) {
        if (e.isAlreadyExists()) {
          throw new IllegalStateException("Workflow instance is already triggered: " + workflowInstance.toKey());
        } else if (e.isConflict()) {
          LOG.debug("Transactional conflict, abort triggering Workflow instance: " + workflowInstance.toKey());
          throw new RuntimeException(e);
        } else {
          throw new RuntimeException(e);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }).thenCompose((nil) -> {
      final Event event = Event.triggerExecution(workflowInstance, trigger);
      try {
        return receive(event);
      } catch (IsClosedException isClosedException) {
        LOG.warn("Failed to send 'triggerExecution' event", isClosedException);
        // Best effort attempt to rollback the creation of the NEW state
        try {
          storage.deleteActiveState(workflowInstance);
        } catch (IOException e) {
          LOG.warn("Failed to remove dangling NEW state for: {}", workflowInstance);
        }
        throw new RuntimeException(isClosedException);
      }
    });
  }

  @Override
  public CompletionStage<Void> receive(Event event) throws IsClosedException {
    ensureRunning();
    LOG.debug("Event {}", event);

    // TODO: optional retry on transaction conflict

    // TODO: run on striped executor to get event execution in order.

    final CompletableFuture<Tuple2<SequenceEvent, RunState>> processedTransition =
        CompletableFuture.supplyAsync(() -> {
          // Perform transactional state transition
          try {
            return storage.runInTransaction(tx -> {

              // Read active state from datastore
              final Optional<RunState> runStateOptional = tx.readActiveState(event.workflowInstance());
              if (!runStateOptional.isPresent()) {
                String message = "Received event for unknown workflow instance: " + event;
                LOG.warn(message);
                throw new IllegalArgumentException(message);
              }
              final RunState runState = runStateOptional.get();
              // Transition to next state
              final RunState nextRunState;
              try {
                nextRunState = runState.transition(event);
              } catch (IllegalStateException e) {
                // TODO: illegal state transitions might become common as multiple scheduler
                //       instances concurrently consume events from k8s.
                LOG.warn("Illegal state transition", e);
                throw e;
              }

              // Write new state to datastore (or remove it if terminal)
              if (nextRunState.state().isTerminal()) {
                tx.deleteActiveState(event.workflowInstance());
              } else {
                tx.updateActiveState(event.workflowInstance(), nextRunState);
              }

              final SequenceEvent sequenceEvent =
                  SequenceEvent.create(event, nextRunState.counter(), nextRunState.timestamp());

              return Tuple.of(sequenceEvent, nextRunState);
            });
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }, eventTransitionExecutor);
    return processedTransition.thenComposeAsync((emitEventInfoTuple) -> {
      emitEvent(emitEventInfoTuple._1, emitEventInfoTuple._2);
      return CompletableFuture.completedFuture(null);
    });
  }

  private void emitEvent(SequenceEvent sequenceEvent, RunState runState) {
    // Write event to bigtable
    try {
      storage.writeEvent(sequenceEvent);
    } catch (IOException e) {
      LOG.warn("Error writing event {}", sequenceEvent, e);
    }

    // Publish event
    try {
      eventConsumerExecutor.execute(() -> eventConsumer.accept(sequenceEvent, runState));
    } catch (Exception e) {
      LOG.warn("Error while consuming event {}", sequenceEvent, e);
    }

    // Execute output handler(s)
    outputHandler.transitionInto(runState);
  }

  @Override
  public Map<WorkflowInstance, RunState> activeStates() {
    try {
      return storage.readActiveStates();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RunState get(WorkflowInstance workflowInstance) {
    final Optional<RunState> runStateOptional;
    try {
      runStateOptional = storage.readActiveState(workflowInstance);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return runStateOptional.orElse(null);
  }

  @Override
  public void close() throws IOException {
    if (!running) {
      return;
    }
    running = false;

    eventTransitionExecutor.shutdown();
    try {
      if (!eventTransitionExecutor
          .awaitTermination(SHUTDOWN_GRACE_PERIOD.toMillis(), MILLISECONDS)) {
        throw new IOException(
            "Graceful shutdown failed, event loop did not finish within grace period");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  void ensureRunning() throws IsClosedException {
    if (!running) {
      throw new IsClosedException();
    }
  }
}
