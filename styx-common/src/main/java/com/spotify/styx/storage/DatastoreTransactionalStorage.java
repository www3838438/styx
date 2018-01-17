/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.styx.storage;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_WORKFLOW_JSON;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.Transaction;
import com.spotify.styx.model.Workflow;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class DatastoreTransactionalStorage implements TransactionalStorage {

  private final Transaction datastoreTransaction;
  private final DatastoreStorage datastoreStorage;

  DatastoreTransactionalStorage(DatastoreStorage datastoreStorage, Transaction transaction) {
    this.datastoreStorage = Objects.requireNonNull(datastoreStorage);
    this.datastoreTransaction = Objects.requireNonNull(transaction);
  }

  @Override
  public void commitOrRollback() throws IOException {
    try {
      datastoreTransaction.commit();
    } finally {
      if (datastoreTransaction.isActive()) {
        datastoreTransaction.rollback();
      }
    }
  }

  @Override
  public void store(Workflow workflow) throws IOException {
    final Key componentKey = datastoreStorage.componentKeyFactory.newKey(workflow.componentId());
    if (datastoreTransaction.get(componentKey) == null) {
      datastoreTransaction.put(Entity.newBuilder(componentKey).build());
    }

    final String json = OBJECT_MAPPER.writeValueAsString(workflow);
    final Key workflowKey = datastoreStorage.workflowKey(workflow.id());
    final Optional<Entity>
        workflowOpt = datastoreStorage.getOpt(datastoreTransaction, workflowKey);
    final Entity workflowEntity = datastoreStorage.asBuilderOrNew(workflowOpt, workflowKey)
        .set(PROPERTY_WORKFLOW_JSON,
            StringValue.newBuilder(json).setExcludeFromIndexes(true).build())
        .build();
    datastoreTransaction.put(workflowEntity);
  }
}
