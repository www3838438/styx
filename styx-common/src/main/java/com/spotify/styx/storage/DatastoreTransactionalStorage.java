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

import com.google.cloud.datastore.Transaction;
import com.google.common.annotations.VisibleForTesting;
import com.spotify.styx.util.TransactionFailedException;
import java.util.Objects;

class DatastoreTransactionalStorage implements TransactionalStorage {

  private final Transaction transaction;

  DatastoreTransactionalStorage(Transaction transaction) {
    this.transaction = Objects.requireNonNull(transaction);
  }

  @Override
  public void commit() throws TransactionFailedException {
    try {
      transaction.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (transaction.isActive()) {
        rollback();
      }
    }
  }

  @VisibleForTesting
  private void rollback() throws TransactionFailedException {
    transaction.rollback();
    throw new TransactionFailedException();
  }
}
