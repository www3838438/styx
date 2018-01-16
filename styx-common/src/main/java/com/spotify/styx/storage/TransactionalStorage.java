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

import com.spotify.styx.model.Workflow;
import java.io.IOException;

/**
 * The interface to the persistence layer where transactions can be used across operations, i.e.
 * the transaction is passed to all the operations and committed elsewhere in the code logic.
 */
public interface TransactionalStorage {

  /**
   * Stores a Workflow definition. This doesn't commit the transaction.
   *
   * @param workflow the workflow to store
   * @param transaction the transaction to perform the storage operations with
   */
  void store(Workflow workflow, StorageTransaction transaction) throws IOException;

  /**
   * This provides the {@link StorageTransaction} compatible with the storage tool that this
   * interface is implemented with. Such element is supposed to be used in the business logic to
   * perform the various operations in this interface and commit when needed.
   *
   * @return the generic transaction element to be used in the logic code
   */
  StorageTransaction newTransaction();
}
