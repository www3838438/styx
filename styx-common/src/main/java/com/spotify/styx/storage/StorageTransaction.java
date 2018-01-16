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

import java.io.IOException;

/**
 * The interface to the transaction functionality from the persistent layer, meant to be used in the
 * logic code across operations provided by the {@link TransactionalStorage} and commit when
 * appropriate according to the logic.
 */
public interface StorageTransaction {

  /**
   * This should commit or rollback all the registered operations.
   *
   * @throws IOException if the commit failes and the rollback is performed
   */
  void commitOrRollback() throws IOException;

  /**
   * This returns the transaction object specific to the {@link TransactionalStorage} implementation.
   * This is not supposed to be accessed from the logic code but only from the {@link TransactionalStorage} class
   * making use of such object. To make sure the transaction object is compatible with the {@link TransactionalStorage}
   * implementation that uses it, an explicit cast check might be required.
   *
   * @return the transaction object specific to the storage implementation
   */
  Object getTransactionObject();
}
