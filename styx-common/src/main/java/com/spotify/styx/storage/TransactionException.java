/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

public class TransactionException extends StorageException {

  private final boolean conflict;

  public TransactionException(String message, boolean conflict, Throwable cause) {
    super(message, cause);
    this.conflict = conflict;
  }

  public TransactionException(boolean conflict, Throwable cause) {
    super(cause);
    this.conflict = conflict;
  }

  public boolean isConflict() {
    return conflict;
  }
}
