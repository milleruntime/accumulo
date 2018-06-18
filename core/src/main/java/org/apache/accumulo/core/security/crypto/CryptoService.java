/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.core.security.crypto;

public interface CryptoService {

  /**
   * Initialize the FileEncrypter for the environment and return
   *
   * @return FileEncrypter
   * @since 2.0
   */
  FileEncrypter encryptFile(CryptoEnvironment environment);

  /**
   * Initialize the FileDecrypter for the environment and return
   *
   * @return FileDecrypter
   * @since 2.0
   */
  FileDecrypter decryptFile(CryptoEnvironment environment);

  /**
   * Returns the unique version identifier for this CryptoService implementation. This string is
   * what will get written to the file being encrypted.
   *
   * @since 2.0
   */
  String getVersion();

  /**
   * Runtime Crypto exception
   */
  public class CryptoException extends RuntimeException {
    public CryptoException() {
      super();
    }

    public CryptoException(String message) {
      super(message);
    }

    public CryptoException(String message, Throwable cause) {
      super(message, cause);
    }

    public CryptoException(Throwable cause) {
      super(cause);
    }
  }
}
