/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.security;

//TODO Move this into SPI once ready.

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;

/**
 * A pluggable security module. The {@link #initialize(String, byte[])} method resets all the
 * security for Accumulo.
 *
 * @since 2.1
 */
public interface SecurityModule {
  String ZKUserAuths = "/Authorizations";
  String ZKUserSysPerms = "/System";
  String ZKUserTablePerms = "/Tables";
  String ZKUserNamespacePerms = "/Namespaces";

  /**
   * Initialize the security for Accumulo. WARNING: Calling this will drop all users for Accumulo
   * and reset security. This is automatically called when Accumulo is initialized.
   *
   * The rootUser or System user can perform ALL actions.
   */
  void initialize(String rootUser, byte[] token);


  /**
   * Verify the userPrincipal and serialized {@link AuthenticationToken} are valid.
   *
   * @param userPrincipal
   *          the user to authenticate
   * @param token
   *          the {@link AuthenticationToken}
   * @return boolean true if successful or false otherwise
   * @throws AccumuloSecurityException
   *           if a problem occurred during authentication
   */
  boolean authenticate(String userPrincipal, AuthenticationToken token)
          throws AccumuloSecurityException;

  /**
   * Check if user can perform the action.
   */
  boolean check(String user, Action action);

}
