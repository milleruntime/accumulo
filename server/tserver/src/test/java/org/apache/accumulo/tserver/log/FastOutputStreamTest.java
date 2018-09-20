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
package org.apache.accumulo.tserver.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastOutputStreamTest {

  private static final Logger log = LoggerFactory.getLogger(FastOutputStreamTest.class);

  //TODO determine if we want to keep this test or not
  @Test
  public void perfTest() throws Exception {
    byte[] bytes = new byte[]{(byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC};
    int integer = 167934;
    long myLong = 108023L;
    String str = "here is a test string";
    Random rand = new SecureRandom();

    long start = System.nanoTime();
    for (int i=0; i < 1000; i++) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           DataOutputStream out = new DataOutputStream(baos)) {
        out.writeBoolean(true);
        out.writeByte(bytes[0]);
        out.writeUTF(str + rand.nextFloat());
        out.write(bytes);
        out.writeInt(integer);
        out.write(bytes, 0, bytes.length);
        out.writeLong(myLong);
        out.flush();
      }
    }
    long stop = System.nanoTime();
    double dataTime = (stop-start)/1000;
    log.debug("DataOuputStream time = {}", dataTime);

    start = System.nanoTime();
    for (int i=0; i < 1000; i++) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           FastOutputStream out = new FastOutputStream(baos)) {
        out.writeBoolean(true);
        out.writeByte(bytes[0]);
        out.writeUTF(str + rand.nextFloat());
        out.write(bytes);
        out.writeInt(integer);
        out.write(bytes, 0, bytes.length);
        out.writeLong(myLong);
        out.flush();
      }
    }
    stop = System.nanoTime();
    double fastTime = (stop-start)/1000;
    log.debug("FastOutputStream time = {}", fastTime);

    double result = (dataTime-fastTime)/dataTime;
    log.info("Fast time was {} percent faster. ", Math.round(result * 100.0) / 100.0);
    assertTrue(fastTime < dataTime);
  }

  // test optimized writeInt
  @Test
  public void testOptimizedWriteInt() throws Exception {
    byte[] dosData;
    byte[] fosData;
    int v1 = 167934;
    int v2 = -892374;
    int v3 = 138;

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream out = new DataOutputStream(baos)) {
      out.writeInt(v1);
      out.writeInt(v2);
      out.writeInt(v3);
      out.flush();
      dosData = baos.toByteArray();
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         FastOutputStream out = new FastOutputStream(baos)) {
      out.writeInt(v1);
      out.writeInt(v2);
      out.writeInt(v3);
      out.flush();
      fosData = baos.toByteArray();
    }
    assertTrue(Arrays.equals(dosData, fosData));

  }

  @Test
  public void testSameAsDataOutputStream() throws Exception {
    byte[] dosData;
    byte[] fosData;
    byte[] bytes = new byte[]{(byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC};
    int integer = 167934;
    long myLong = 108023L;
    String str = "here is a test string";

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream out = new DataOutputStream(baos)) {
      out.writeBoolean(true);
      out.writeByte(bytes[0]);
      out.writeUTF(str);
      out.write(bytes);
      out.writeInt(integer);
      out.write(bytes, 0, bytes.length);
      out.writeLong(myLong);
      out.flush();
      dosData = baos.toByteArray();
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         FastOutputStream out = new FastOutputStream(baos)) {
      out.writeBoolean(true);
      out.writeByte(bytes[0]);
      out.writeUTF(str);
      out.write(bytes);
      out.writeInt(integer);
      out.write(bytes, 0, bytes.length);
      out.writeLong(myLong);
      out.flush();
      fosData = baos.toByteArray();
    }

    assertTrue(Arrays.equals(dosData, fosData));

    byte[] readBytes = new byte[bytes.length];
    byte[] readBytes2 = new byte[bytes.length];

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(fosData))) {
      assertTrue(in.readBoolean());
      assertEquals(bytes[0], in.readByte());
      assertEquals(str, in.readUTF());
      in.readFully(readBytes);
      assertTrue(Arrays.equals(bytes, readBytes));
      assertEquals(integer, in.readInt());
      in.read(readBytes2, 0, readBytes2.length);
      assertTrue(Arrays.equals(bytes, readBytes2));
    }
  }
}
