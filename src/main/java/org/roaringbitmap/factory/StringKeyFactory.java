/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.roaringbitmap.factory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public class StringKeyFactory implements KeyFactory<String> {

    @Override
    public KeySerializer<String> createSerializer() {
        return new KeySerializer<String>() {
            @Override
            public void serialize(ByteBuffer input, String s) {
                int length = s.length();
                input.putInt(length);
                input.put(s.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public void serialize(DataOutputStream stream, String s) throws IOException {
                int length = s.length();
                stream.writeInt(length);
                stream.write(s.getBytes(StandardCharsets.UTF_8), 0, length);
            }

            @Override
            public int serializedSizeInBytes(String s) {
                return Integer.BYTES + s.length();
            }

            @Override
            public boolean fixedSize() {
                return false;
            }
        };
    }

    @Override
    public KeyDeserializer<String> createDeserializer() {
        return buffer -> {
            int length = buffer.getInt();
            byte[] bytes = new byte[length];
            buffer.get(bytes, 0, length);
            return new String(bytes);
        };
    }

    @Override
    public Comparator<String> createCompactor() {
        return Comparator.comparing(o -> o);
    }
}
