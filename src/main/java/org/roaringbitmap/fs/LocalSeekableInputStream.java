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

package org.roaringbitmap.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

/** Local {@link SeekableInputStream}. */
public class LocalSeekableInputStream extends SeekableInputStream {

    private final FileInputStream in;
    private final FileChannel channel;

    public LocalSeekableInputStream(File file) throws FileNotFoundException {
        this.in = new FileInputStream(file);
        this.channel = in.getChannel();
    }

    @Override
    public void seek(long desired) throws IOException {
        if (desired != getPos()) {
            this.channel.position(desired);
        }
    }

    @Override
    public long getPos() throws IOException {
        return channel.position();
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
