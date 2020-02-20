// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.sequoiadb;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.bson.types.Binary;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class SequoiadbStaticBufferChange {

    public static final StandardSerializer serializer = new StandardSerializer();

    private static final char[] HEX_CHARS_UPPER;

    static {
        HEX_CHARS_UPPER = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    }

    public StaticBuffer string2StaticBuffer(final String s) {
        ByteBuffer out = ByteBuffer.wrap(s.getBytes(Charset.forName("UTF-8")));
//        ByteBuffer out = ByteBuffer.wrap (toBytesBinary (s));
        return StaticArrayBuffer.of(out);
    }

    public String staticBuffer2String(final StaticBuffer s) {
        return toStringBinary (s.as(StaticBuffer.ARRAY_FACTORY));
//        return new String(s.as(StaticBuffer.ARRAY_FACTORY),Charset.forName("UTF-8"));
    }

    public<O> StaticBuffer object2StaticBuffer(final O value) {
        if (value==null) throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        if (!serializer.validDataType(value.getClass())) throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value);
        DataOutput out = serializer.getDataOutput(128);
        out.writeClassAndObject(value);
        return out.getStaticBuffer();
    }

    public<O> O staticBuffer2Object(final StaticBuffer s, Class<O> dataType) {
        Object value = serializer.readClassAndObject(s.asReadBuffer());
        Preconditions.checkArgument(dataType.isInstance(value),"Could not deserialize to [%s], got: %s",dataType,value);
        return (O)value;
    }

    public static String toStringBinary(byte[] b) {
        return b == null ? "null" : toStringBinary(b, 0, b.length);
    }

    public static byte[] toBytesBinary(String in) {
        byte[] b = new byte[in.length()];
        int size = 0;

        for(int i = 0; i < in.length(); ++i) {
            char ch = in.charAt(i);
            if (ch == '\\' && in.length() > i + 1 && in.charAt(i + 1) == 'x') {
                char hd1 = in.charAt(i + 2);
                char hd2 = in.charAt(i + 3);
                if (isHexDigit(hd1) && isHexDigit(hd2)) {
                    byte d = (byte)((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));
                    b[size++] = d;
                    i += 3;
                }
            } else {
                b[size++] = (byte)ch;
            }
        }

        byte[] b2 = new byte[size];
        System.arraycopy(b, 0, b2, 0, size);
        return b2;
    }

    private static String toStringBinary(byte[] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        if (off >= b.length) {
            return result.toString();
        } else {
            if (off + len > b.length) {
                len = b.length - off;
            }

            for(int i = off; i < off + len; ++i) {
                int ch = b[i] & 255;
                if (ch >= 32 && ch <= 126 && ch != 92) {
                    result.append((char)ch);
                } else {
                    result.append("\\x");
                    result.append(HEX_CHARS_UPPER[ch / 16]);
                    result.append(HEX_CHARS_UPPER[ch % 16]);
                }
            }

            return result.toString();
        }
    }

    public Binary staticBuffer2Binary (final StaticBuffer s) {
//        byte[] bs = s.getBytes(0, s.length());
        byte[] bs = s.as(StaticBuffer.ARRAY_FACTORY);
        return new Binary(bs);
    }

    public StaticBuffer byte2StaticBuffer (byte[] bs) {
        return object2StaticBuffer (bs);
    }

    public StaticBuffer binary2StaticBuffer (final Binary b) {
        byte[] bs = b.getData();

        return byte2StaticBuffer(bs);
    }



    private static boolean isHexDigit(char c) {
        return c >= 'A' && c <= 'F' || c >= '0' && c <= '9';
    }

    private static byte toBinaryFromHex(byte ch) {
        return ch >= 65 && ch <= 70 ? (byte)(10 + (byte)(ch - 65)) : (byte)(ch - 48);
    }
}
