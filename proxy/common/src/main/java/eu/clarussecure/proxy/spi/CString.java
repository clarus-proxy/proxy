package eu.clarussecure.proxy.spi;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

public class CString implements CharSequence {

    private ByteBuf buffer;
    private int strLen;
    private String str;
    private int hash;

    public static CString valueOf(ByteBuf buffer) {
        if (buffer == null) {
            return null;
        }
        return new CString(buffer.readRetainedSlice(buffer.readableBytes()));
    }

    public static CString valueOf(ByteBuf buffer, int initialReadableBytes) {
        if (buffer == null) {
            return null;
        }
        if (initialReadableBytes < 0 || initialReadableBytes > buffer.readableBytes()) {
            throw new IndexOutOfBoundsException(
                    String.format("valueOf: %d (expected: %d <= initialReadableBytes <= buffer.readableBytes()(%d))",
                            initialReadableBytes, 0, buffer.readableBytes()));
        }
        buffer = buffer.readRetainedSlice(buffer.readableBytes());
        buffer.writerIndex(initialReadableBytes);
        return new CString(buffer);
    }

    public static CString valueOf(int capacity, ByteBufAllocator allocator) {
        return new CString(allocator.buffer(capacity));
    }

    public static CString valueOf(String str) {
        if (str == null) {
            return null;
        }
        return new CString(str);
    }

    private CString(ByteBuf buffer) {
        this(buffer, null);
    }

    private CString(String str) {
        this(null, str);
    }

    private CString(ByteBuf buffer, String str) {
        this.buffer = buffer;
        strLen = 0;
        if (buffer != null) {
            strLen = buffer.bytesBefore((byte) 0);
            if (strLen == -1) {
                strLen = buffer.writerIndex();
            }
        }
        this.str = str;
    }

    public boolean isBuffered() {
        return buffer != null;
    }

    public ByteBuf getByteBuf(ByteBufAllocator allocator) {
        if (buffer == null && str != null) {
            buffer = ByteBufUtil.encodeString(allocator, CharBuffer.wrap(str), StandardCharsets.ISO_8859_1, 1);
            buffer.writeByte((byte) 0);
            strLen = buffer.writerIndex() - 1;
        }
        return buffer;
    }

    @Override
    public int length() {
        return str != null ? str.length() : strLen;
    }

    public int clen() {
//        return str != null ? str.length() + 1 : buffer.capacity();
        return length() + 1;
    }

    public boolean isEmpty() {
        return str != null ? str.isEmpty() : strLen == 0;
    }

    @Override
    public char charAt(int index) {
        return str != null ? str.charAt(index) : (char) (buffer.getByte(index) & 0xFF);
    }

    @Override
    public CString subSequence(int start, int end) {
        ByteBuf subBuffer = buffer != null ? buffer.slice(start, end - start) : null;
        String subStr = null;
        if (str != null) {
            if (end > str.length()) {
                end = str.length();
            }
            subStr = str.substring(start, end);
        }
        return new CString(subBuffer, subStr);
    }

    public int indexOf(CharSequence target) {
        return indexOf(target, 0);
    }

    public int indexOf(CharSequence target, int fromIndex) {
        return indexOf(this, 0, this.length(), target, 0, target.length(), fromIndex);
    }

    private static int indexOf(CString source, int sourceOffset, int sourceCount, CharSequence target, int targetOffset,
            int targetCount, int fromIndex) {
        if (fromIndex >= sourceCount) {
            return (targetCount == 0 ? sourceCount : -1);
        }
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        if (targetCount == 0) {
            return fromIndex;
        }

        char first = target.charAt(targetOffset);
        int max = sourceOffset + (sourceCount - targetCount);

        for (int i = sourceOffset + fromIndex; i <= max; i++) {
            /* Look for first character. */
            if (source.charAt(i) != first) {
                while (++i <= max && source.charAt(i) != first)
                    ;
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && source.charAt(j) == target.charAt(k); j++, k++)
                    ;

                if (j == end) {
                    /* Found whole string. */
                    return i - sourceOffset;
                }
            }
        }
        return -1;
    }

    public CString append(CharSequence cs, ByteBufAllocator allocator) {
        return append(cs, cs.length(), allocator);
    }

    public CString append(CharSequence cs, int length, ByteBufAllocator allocator) {
        if (buffer != null) {
            if (buffer.capacity() - strLen < length + 1) {
                ByteBuf newBuffer = allocator.buffer(strLen + length + 1);
                newBuffer.writeBytes(buffer);
                buffer.release();
                buffer = newBuffer;
            }
            if (buffer.writerIndex() > 0 && buffer.getByte(buffer.writerIndex() - 1) == 0) {
                buffer.writerIndex(buffer.writerIndex() - 1);
            }
            if (cs instanceof CString && ((CString) cs).buffer != null) {
                ByteBuf src = ((CString) cs).buffer;
                boolean copy = true;
                if (buffer.hasMemoryAddress() && src.hasMemoryAddress()) {
                    long dstAddr = buffer.memoryAddress() + buffer.writerIndex();
                    long srcAddr = src.memoryAddress() + src.readerIndex();
                    if (dstAddr <= srcAddr && (dstAddr + buffer.writableBytes()) >= (srcAddr + /*src.readableBytes()*/length + 1)) {
                        copy = false;
                    }
                }
                if (copy) {
                    buffer.writeBytes(src, length);
                    buffer.writeByte(0);
                } else {
                    buffer.writerIndex(buffer.writerIndex() + /*src.readableBytes()*/length + 1);
                }
            } else {
                ByteBuf src = ByteBufUtil.encodeString(allocator, CharBuffer.wrap(cs.subSequence(0, length)), StandardCharsets.ISO_8859_1, 1);
                src.writeByte(0);
                buffer.writeBytes(src);
                src.release();
            }
            strLen = buffer.bytesBefore((byte) 0);
            if (strLen == -1) {
                strLen = buffer.writerIndex();
            }
        }
        if (str != null) {
            CharSequence src = cs;
            if (cs instanceof CString && ((CString) cs).str != null) {
                src = ((CString) cs).str;
            }
            str += src.subSequence(0, length);
        }
        hash = 0;
        return this;
    }

    public CString reset() {
        if (buffer != null) {
            buffer.readerIndex(0);
            buffer.writerIndex(0);
            strLen = 0;
        }
        str = null;
        hash = 0;
        return this;
    }

    @Override
    public String toString() {
        if (str == null) {
            str = buffer.toString(0, strLen, StandardCharsets.ISO_8859_1);
        }
        return str;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            if (str != null) {
                h = str.hashCode();
            } else {
                for (int i = 0; i < strLen; i++) {
                    h = 31 * h + ((int) buffer.getByte(i) & 0xFF);
                }
            }
            hash = h;
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof CharSequence)) {
            return false;
        }
        CharSequence other = (CharSequence) obj;
        if (length() != other.length()) {
            return false;
        }
        for (int i = 0; i < length(); i++) {
            if (charAt(i) != other.charAt(i)) {
                return false;
            }
        }
        return true;
    }

}
