package eu.clarussecure.proxy.spi;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import eu.clarussecure.proxy.spi.buffer.CustomByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

public class CString implements CharSequence, Cloneable {

    private ByteBuf buffer;
    private int strLen;
    private CharSequence str;
    private int hash;

    public static CString valueOf(ByteBuf buffer) {
        if (buffer == null) {
            return null;
        }
        return new CString(buffer);
    }

    public static CString valueOf(ByteBuf buffer, int strLen) {
        if (buffer == null) {
            return null;
        }
        return new CString(buffer, strLen);
    }

    public static CString valueOf(CharSequence cs) {
        if (cs == null) {
            return null;
        }
        return new CString(cs);
    }

    private CString(ByteBuf buffer) {
        this(buffer, null, -1);
    }

    private CString(ByteBuf buffer, int strLen) {
        this(buffer, null, strLen);
    }

    private CString(CharSequence cs) {
        this(null, cs, -1);
    }

    private CString(ByteBuf buffer, CharSequence cs, int strLen) {
        this.buffer = buffer;
        this.strLen = strLen;
        if (buffer != null) {
            if (this.strLen == -1) {
                this.strLen = buffer.bytesBefore((byte) 0);
                if (this.strLen == -1) {
                    this.strLen = buffer.writerIndex();
                }
            }
        }
        this.str = cs;
    }

    private CString(ByteBuf buffer, int strLen, CharSequence cs, int hash) {
        this.buffer = buffer;
        this.strLen = strLen;
        this.str = cs;
        this.hash = hash;
    }

    @Override
    public Object clone() {
        ByteBuf newBuffer = buffer == null ? null : buffer.copy();
        return new CString(newBuffer, strLen, str, hash);
    }

    public boolean release() {
        if (buffer != null) {
            if (buffer.release()) {
                buffer = null;
                strLen = 0;
                hash = 0;
            }
        }
        return buffer == null;
    }

    public void retain() {
        if (buffer != null) {
            buffer.retain();
        }
    }

    public boolean isBuffered() {
        return buffer != null;
    }

    public ByteBuf getByteBuf() {
        if (buffer == null && str != null) {
            buffer = ByteBufUtil.encodeString(UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap(str),
                    StandardCharsets.ISO_8859_1, 1);
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
        return length() + 1;
    }

    public boolean isEmpty() {
        return str != null ? str.length() == 0 : strLen == 0;
    }

    @Override
    public char charAt(int index) {
        if (index < 0 || index >= length()) {
            throw new StringIndexOutOfBoundsException(index);
        }
        return str != null ? str.charAt(index) : (char) (buffer.getByte(index) & 0xFF);
    }

    public CString substring(int beginIndex) {
        return subSequence(beginIndex, length());
    }

    public CString substring(int beginIndex, int endIndex) {
        return subSequence(beginIndex, endIndex);
    }

    @Override
    public CString subSequence(int start, int end) {
        CharSequence subCs = null;
        if (str != null) {
            subCs = str.subSequence(start, end > str.length() ? str.length() : end);
        }
        ByteBuf subBuffer = null;
        if (buffer != null) {
            subBuffer = buffer.slice(start, end - start);
            while (subBuffer.getByte(end - start - 1) == 0) {
                end--;
            }
        }
        return new CString(subBuffer, subCs, end - start);
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

    public CString append(CharSequence other) {
        return append(other, other.length());
    }

    public CString append(CharSequence other, int length) {
        if (length <= 0) {
            return this;
        }
        if (buffer != null) {
            // Append other to this internal buffer
            ensureBufferCapacity(other, length);
            // Remove zero byte terminator (if any) 
            if (buffer.writerIndex() > 0 && buffer.getByte(buffer.writerIndex() - 1) == 0) {
                buffer.writerIndex(buffer.writerIndex() - 1);
            }
            if (other instanceof CString && ((CString) other).buffer != null) {
                // Append other's internal buffer to this internal buffer
                copy(((CString) other).buffer, length);
            } else {
                // Copy other (char sequence) to this internal buffer
                copy(other, length);
            }
            // Adjust string length
            strLen += length;
        }
        if (str != null) {
            // Append other to this internal char sequence
            CharSequence src = other;
            if (other instanceof CString && ((CString) other).str != null) {
                src = ((CString) other).str;
            }
            if (!(str instanceof Appendable)) {
                // Replace this internal char sequence by a StringBuilder to speed future copy
                str = new StringBuilder(str);
            }
            // Append other's char sequence to this internal char sequence
            try {
                ((Appendable) str).append(src.subSequence(0, length));
            } catch (IOException e) {
                // Should not occurs
                throw new IllegalStateException(e);
            }
        }
        // Reset hashcode (really necessary ?)
        hash = 0;
        return this;
    }

    private void ensureBufferCapacity(CharSequence other, int length) {
        // For CompositeByteBuf only: because this internal buffer is composed of multiple buffer (including other's internal buffer),
        // the zero byte terminator must be removed (to avoid having a 0 byte in the middle of the buffer).  
        if (buffer instanceof CompositeByteBuf && buffer.writerIndex() > 0
                && buffer.getByte(buffer.writerIndex() - 1) == 0) {
            // Netty memory leak: calling capacity(new capacity) remove the last component if not used, but don't release the buffer
            // Workaround: test the capacity of the last component: if 1 -> explicitly remove it, if > 1 -> call capacity(new capacity) 
            int index = ((CompositeByteBuf) buffer).toComponentIndex(strLen);
            ByteBuf last = ((CompositeByteBuf) buffer).internalComponent(index);
            if (last.capacity() == 1) {
                // Last component only contains the zero byte terminator -> just remove it
                ((CompositeByteBuf) buffer).removeComponent(index);
                buffer.writerIndex(buffer.writerIndex() - 1);
            } else {
                // Last component contains more than the zero byte terminator -> call capacity to resize it
                buffer.capacity(strLen);
            }
        }
        // Increase capacity if this internal buffer is not composite or if other is not a CString or don't have internal buffer
        if (!(buffer instanceof CompositeByteBuf)
                || ((CompositeByteBuf) buffer).numComponents() == ((CompositeByteBuf) buffer).maxNumComponents()
                || (!(other instanceof CString) || ((CString) other).buffer == null)) {
            if (buffer.capacity() - strLen < length + 1) {
                // Increase capacity
                if (buffer.maxCapacity() > strLen + length + 1) {
                    // ... using the capacity(new capacity) method
                    buffer.capacity(strLen + length + 1);
                } else {
                    // ... allocating a new buffer
                    ByteBuf newBuffer = buffer.alloc().buffer(strLen + length + 1);
                    newBuffer.writeBytes(buffer);
                    buffer.release();
                    buffer = newBuffer;
                }
            }
        }
    }

    private void copy(ByteBuf src, int length) {
        if (buffer instanceof CompositeByteBuf
                && ((CompositeByteBuf) buffer).numComponents() < ((CompositeByteBuf) buffer).maxNumComponents() - 1) {
            // For CompositeByteBuf only: because this internal buffer is composed of multiple buffer (including other's internal buffer),
            // we simply add the other's internal buffer (no copy).  
            ((CompositeByteBuf) buffer).addComponent(true, /*src.readSlice(length)*/src.slice(0, length));
            // Add an additional buffer for the zero byte terminator
            ((CompositeByteBuf) buffer).capacity(strLen + length + 1);
            buffer.writeByte(0);
        } else {
            // Determinate if copy is necessary
            boolean copy = true;
            // Skip copy if source and destination are the same
            if (buffer.hasMemoryAddress() && src.hasMemoryAddress()) {
                long dstAddr = buffer.memoryAddress() + buffer.writerIndex();
                long srcAddr = src.memoryAddress() /*+ src.readerIndex()*/;
                if (dstAddr == srcAddr && (dstAddr + buffer.writableBytes()) >= (srcAddr + length + 1)) {
                    copy = false;
                }
            }
            if (copy) {
                // Copy source to destination (with zero byte terminator)
                buffer.writeBytes(src, length);
                buffer.writeByte(0);
            } else {
                // Simply adjust writer index
                buffer.writerIndex(buffer.writerIndex() + length + 1);
            }
        }
    }

    private void copy(CharSequence other, int length) {
        // Custom byte allocator to use this internal buffer instead of creating a temporary buffer for the ByteBufUtil.encodeString() method
        CustomByteBufAllocator allocator = new CustomByteBufAllocator() {
            @Override
            public ByteBuf buffer(int length) {
                ByteBuf byteBuf = null;
                if (buffer instanceof CompositeByteBuf) {
                    // ByteBufUtil.encodeString() calls ByteBuf.internalNioBuffer() method, but CompositeByteBuf don't support it
                    // -> determinate if there is one (and only one) component that can be used as the destination buffer 
                    List<ByteBuf> byteBufs = ((CompositeByteBuf) buffer).decompose(buffer.writerIndex(), length);
                    if (byteBufs.size() == 1) {
                        // One component to store 'length' bytes at 'buffer.writerIndex()' index
                        byteBuf = byteBufs.get(0);
                    } else {
                        // More than one component to store 'length' bytes at 'buffer.writerIndex()' index
                        // -> unsupported
                        throw new UnsupportedOperationException();
                    }
                } else {
                    // Create a sliced buffer to store 'length' bytes at 'buffer.writerIndex()' index
                    byteBuf = buffer.slice(buffer.writerIndex(), length);
                }
                // Adjust writer index
                byteBuf.writerIndex(0);
                return byteBuf;
            }
        };
        // Encode other char sequence to this internal buffer.
        // ISO-LATIN-1 alphbet ensures to have one byte per character
        ByteBuf src = ByteBufUtil.encodeString(allocator, CharBuffer.wrap(other.subSequence(0, length)),
                StandardCharsets.ISO_8859_1, 1);
        // Add zero byte terminator
        src.writeByte(0);
        // Adjust writer index
        buffer.writerIndex(buffer.writerIndex() + src.writerIndex());
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
            if (buffer == null) {
                return null;
            }
            str = buffer.toString(0, strLen, StandardCharsets.ISO_8859_1);
        }
        return str.toString();
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            if (str != null) {
                h = str.hashCode();
            } else if (buffer != null) {
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

    /**
     * Compares this {@code CString} to a {@code CharSequence}, ignoring case
     * considerations.  Two are considered equal ignoring case if they
     * are of the same length and corresponding characters in the two 
     * are equal ignoring case.
     *
     * <p> Two characters {@code c1} and {@code c2} are considered the same
     * ignoring case if at least one of the following is true:
     * <ul>
     *   <li> The two characters are the same (as compared by the
     *        {@code ==} operator)
     *   <li> Applying the method {@link
     *        java.lang.Character#toUpperCase(char)} to each character
     *        produces the same result
     *   <li> Applying the method {@link
     *        java.lang.Character#toLowerCase(char)} to each character
     *        produces the same result
     * </ul>
     *
     * @param  cs
     *         The {@code CharSequence} to compare this {@code CString} against
     *
     * @return  {@code true} if the argument is not {@code null} and it
     *          represents an equivalent {@code CharSequence} ignoring case; {@code
     *          false} otherwise
     *
     * @see  #equals(Object)
     */
    public boolean equalsIgnoreCase(CharSequence cs) {
        return (this == cs) ? true
                : (cs != null) && (cs.length() == length()) && regionMatches(true, 0, cs, 0, length());
    }

    /**
     * Tests if this {@code CString} region and a {@code CharSequence} region are equal.
     * <p>
     * A subsequence of this {@code CString} object is compared to a subsequence
     * of the argument {@code other}. The result is {@code true} if these
     * subsequences represent character sequences that are the same, ignoring
     * case if and only if {@code ignoreCase} is true. The subsequence of
     * this {@code CString} object to be compared begins at index
     * {@code toffset} and has length {@code len}. The subsequence of
     * {@code other} to be compared begins at index {@code ooffset} and
     * has length {@code len}. The result is {@code false} if and only if
     * at least one of the following is true:
     * <ul><li>{@code toffset} is negative.
     * <li>{@code ooffset} is negative.
     * <li>{@code toffset+len} is greater than the length of this
     * {@code CString} object.
     * <li>{@code ooffset+len} is greater than the length of the other
     * argument.
     * <li>{@code ignoreCase} is {@code false} and there is some nonnegative
     * integer <i>k</i> less than {@code len} such that:
     * <blockquote><pre>
     * this.charAt(toffset+k) != other.charAt(ooffset+k)
     * </pre></blockquote>
     * <li>{@code ignoreCase} is {@code true} and there is some nonnegative
     * integer <i>k</i> less than {@code len} such that:
     * <blockquote><pre>
     * Character.toLowerCase(this.charAt(toffset+k)) !=
     Character.toLowerCase(other.charAt(ooffset+k))
     * </pre></blockquote>
     * and:
     * <blockquote><pre>
     * Character.toUpperCase(this.charAt(toffset+k)) !=
     *         Character.toUpperCase(other.charAt(ooffset+k))
     * </pre></blockquote>
     * </ul>
     *
     * @param   ignoreCase   if {@code true}, ignore case when comparing
     *                       characters.
     * @param   toffset      the starting offset of the subregion in this
     *                       {@code CString}.
     * @param   other        the {@code CharSequence} argument.
     * @param   ooffset      the starting offset of the subregion in the {@code CharSequence}
     *                       argument.
     * @param   len          the number of characters to compare.
     * @return  {@code true} if the specified subregion of this {@code CString}
     *          matches the specified subregion of the {@code CharSequence} argument;
     *          {@code false} otherwise. Whether the matching is exact
     *          or case insensitive depends on the {@code ignoreCase}
     *          argument.
     */
    public boolean regionMatches(boolean ignoreCase, int toffset, CharSequence other, int ooffset, int len) {
        int to = toffset;
        int po = ooffset;
        // Note: toffset, ooffset, or len might be near -1>>>1.
        if ((ooffset < 0) || (toffset < 0) || (toffset > (long) length() - len)
                || (ooffset > (long) other.length() - len)) {
            return false;
        }
        while (len-- > 0) {
            char c1 = charAt(to++);
            char c2 = other.charAt(po++);
            if (c1 == c2) {
                continue;
            }
            if (ignoreCase) {
                // If characters don't match but case may be ignored,
                // try converting both characters to uppercase.
                // If the results match, then the comparison scan should
                // continue.
                char u1 = Character.toUpperCase(c1);
                char u2 = Character.toUpperCase(c2);
                if (u1 == u2) {
                    continue;
                }
                // Unfortunately, conversion to uppercase does not work properly
                // for the Georgian alphabet, which has strange rules about case
                // conversion.  So we need to make one last check before
                // exiting.
                if (Character.toLowerCase(u1) == Character.toLowerCase(u2)) {
                    continue;
                }
            }
            return false;
        }
        return true;
    }

}
