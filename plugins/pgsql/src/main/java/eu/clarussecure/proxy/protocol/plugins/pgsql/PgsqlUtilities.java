package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.nio.charset.Charset;

import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;

public class PgsqlUtilities {

    public static CString getCString(ByteBuf buffer) {
        CString str = null;

        if (buffer != null) {
            int len = buffer.bytesBefore((byte) 0);
            if (len > -1) {
                str = CString.valueOf(buffer.readSlice(len + 1), len);
            }
        }
        return str;
    }

    public static int computeLength(CharSequence str) {
        return (str != null ? str.length() : 0) + 1;
    }

    public static void putString(ByteBuf byteBuf, CharSequence str) {
        if (str != null) {
            byteBuf.writeCharSequence(str, Charset.forName("ISO-8859-1"));
        }
        byteBuf.writeByte((byte) 0);
    }

    public static String toString(ByteBuf byteBuf) {
        if (byteBuf == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        byteBuf.forEachByte(new ByteProcessor() {

            @Override
            public boolean process(byte value) throws Exception {
                builder.append(String.format("%02x", value & 0xff));
                return true;
            }
        });
        return builder.toString();
    }

}
