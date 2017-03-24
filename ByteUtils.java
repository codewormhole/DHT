import java.io.IOException;

public class ByteUtils {
    
    public static int decodeInt(byte[] source, int pos) throws IOException {
        if(source.length < pos + 4) {
            throw new IOException("EOF reached while decoding int from byte array");
        }
        return (source[pos] & 0xFF) << 24 | (source[pos+1] & 0xFF) << 16 | (source[pos+2] & 0xFF) << 8 | (source[pos+3] & 0xFF);
    }

    public static void encodeInt(int data, byte[] destination, int pos) throws IOException {
        if(destination.length < pos + 4) {
            throw new IOException("Destination array too short to store int");
        }
        destination[pos] = (byte) (data >> 24);
        destination[pos+1] = (byte) (data >> 16);
        destination[pos+2] = (byte) (data >> 8);
        destination[pos+3] = (byte) data;
    }
}
