package bench;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.mapdb.Serializer;
import org.objenesis.strategy.SerializingInstantiatorStrategy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class KryoSerializer implements Serializer<Object>, Serializable {

  private transient Kryo kryo;

  public KryoSerializer() {
    kryo = new Kryo();
    kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
  }

  @Override
  public void serialize(DataOutput out, Object value) throws IOException {
    Output output = new Output(new DataOutputOutputStream(out));
    kryo.writeClassAndObject(output, value);
    output.close();
  }

  @Override
  public Object deserialize(DataInput in, int available) throws IOException {
    if (available == 0) {
      return null;
    }
    Input input = new Input(new DataInputInputStream(in));
    Object someObject = kryo.readClassAndObject(input);
    input.close();
    return someObject;
  }

  @Override
  public int fixedSize() {
    return -1;
  }

  private static class DataInputInputStream extends InputStream {

    private DataInput dataInput;

    public DataInputInputStream(DataInput in) {
      this.dataInput = in;
    }

    @Override
    public int read() throws IOException {
      try {
        return dataInput.readByte();
      } catch (EOFException e) {
        return -1;
      }
    }

  }

  private static class DataOutputOutputStream extends OutputStream {

    private DataOutput out;

    public DataOutputOutputStream(DataOutput out) {
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
      out.write((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

  }

}
