package bench;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.objenesis.strategy.SerializingInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

public class LuceneCache<V extends Serializable> implements Cache<String, V> {

  private IndexWriter writer;
  private Kryo kryo;
  private DirectoryReader reader;
  private Class<V> valueClass;
  private StandardAnalyzer analyzer;
  private IndexSearcher searcher;

  public LuceneCache(Class<V> valueClass) throws IOException {
    this.valueClass = valueClass;
    analyzer = new StandardAnalyzer(Version.LUCENE_47);
    Directory index = new SimpleFSDirectory(new File("target/lucene"));

    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
    config.setOpenMode(OpenMode.CREATE);

    kryo = new Kryo();
    kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());

    writer = new IndexWriter(index, config);

    reader = DirectoryReader.open(writer, true);
    searcher = new IndexSearcher(reader);
  }

  @Override
  public void close() {
    try {
      writer.close();
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LuceneCache<V> put(String key, V value) {
    ByteArrayOutputStream serData = new ByteArrayOutputStream();
    serialize(value, serData);
    Document doc = new Document();
    doc.add(new TextField("key", key, Field.Store.YES));
    doc.add(new StoredField("data", serData.toByteArray()));
    try {
      writer.addDocument(doc);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Iterable<V> values() {
    reopenReader();
    return new LuceneIterable();
  }

  private void reopenReader() {
    try {
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer, true);
      if (newReader != null) {
        reader = newReader;
        searcher = new IndexSearcher(reader);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private class LuceneIterable implements Iterable<V> {

    @Override
    public Iterator<V> iterator() {
      return new LuceneIterator();
    }

  }

  private final class LuceneIterator implements Iterator<V> {

    private int position;
    private Bits liveDocs;

    public LuceneIterator() {
      position = 0;
      liveDocs = MultiFields.getLiveDocs(reader);
    }

    @Override
    public boolean hasNext() {
      return position < reader.maxDoc() && (liveDocs == null || liveDocs.get(position));
    }

    @Override
    public V next() {
      try {
        Document doc = reader.document(position++);
        byte[] serData = doc.getBinaryValue("data").bytes;
        return deserialize(serData);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");

    }
  }

  private void serialize(V data, ByteArrayOutputStream serData) {
    Output output = new Output(serData);
    kryo.writeObject(output, data);
    output.close();
  }

  private V deserialize(byte[] serData) {
    Input input = new Input(serData);
    try {
      return kryo.readObject(input, valueClass);
    } finally {
      input.close();
    }
  }

  @Override
  public V get(String key) {
    reopenReader();
    try {
      Query q = new QueryParser(Version.LUCENE_47, "key", analyzer).parse("key:" + key);
      int hitsPerPage = 1;
      TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
      searcher.search(q, collector);
      TopDocs topDocs = collector.topDocs();
      if (topDocs.totalHits == 0) {
        return null;
      }
      ScoreDoc[] hits = topDocs.scoreDocs;
      Document doc = searcher.doc(hits[0].doc);
      byte[] serData = doc.getBinaryValue("data").bytes;
      return deserialize(serData);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
