package bench;

import javax.annotation.CheckForNull;

import java.io.Serializable;

public interface Cache<K, V extends Serializable> {

  void close();

  Cache<K, V> put(K key, V value);

  Iterable<V> values();

  @CheckForNull
  V get(K key);

}
