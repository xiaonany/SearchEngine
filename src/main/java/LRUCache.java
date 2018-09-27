import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
  private int cacheSize;

  public LRUCache(int cacheSize) {
    super(32, 0.75f, true);
    this.cacheSize = cacheSize;
  }

  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() >= cacheSize;
  }
  
  public static void main(String[] args) {
	  LRUCache<String, String> test0 = new LRUCache<String, String>(5);
	  test0.put("1", "1");
	  test0.put("2", "1");
	  test0.put("3", "1");
	  test0.put("4", "1");
	  test0.get("1");
	  test0.put("5", "1");
	  test0.put("6", "1");
	  System.out.println(test0.get("1"));
	  System.out.println(test0.get("2"));
  }
  
}