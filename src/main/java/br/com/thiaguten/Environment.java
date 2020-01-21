package br.com.thiaguten;

import br.com.thiaguten.model.City;
import br.com.thiaguten.model.Person;
import br.com.thiaguten.model.PersonPK;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

public class Environment {

  public static final String CLIENT_CONNECTOR_HOST = "127.0.0.1";
  public static final int CLIENT_CONNECTOR_PORT = 3500;
  public static final String SCHEMA = "MY_SCHEMA";
  public static final String CITY_CACHE_NAME = "City";
  public static final String PERSON_CACHE_NAME = "Person";
  public static final String PERSON_KEY_TYPE = "br.com.thiaguten.model.PersonPK";
  public static final String PERSON_VALUE_TYPE = "br.com.thiaguten.model.Person";
  public static final String CITY_KEY_TYPE = "java.lang.Long";
  public static final String CITY_VALUE_TYPE = "br.com.thiaguten.model.City";

  // Create table based on REPLICATED template
  public static final String CREATE_CITY_TABLE_DDL = ""
      + "CREATE TABLE IF NOT EXISTS City (id LONG PRIMARY KEY, name VARCHAR) " +
      " WITH \"TEMPLATE=replicated, " +
      " CACHE_NAME="+CITY_CACHE_NAME+", " +
      " KEY_TYPE="+CITY_KEY_TYPE+", " +
      " VALUE_TYPE="+CITY_VALUE_TYPE+"\"";

  // Create table based on PARTITIONED template with one backup
  public static final String CREATE_PERSON_TABLE_DDL = ""
      + "CREATE TABLE IF NOT EXISTS Person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
      " WITH \"TEMPLATE=partitioned, " +
      " BACKUPS=1, " +
      " AFFINITY_KEY=city_id, " +
      " CACHE_NAME="+PERSON_CACHE_NAME+", " +
      " KEY_TYPE="+PERSON_KEY_TYPE+", " +
      " VALUE_TYPE="+PERSON_VALUE_TYPE+"\"";

  // Create an index on the City table
  public static final String CREATE_CITY_NAME_INDEX_DLL =
      "CREATE INDEX idx_city_name ON City (name)";

  // Create an index on the Person table
  public static final String CREATE_PERSON_NAME_INDEX_DLL =
      "CREATE INDEX idx_person_name ON Person (name)";

  public static Ignite newIgnite() {
    // Create custom ignite work directory path
    Path customWorkDir = Paths.get(".", "target", "ignite", "work").toAbsolutePath().normalize();

    // Create Ignite configuration
    IgniteConfiguration igniteConfiguration = new IgniteConfiguration()
        .setWorkDirectory(customWorkDir.toString())
        .setDiscoverySpi(new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryMulticastIpFinder()
                .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))))
        .setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setHost(CLIENT_CONNECTOR_HOST)
            .setPort(CLIENT_CONNECTOR_PORT))
        .setSqlSchemas(SCHEMA);
    return newIgnite(igniteConfiguration);
  }

  public static Ignite newIgnite(IgniteConfiguration igniteConfiguration) {
    // Set Ignite properties
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty(org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET, "true");
    System.setProperty(IgniteSystemProperties.IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true");
//    System.setProperty(IgniteSystemProperties.IGNITE_H2_DEBUG_CONSOLE, "true");

    // Create Ignite
    Ignite ignite = Ignition.start(igniteConfiguration);
    Runtime.getRuntime().addShutdownHook(new Thread(ignite::close));
    return ignite;
  }

  public static void createJdbcTablesAndIndexes() throws Exception {
    // Register JDBC driver
    Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

    // Create JDBC URL
    String url = "jdbc:ignite:thin://"+CLIENT_CONNECTOR_HOST+":"+CLIENT_CONNECTOR_PORT+"/"+SCHEMA;

    // Open JDBC connection and create JDBC Statement
    try (Connection connection = DriverManager.getConnection(url);
        Statement stmt = connection.createStatement()) {

      // Create tables
      stmt.executeUpdate(CREATE_CITY_TABLE_DDL);
      stmt.executeUpdate(CREATE_PERSON_TABLE_DDL);

      // Create indexes
      stmt.executeUpdate(CREATE_CITY_NAME_INDEX_DLL);
      stmt.executeUpdate(CREATE_PERSON_NAME_INDEX_DLL);
    }
  }

  public static void sqlQueryingCityCache(IgniteCache<?, ?> cache) {
    System.out.println("> [City] SQL query result:");
    SqlFieldsQuery select = new SqlFieldsQuery("SELECT * FROM City ORDER BY id");
    try (FieldsQueryCursor<List<?>> cursor = cache.query(select)) {
      for (List<?> row : cursor) {
        String rowValues = row.stream().map(Object::toString).collect(Collectors.joining(", "));
        System.out.println("\t" + rowValues);
      }
    }
  }

  public static void sqlQueryingPersonCache(IgniteCache<?, ?> cache) {
    System.out.println("> [Person] SQL query result:");
    SqlFieldsQuery select = new SqlFieldsQuery("SELECT id, city_id, name FROM Person ORDER BY id");
    try (FieldsQueryCursor<List<?>> cursor = cache.query(select)) {
      for (List<?> row : cursor) {
        String rowValues = row.stream().map(Object::toString).collect(Collectors.joining(", "));
        System.out.println("\t" + rowValues);
      }
    }
  }

  public static void keyValueQueryingCityCache(
      Set<Long> keys, IgniteCache<Long, City> cityCache) {
    System.out.println("> [City] Key-Value query result:");

//    keys.forEach(key -> System.out.println(cityCache.get(key).getName()));

//    Map<Long, City> map = cityCache.getAll(keys);

////    TreeMap<Long, City> sortedMap = new TreeMap<>(Comparator.comparing(Function.identity()));
////    sortedMap.putAll(map);
//    TreeMap<Long, City> sortedMap = new TreeMap<>(map);
//    sortedMap.forEach((id,city) -> System.out.println("\t" + id + ", " + city.getName()));

//    map.entrySet()
//        .stream()
//        .sorted(Map.Entry.comparingByKey())
//        .map(Map.Entry::getValue)
//        .map(city -> "\t" + city.getId() + ", " + city.getName())
//        .forEach(System.out::println);

    Collection<CacheEntry<Long, City>> entries = cityCache.getEntries(keys);
    entries.stream()
        .sorted(Comparator.comparing(CacheEntry::getKey))
        .map(CacheEntry::getValue)
        .map(city -> "\t" + city.getId() + ", " + city.getName())
        .forEach(System.out::println);
  }

  public static void keyValueQueryingCityCacheBinary(
      Set<Long> keys, IgniteCache<Long, BinaryObject> cityCache) {
    System.out.println("> [City] Key-Value query result:");

//    keys.forEach(key -> System.out.println((String) cityCache.get(key).field("name")));

//    Map<Long, BinaryObject> map = cityCache.getAll(keys);

////    TreeMap<Long, BinaryObject> sortedMap = new TreeMap<>(Comparator.comparing(Function.identity()));
////    sortedMap.putAll(map);
//    TreeMap<Long, BinaryObject> sortedMap = new TreeMap<>(map);
//    sortedMap.forEach((id,binaryObject) ->
//        System.out.println("\t" + id + ", " + binaryObject.field("name")));

//    map.entrySet()
//        .stream()
//        .sorted(Map.Entry.comparingByKey())
//        .map(Map.Entry::getValue)
//        .map(binaryObject -> "\t" + binaryObject.field("id") + ", " + binaryObject.field("name"))
//        .forEach(System.out::println);

    Collection<CacheEntry<Long, BinaryObject>> entries = cityCache.getEntries(keys);
    entries.stream()
        .sorted(Comparator.comparing(CacheEntry::getKey))
        .map(CacheEntry::getValue)
        .map(binaryObject -> "\t" + binaryObject.field("id") + ", " + binaryObject.field("name"))
        .forEach(System.out::println);
  }

  public static void keyValueQueryingPersonCache(
      Set<PersonPK> keys, IgniteCache<PersonPK, Person> personCache) {
    System.out.println("> [Person] Key-Value query result:");

//    keys.forEach(key -> System.out.println(personCache.get(key).getName()));

//    Map<PersonPK, Person> map = personCache.getAll(keys);

//    TreeMap<PersonPK, Person> sortedMap = new TreeMap<>(Comparator.comparing(PersonPK::getId));
//    sortedMap.putAll(map);
//    sortedMap.forEach((personPK,person) ->
//        System.out.println("\t" + personPK.getId() + ", " + personPK.getCityId() + ", " + person.getName()));

//    map.entrySet()
//        .stream()
//        .sorted(Comparator.comparing(cacheEntry -> cacheEntry.getKey().getId()))
//        .map(entry -> {
//          PersonPK key = entry.getKey();
//          Person value = entry.getValue();
//          return "\t" + key.getId() + ", " + key.getCityId() + ", " + value.getName();
//        })
//        .forEach(System.out::println);

    Collection<CacheEntry<PersonPK, Person>> entries = personCache.getEntries(keys);
    entries.stream()
        .sorted(Comparator.comparing(cacheEntry -> cacheEntry.getKey().getId()))
        .map(entry -> {
          PersonPK key = entry.getKey();
          Person value = entry.getValue();
          return "\t" + key.getId() + ", " + key.getCityId() + ", " + value.getName();
        })
        .forEach(System.out::println);
  }

  public static void keyValueQueryingPersonCacheBinary(
      Set<BinaryObject> keys, IgniteCache<BinaryObject, BinaryObject> personCache) {
    System.out.println("> [Person] Key-Value query result:");

//    keys.forEach(key -> System.out.println((String) personCache.get(key).field("name")));

//    Map<BinaryObject, BinaryObject> map = personCache.getAll(keys);

//    TreeMap<BinaryObject, BinaryObject> sortedMap = new TreeMap<>(Comparator.comparing(binary -> binary.field("id")));
//    sortedMap.putAll(map);
//    sortedMap.forEach((k,v) ->
//        System.out.println("\t" + k.field("id") + ", " + k.field("city_id") + ", " + v.field("name")));

//    map.entrySet()
//        .stream()
//        .sorted(Comparator.comparing(cacheEntry -> cacheEntry.getKey().field("id")))
//        .map(entry -> {
//          BinaryObject key = entry.getKey();
//          BinaryObject value = entry.getValue();
//          return "\t" + key.field("id") + ", " + key.field("city_id") + ", " + value.field("name");
//        })
//        .forEach(System.out::println);

    Collection<CacheEntry<BinaryObject, BinaryObject>> entries = personCache.getEntries(keys);
    entries.stream()
        .sorted(Comparator.comparing(cacheEntry -> cacheEntry.getKey().field("id")))
        .map(entry -> {
          BinaryObject key = entry.getKey();
          BinaryObject value = entry.getValue();
          return "\t" + key.field("id") + ", " + key.field("city_id") + ", " + value.field("name");
        })
        .forEach(System.out::println);
  }

  public static void sqlDistributedJoinQueryCache(IgniteCache<?, ?> cache) {
    String cacheName = cache.getName();

    // Querying data from the cluster using a distributed JOIN.
    SqlFieldsQuery select = new SqlFieldsQuery(
        "SELECT p.name, c.name FROM Person p, City c WHERE p.city_id = c.id");

    System.out.println("> ["+cacheName+"] SQL query result using a distributed JOIN:");
    try (FieldsQueryCursor<List<?>> cursor = cache.query(select)) {
      for (List<?> row : cursor) {
        String rowValues = row.stream().map(Object::toString).collect(Collectors.joining(", "));
        System.out.println("\t" + rowValues);
      }
    }
  }

}
