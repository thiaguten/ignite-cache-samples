package br.com.thiaguten;

import static br.com.thiaguten.Environment.CITY_CACHE_NAME;
import static br.com.thiaguten.Environment.CREATE_PERSON_NAME_INDEX_DLL;
import static br.com.thiaguten.Environment.CREATE_PERSON_TABLE_DDL;
import static br.com.thiaguten.Environment.PERSON_CACHE_NAME;
import static br.com.thiaguten.Environment.SCHEMA;
import static br.com.thiaguten.Environment.keyValueQueryingCityCache;
import static br.com.thiaguten.Environment.keyValueQueryingPersonCache;
import static br.com.thiaguten.Environment.sqlDistributedJoinQueryCache;
import static br.com.thiaguten.Environment.sqlQueryingCityCache;
import static br.com.thiaguten.Environment.sqlQueryingPersonCache;

import br.com.thiaguten.model.City;
import br.com.thiaguten.model.Person;
import br.com.thiaguten.model.PersonPK;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

public class IgniteModelCacheDDL {

  public static void main(String[] args) {
    Ignite ignite = Environment.newIgnite();

    // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
    // will appear in future versions, JDBC and ODBC drivers do not require it already).
//    CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME).setSqlSchema("PUBLIC");
//    IgniteCache<?, ?> dummyCache = ignite.getOrCreateCache(cacheCfg);

    CacheConfiguration<Long, City> cityCacheConfig = new CacheConfiguration<>();
    cityCacheConfig.setName(CITY_CACHE_NAME);
    cityCacheConfig.setSqlSchema(SCHEMA);
    cityCacheConfig.setCacheMode(CacheMode.REPLICATED);
    cityCacheConfig.setIndexedTypes(Long.class, City.class);

    IgniteCache<Long, City> cityCache = ignite.getOrCreateCache(cityCacheConfig);

    // Creating a new cache (Person) from another cache previously created (City), using the Ignite SQL API.
    // https://apacheignite-sql.readme.io/docs/schema-and-indexes
    cityCache.query(new SqlFieldsQuery(CREATE_PERSON_TABLE_DDL).setSchema(SCHEMA)).getAll();

    IgniteCache<PersonPK, Person> personCache = ignite.cache(PERSON_CACHE_NAME);
    personCache.query(new SqlFieldsQuery(CREATE_PERSON_NAME_INDEX_DLL).setSchema(SCHEMA)).getAll();

    System.out.println("> Ignite cache names: " + ignite.cacheNames());

    // 1 - SQL API:
    // ------------
    SqlFieldsQuery insertCity = new SqlFieldsQuery(
        "INSERT INTO City (_key, id, name) VALUES (?, ?, ?)");
    cityCache.query(insertCity.setArgs(1L, 1L, "Forest Hill")).getAll();
    cityCache.query(insertCity.setArgs(2L, 2L, "Denver")).getAll();
    cityCache.query(insertCity.setArgs(3L, 3L, "St. Petersburg")).getAll();

    SqlFieldsQuery insertPerson = new SqlFieldsQuery(
        "INSERT INTO Person (id, name, city_id) VALUES (?, ?, ?)");
    personCache.query(insertPerson.setArgs(1L, "John Doe", 3L)).getAll();
    personCache.query(insertPerson.setArgs(2L, "Jane Roe", 2L)).getAll();
    personCache.query(insertPerson.setArgs(3L, "Mary Major", 1L)).getAll();
    personCache.query(insertPerson.setArgs(4L, "Richard Miles", 2L)).getAll();

    // 2 - Key-Value API:
    // --------------------------
    City city = new City(4L, "Brasília");
    cityCache.put(city.getId(), city);

    PersonPK personPK = new PersonPK(5L, city.getId());
    Person person = new Person("Thiago");
    personCache.put(personPK, person);

    System.out.println("> [City] total size: " + cityCache.size(CachePeekMode.PRIMARY));
    System.out.println("> [Person] total size: " + personCache.size());

    // Querying caches:
    // ---------------
    sqlDistributedJoinQueryCache(personCache);

    sqlQueryingCityCache(cityCache);

    Set<Long> cityKeys = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L));
    keyValueQueryingCityCache(cityKeys, cityCache);

    sqlQueryingPersonCache(personCache);

    Set<PersonPK> personKeys = new HashSet<>(Arrays.asList(
        new PersonPK(1L, 3L),
        new PersonPK(2L, 2L),
        new PersonPK(3L, 1L),
        new PersonPK(4L, 2L),
        new PersonPK(5L, 4L)));
    keyValueQueryingPersonCache(personKeys, personCache);
  }

}
