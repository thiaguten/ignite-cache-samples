package br.com.thiaguten;

import static br.com.thiaguten.Environment.CITY_CACHE_NAME;
import static br.com.thiaguten.Environment.PERSON_CACHE_NAME;
import static br.com.thiaguten.Environment.SCHEMA;

import br.com.thiaguten.model.City;
import br.com.thiaguten.model.Person;
import br.com.thiaguten.model.PersonPK;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

public class IgniteCacheMyPreferredWay2 {

    public static final String PERSON_KEY_TYPE = "PKey";
    public static final String PERSON_VALUE_TYPE = "PValue";
    public static final String CITY_KEY_TYPE = "CKey";
    public static final String CITY_VALUE_TYPE = "CValue";

    public static void main(String[] args) {
        Ignite ignite = Environment.newIgnite();
        IgniteBinary binary = ignite.binary();

        // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
        // will appear in future versions, JDBC and ODBC drivers do not require it already).
        // https://apacheignite-sql.readme.io/docs/schema-and-indexes
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("temp").setSqlSchema("PUBLIC");
        IgniteCache<?, ?> dummyCache = ignite.getOrCreateCache(cacheCfg);

        dummyCache.query(new SqlFieldsQuery(
                "CREATE TABLE IF NOT EXISTS City (id LONG PRIMARY KEY, name VARCHAR) " +
                        " WITH \"TEMPLATE=replicated, " +
                        " CACHE_NAME=" + CITY_CACHE_NAME + ", " +
                        " KEY_TYPE=" + CITY_KEY_TYPE + ", " +
                        " VALUE_TYPE=" + CITY_VALUE_TYPE + "\"")
                .setSchema(SCHEMA))
                .getAll();

        dummyCache.query(new SqlFieldsQuery(
                "CREATE TABLE IF NOT EXISTS Person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
                        " WITH \"TEMPLATE=partitioned, " +
                        " BACKUPS=1, " +
                        " AFFINITY_KEY=city_id, " +
                        " CACHE_NAME=" + PERSON_CACHE_NAME + ", " +
                        " KEY_TYPE=" + PERSON_KEY_TYPE + ", " +
                        " VALUE_TYPE=" + PERSON_VALUE_TYPE + "\"")
                .setSchema(SCHEMA))
                .getAll();

        dummyCache.query(new SqlFieldsQuery("CREATE INDEX idx_city_name ON City (name)").setSchema(SCHEMA)).getAll();
        dummyCache.query(new SqlFieldsQuery("CREATE INDEX idx_person_name ON Person (name)").setSchema(SCHEMA)).getAll();

        IgniteCache<BinaryObject, BinaryObject> cityCache = ignite.cache(CITY_CACHE_NAME).withKeepBinary();
        IgniteCache<BinaryObject, BinaryObject> personCache = ignite.cache(PERSON_CACHE_NAME).withKeepBinary();

        dummyCache.destroy();

        System.out.println("> Ignite cache names: " + ignite.cacheNames());

        // 1 - SQL API usage to interact with the cache
        // --------------------------------------------

        SqlFieldsQuery insertCity = new SqlFieldsQuery(
                "INSERT INTO City (id, name) VALUES (?, ?)");
        cityCache.query(insertCity.setArgs(1L, "Forest Hill")).getAll();
        cityCache.query(insertCity.setArgs(2L, "Denver")).getAll();
        cityCache.query(insertCity.setArgs(3L, "St. Petersburg")).getAll();

        SqlFieldsQuery insertPerson = new SqlFieldsQuery(
                "INSERT INTO Person (id, city_id, name) VALUES (?, ?, ?)");
        personCache.query(insertPerson.setArgs(1L, 3L, "John Doe")).getAll();
        personCache.query(insertPerson.setArgs(2L, 2L, "Jane Roe")).getAll();
        personCache.query(insertPerson.setArgs(3L, 1L, "Mary Major")).getAll();
        personCache.query(insertPerson.setArgs(4L, 2L, "Richard Miles")).getAll();

        System.out.println(cityCache.query(new SqlFieldsQuery("select _key from City")).getAll());
        System.out.println(cityCache.query(new SqlFieldsQuery("select _key from Person")).getAll());

        // 2 - Key-Value API usage to interact with the cache
        // --------------------------------------------------

        //    2.1 - using binary builder

        System.out.println("1----");
        BinaryObject cKey = binary.builder(CITY_KEY_TYPE)
                .setField("ID", 1L)
                .build();
        BinaryObject pKey = binary.builder(PERSON_KEY_TYPE)
                .setField("ID", 1L)
                .setField("CITY_ID", 3L)
                .build();
        System.out.println(cityCache.get(cKey));
        System.out.println(personCache.get(pKey));

        System.out.println("2----");
        BinaryObject cKey2 = binary.builder(CITY_KEY_TYPE)
                .setField("ID", 4L)
                .build();
        BinaryObject cValue2 = binary.builder(CITY_VALUE_TYPE)
                .setField("NAME", "Brasília")
                .build();
        BinaryObject pKey2 = binary.builder(PERSON_KEY_TYPE)
                .setField("ID", 5L)
                .setField("CITY_ID", 4L)
                .build();
        BinaryObject pValue2 = binary.builder(PERSON_VALUE_TYPE)
                .setField("NAME", "Thiago Gutenberg")
                .build();
        cityCache.put(cKey2, cValue2);
        personCache.put(pKey2, pValue2);
        System.out.println(cityCache.get(cKey2));
        System.out.println(personCache.get(pKey2));

        //    2.2 - using model to binary
        // DID NOT WORK! Infelizmente assim (binary.toBinary) não funciona por causa do tipo da chave e valor do(s) cache(s)

        System.out.println("3----");
        City city = new City(5L, "Natalândia");
        PersonPK personPK = new PersonPK(6L, city.getId());
        Person person = new Person("Dayana Gutenberg");
        BinaryObject cKey3 = binary.builder(CITY_KEY_TYPE)
                .setField("ID", city.getId())
                .build();
        BinaryObject cValue3 = binary.toBinary(city);
        BinaryObject pKey3 = binary.toBinary(personPK);
        BinaryObject pValue3 = binary.toBinary(person);
        cityCache.put(cKey3, cValue3);
        personCache.put(pKey3, pValue3);
        System.out.println(cityCache.get(cKey3));
        System.out.println(personCache.get(pKey3));

        System.out.println(cityCache.query(new SqlFieldsQuery("select _key from City")).getAll());
        System.out.println(cityCache.query(new SqlFieldsQuery("select _key from Person")).getAll());
    }


}
