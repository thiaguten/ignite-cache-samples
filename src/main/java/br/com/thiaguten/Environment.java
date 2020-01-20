package br.com.thiaguten;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
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

  public static Ignite newIgnite() {
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

}
