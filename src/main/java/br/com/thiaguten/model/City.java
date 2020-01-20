package br.com.thiaguten.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class City {

  @QuerySqlField
  private Long id;

  @QuerySqlField(index = true)
  private String name;

  public City(Long id, String name) {
    this.id = id;
    this.name = name;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
