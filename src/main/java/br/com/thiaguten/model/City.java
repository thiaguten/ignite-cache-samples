package br.com.thiaguten.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class City {

  @QuerySqlField
  private Long ID;

  @QuerySqlField(index = true)
  private String NAME;

  public City(Long id, String name) {
    this.ID = id;
    this.NAME = name;
  }

  public Long getId() {
    return ID;
  }

  public void setId(Long id) {
    this.ID = id;
  }

  public String getName() {
    return NAME;
  }

  public void setName(String name) {
    this.NAME = name;
  }

}
