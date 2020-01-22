package br.com.thiaguten.model2;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class City {

    @QuerySqlField(index = true)
    private String NAME;

    public City(String name) {
        this.NAME = name;
    }

    public String getName() {
        return NAME;
    }

    public void setName(String name) {
        this.NAME = name;
    }

}
