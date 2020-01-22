package br.com.thiaguten.model2;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class CityPK {

    @QuerySqlField
    @AffinityKeyMapped
    private Long ID;

    public CityPK(Long id) {
        this.ID = id;
    }

    public Long getId() {
        return ID;
    }

    public void setId(Long id) {
        this.ID = id;
    }

}
