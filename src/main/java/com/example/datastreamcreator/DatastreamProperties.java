package com.example.datastreamcreator;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "datastream")
public class DatastreamProperties {

    private String sourceProfileId;
    private String destinationProfileId;
    private String streamId;
    private String mysqlSchemaName;
    private List<String> tablesToSync;

    // Getter & Setter
    public String getSourceProfileId() {
        return sourceProfileId;
    }

    public void setSourceProfileId(String sourceProfileId) {
        this.sourceProfileId = sourceProfileId;
    }

    public String getDestinationProfileId() {
        return destinationProfileId;
    }

    public void setDestinationProfileId(String destinationProfileId) {
        this.destinationProfileId = destinationProfileId;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getMysqlSchemaName() {
        return mysqlSchemaName;
    }

    public void setMysqlSchemaName(String mysqlSchemaName) {
        this.mysqlSchemaName = mysqlSchemaName;
    }

    public List<String> getTablesToSync() {
        return tablesToSync;
    }

    public void setTablesToSync(List<String> tablesToSync) {
        this.tablesToSync = tablesToSync;
    }
}
