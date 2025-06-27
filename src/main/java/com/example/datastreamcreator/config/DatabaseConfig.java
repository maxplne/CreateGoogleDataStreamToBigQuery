package com.example.datastreamcreator.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

@JacksonXmlRootElement(localName = "databases")
public class DatabaseConfig {
    
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "database")
    private List<Database> databases;
    
    public List<Database> getDatabases() {
        return databases;
    }
    
    public void setDatabases(List<Database> databases) {
        this.databases = databases;
    }
    
    public static class Database {
        @JacksonXmlProperty(isAttribute = true)
        private Long id;
        private String name;
        private String type;
        
        @JacksonXmlProperty(localName = "source-profile-id")
        private String sourceProfileId;
        
        @JacksonXmlProperty(localName = "destination-profile-id")
        private String destinationProfileId;
        
        @JacksonXmlElementWrapper(localName = "schemas")
        @JacksonXmlProperty(localName = "schema")
        private List<Schema> schemas;
        
        // Getters and Setters
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        
        public String getSourceProfileId() { return sourceProfileId; }
        public void setSourceProfileId(String sourceProfileId) { this.sourceProfileId = sourceProfileId; }
        
        public String getDestinationProfileId() { return destinationProfileId; }
        public void setDestinationProfileId(String destinationProfileId) { this.destinationProfileId = destinationProfileId; }
        
        public List<Schema> getSchemas() { return schemas; }
        public void setSchemas(List<Schema> schemas) { this.schemas = schemas; }
    }
    
    public static class Schema {
        @JacksonXmlProperty(isAttribute = true)
        private Long id;
        
        @JacksonXmlProperty(localName = "stream-id")
        private String streamId;
        
        @JacksonXmlProperty(localName = "mysql-schema-name")
        private String mysqlSchemaName;
        
        private String description;
        
        // Getters and Setters
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        
        public String getStreamId() { return streamId; }
        public void setStreamId(String streamId) { this.streamId = streamId; }
        
        public String getMysqlSchemaName() { return mysqlSchemaName; }
        public void setMysqlSchemaName(String mysqlSchemaName) { this.mysqlSchemaName = mysqlSchemaName; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
    }
}