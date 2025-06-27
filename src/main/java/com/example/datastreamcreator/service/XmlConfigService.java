package com.example.datastreamcreator.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import com.example.datastreamcreator.config.DatabaseConfig;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

@Service
public class XmlConfigService {
    
    private final XmlMapper xmlMapper = new XmlMapper();
    private final String configPath = "config/database-config.xml";
    
    public DatabaseConfig loadConfig() throws IOException {
        Resource resource = new ClassPathResource(configPath);
        return xmlMapper.readValue(resource.getInputStream(), DatabaseConfig.class);
    }
    
    public void saveConfig(DatabaseConfig config) throws IOException {
        File file = new File("src/main/resources/" + configPath);
        xmlMapper.writeValue(file, config);
    }
    
    public DatabaseConfig.Database getDatabaseById(Long id) throws IOException {
        DatabaseConfig config = loadConfig();
        return config.getDatabases().stream()
                .filter(db -> db.getId().equals(id))
                .findFirst()
                .orElse(null);
    }
    
    public DatabaseConfig.Schema getSchemaById(Long databaseId, Long schemaId) throws IOException {
        DatabaseConfig.Database database = getDatabaseById(databaseId);
        if (database != null && database.getSchemas() != null) {
            return database.getSchemas().stream()
                    .filter(schema -> schema.getId().equals(schemaId))
                    .findFirst()
                    .orElse(null);
        }
        return null;
    }
    
    public void updateDatabase(DatabaseConfig.Database database) throws IOException {
        DatabaseConfig config = loadConfig();
        List<DatabaseConfig.Database> databases = config.getDatabases();
        
        for (int i = 0; i < databases.size(); i++) {
            if (databases.get(i).getId().equals(database.getId())) {
                databases.set(i, database);
                break;
            }
        }
        
        saveConfig(config);
    }
    
    public void addDatabase(DatabaseConfig.Database database) throws IOException {
        DatabaseConfig config = loadConfig();
        
        // 生成新的数据库ID
        Long maxDbId = config.getDatabases().stream()
                .mapToLong(DatabaseConfig.Database::getId)
                .max()
                .orElse(0L);
        database.setId(maxDbId + 1);
        
        // 初始化schemas列表
        if (database.getSchemas() == null) {
            database.setSchemas(new ArrayList<>());
        }
        
        config.getDatabases().add(database);
        saveConfig(config);
    }
    
    public void deleteDatabase(Long id) throws IOException {
        DatabaseConfig config = loadConfig();
        config.getDatabases().removeIf(db -> db.getId().equals(id));
        saveConfig(config);
    }
    
    public void addSchemaToDatabase(Long databaseId, DatabaseConfig.Schema schema) throws IOException {
        DatabaseConfig config = loadConfig();
        DatabaseConfig.Database database = config.getDatabases().stream()
                .filter(db -> db.getId().equals(databaseId))
                .findFirst()
                .orElse(null);
        
        if (database != null) {
            if (database.getSchemas() == null) {
                database.setSchemas(new ArrayList<>());
            }
            
            // 生成新的Schema ID
            Long maxSchemaId = config.getDatabases().stream()
                    .flatMap(db -> db.getSchemas() != null ? db.getSchemas().stream() : new ArrayList<DatabaseConfig.Schema>().stream())
                    .mapToLong(DatabaseConfig.Schema::getId)
                    .max()
                    .orElse(0L);
            schema.setId(maxSchemaId + 1);
            
            database.getSchemas().add(schema);
            saveConfig(config);
        }
    }
    
    public void updateSchema(Long databaseId, DatabaseConfig.Schema schema) throws IOException {
        DatabaseConfig config = loadConfig();
        DatabaseConfig.Database database = config.getDatabases().stream()
                .filter(db -> db.getId().equals(databaseId))
                .findFirst()
                .orElse(null);
        
        if (database != null && database.getSchemas() != null) {
            List<DatabaseConfig.Schema> schemas = database.getSchemas();
            for (int i = 0; i < schemas.size(); i++) {
                if (schemas.get(i).getId().equals(schema.getId())) {
                    schemas.set(i, schema);
                    break;
                }
            }
            saveConfig(config);
        }
    }
    
    public void deleteSchema(Long databaseId, Long schemaId) throws IOException {
        DatabaseConfig config = loadConfig();
        DatabaseConfig.Database database = config.getDatabases().stream()
                .filter(db -> db.getId().equals(databaseId))
                .findFirst()
                .orElse(null);
        
        if (database != null && database.getSchemas() != null) {
            database.getSchemas().removeIf(schema -> schema.getId().equals(schemaId));
            saveConfig(config);
        }
    }
}