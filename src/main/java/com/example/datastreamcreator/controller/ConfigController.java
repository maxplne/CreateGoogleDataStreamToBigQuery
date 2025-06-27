package com.example.datastreamcreator.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.example.datastreamcreator.config.DatabaseConfig;
import com.example.datastreamcreator.service.XmlConfigService;

@Controller
public class ConfigController {
    
    @Autowired
    private XmlConfigService xmlConfigService;
    
    @GetMapping("/")
    public String index(Model model) {
        try {
            DatabaseConfig config = xmlConfigService.loadConfig();
            model.addAttribute("databases", config.getDatabases());
        } catch (IOException e) {
            model.addAttribute("error", "加载配置失败: " + e.getMessage());
        }
        return "config";
    }
    
    // 数据库相关API
    @GetMapping("/api/databases")
    @ResponseBody
    public ResponseEntity<?> getDatabases() {
        try {
            DatabaseConfig config = xmlConfigService.loadConfig();
            return ResponseEntity.ok(config.getDatabases());
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("加载配置失败: " + e.getMessage());
        }
    }
    
    @PostMapping("/api/databases")
    @ResponseBody
    public ResponseEntity<?> addDatabase(@RequestBody DatabaseConfig.Database database) {
        try {
            xmlConfigService.addDatabase(database);
            return ResponseEntity.ok("数据库配置添加成功");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("添加配置失败: " + e.getMessage());
        }
    }
    
    @PutMapping("/api/databases/{id}")
    @ResponseBody
    public ResponseEntity<?> updateDatabase(@PathVariable Long id, 
                                          @RequestBody DatabaseConfig.Database database) {
        try {
            database.setId(id);
            xmlConfigService.updateDatabase(database);
            return ResponseEntity.ok("数据库配置更新成功");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("更新配置失败: " + e.getMessage());
        }
    }
    
    @DeleteMapping("/api/databases/{id}")
    @ResponseBody
    public ResponseEntity<?> deleteDatabase(@PathVariable Long id) {
        try {
            xmlConfigService.deleteDatabase(id);
            return ResponseEntity.ok("数据库配置删除成功");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("删除配置失败: " + e.getMessage());
        }
    }
    
    // Schema相关API
    @PostMapping("/api/databases/{databaseId}/schemas")
    @ResponseBody
    public ResponseEntity<?> addSchema(@PathVariable Long databaseId, 
                                     @RequestBody DatabaseConfig.Schema schema) {
        try {
            xmlConfigService.addSchemaToDatabase(databaseId, schema);
            return ResponseEntity.ok("Schema添加成功");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("添加Schema失败: " + e.getMessage());
        }
    }
    
    @PutMapping("/api/databases/{databaseId}/schemas/{schemaId}")
    @ResponseBody
    public ResponseEntity<?> updateSchema(@PathVariable Long databaseId,
                                        @PathVariable Long schemaId,
                                        @RequestBody DatabaseConfig.Schema schema) {
        try {
            schema.setId(schemaId);
            xmlConfigService.updateSchema(databaseId, schema);
            return ResponseEntity.ok("Schema更新成功");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("更新Schema失败: " + e.getMessage());
        }
    }
    
    @DeleteMapping("/api/databases/{databaseId}/schemas/{schemaId}")
    @ResponseBody
    public ResponseEntity<?> deleteSchema(@PathVariable Long databaseId, 
                                        @PathVariable Long schemaId) {
        try {
            xmlConfigService.deleteSchema(databaseId, schemaId);
            return ResponseEntity.ok("Schema删除成功");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("删除Schema失败: " + e.getMessage());
        }
    }
    
    // 数据流创建 - GET方法（用于单个Schema执行）
    @GetMapping("/create-datastream1")
    @ResponseBody
    public ResponseEntity<?> createDatastreamGet(@RequestParam("source-profile-id") String sourceProfileId,
                                                @RequestParam("stream-id") String streamId,
                                                @RequestParam("mysql-schema-name") String mysqlSchemaName) {
        try {
            // 模拟处理过程
            Thread.sleep(2000); // 模拟执行时间
            
            String logMessage = String.format(
                "数据流创建成功!\n" +
                "源配置ID: %s\n" +
                "Stream ID: %s\n" +
                "MySQL Schema: %s\n" +
                "执行时间: %s\n" +
                "状态: 成功",
                sourceProfileId,
                streamId,
                mysqlSchemaName,
                new java.util.Date().toString()
            );
            
            System.out.println("=== 单个Schema执行 ===");
            System.out.println(logMessage);
            System.out.println("===================");
            
            return ResponseEntity.ok(logMessage);
        } catch (Exception e) {
            String errorMessage = String.format(
                "数据流创建失败!\n" +
                "错误信息: %s\n" +
                "源配置ID: %s\n" +
                "Stream ID: %s\n" +
                "MySQL Schema: %s",
                e.getMessage(),
                sourceProfileId,
                streamId,
                mysqlSchemaName
            );
            
            return ResponseEntity.badRequest().body(errorMessage);
        }
    }
    
    // 数据流创建 - POST方法（用于批量处理）
    @PostMapping("/create-datastream")
    @ResponseBody
    public ResponseEntity<?> createDatastreamPost(@RequestBody Map<String, Object> request) {
        try {
            List<Map<String, Object>> selectedItems = (List<Map<String, Object>>) request.get("selectedItems");
            
            int processedCount = 0;
            StringBuilder logBuilder = new StringBuilder();
            
            for (Map<String, Object> item : selectedItems) {
                String type = (String) item.get("type");
                Long databaseId = Long.valueOf(item.get("databaseId").toString());
                
                if ("database".equals(type)) {
                    // 处理整个数据库
                    DatabaseConfig.Database db = xmlConfigService.getDatabaseById(databaseId);
                    if (db != null) {
                        logBuilder.append(String.format("数据库: %s (%s)\n", 
                            db.getName(), db.getSourceProfileId()));
                        processedCount++;
                    }
                } else if ("schema".equals(type)) {
                    // 处理单个Schema
                    Long schemaId = Long.valueOf(item.get("schemaId").toString());
                    String streamId = (String) item.get("streamId");
                    String mysqlSchemaName = (String) item.get("mysqlSchemaName");
                    
                    DatabaseConfig.Database db = xmlConfigService.getDatabaseById(databaseId);
                    DatabaseConfig.Schema schema = xmlConfigService.getSchemaById(databaseId, schemaId);
                    
                    if (db != null && schema != null) {
                        logBuilder.append(String.format("Schema: %s -> %s (%s)\n", 
                            streamId, mysqlSchemaName, schema.getDescription()));
                        processedCount++;
                    }
                }
            }
            
            System.out.println("=== 批量创建数据流 ===");
            System.out.println(logBuilder.toString());
            System.out.println("===================");
            
            return ResponseEntity.ok(Map.of(
                "message", "数据流创建成功",
                "processedCount", processedCount,
                "details", logBuilder.toString()
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("创建数据流失败: " + e.getMessage());
        }
    }
}