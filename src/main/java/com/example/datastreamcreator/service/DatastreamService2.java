package com.example.datastreamcreator.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.datastreamcreator.config.DatastreamProperties;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Service
public class DatastreamService2 {

    @Value("${gcp.project-id}")
    private String projectId;

    @Value("${gcp.region}")
    private String region;

    @Value("${datastream.source-profile-id}")
    private String sourceProfileId;

    @Value("${datastream.destination-profile-id}")
    private String destinationProfileId;

    @Value("${datastream.stream-id:default-stream}")
    private String defaultStreamId;

    @Value("${datastream.mysql-schema-name:default_mysql_schema}")
    private String defaultMysqlSchemaName;

    private final DatastreamProperties datastreamProperties;

    private static final String GCLOUD_COMMAND_PATH = "C:\\Users\\yuan_\\AppData\\Local\\Google\\Cloud SDK\\google-cloud-sdk\\bin\\gcloud.cmd";
    private static final String BQ_COMMAND_PATH = "C:\\Users\\yuan_\\AppData\\Local\\Google\\Cloud SDK\\google-cloud-sdk\\bin\\bq.cmd";

    public DatastreamService2(DatastreamProperties datastreamProperties) {
        this.datastreamProperties = datastreamProperties;
    }

    public String createDatastream() {
        String actualStreamId = datastreamProperties.getStreamId();
        String actualMysqlSchemaName = datastreamProperties.getMysqlSchemaName();
        List<String> actualTablesToSync = datastreamProperties.getTablesToSync();

        if (actualTablesToSync == null || actualTablesToSync.isEmpty()) {
            return "Error: 无法创建 Datastream 流，因为要同步的表列表为空。请在 application.yml 中配置 datastream.tables-to-sync。";
        }

        Path mysqlConfigPath = null;
        Path bigqueryConfigPath = null;

        try {
            // 创建 BigQuery dataset
            String datasetCreationResult = ensureBigQueryDataset(actualMysqlSchemaName);
            if (datasetCreationResult.startsWith("Error")) {
                return "创建 BigQuery Dataset 失败: " + datasetCreationResult;
            }

            // 生成 mysql-source-config JSON 文件
            StringBuilder tablesJson = new StringBuilder();
            for (int i = 0; i < actualTablesToSync.size(); i++) {
                tablesJson.append("{\"table\": \"").append(actualTablesToSync.get(i)).append("\"}");
                if (i < actualTablesToSync.size() - 1) {
                    tablesJson.append(",");
                }
            }

            String mysqlJsonContent = "{\n" +
                    "  \"includeObjects\": {\n" +
                    "    \"mysqlDatabases\": [\n" +
                    "      {\n" +
                    "        \"database\": \"" + actualMysqlSchemaName + "\",\n" +
                    "        \"mysqlTables\": [" + tablesJson.toString() + "]\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "}";

            mysqlConfigPath = Files.createTempFile("mysql-source-config", ".json");
            Files.writeString(mysqlConfigPath, mysqlJsonContent);

            // 生成 bigquery-destination-config JSON 文件 (修正结构)
            String bqJsonContent = "{\n" +
            	    "  \"singleTargetDataset\": {\n" +
            	    "    \"datasetId\": \"" + projectId + ":" + actualMysqlSchemaName + "\"\n" +
            	    "  },\n" +
            	    "  \"dataFreshness\": \"0s\"\n" +
            	    "}";

            bigqueryConfigPath = Files.createTempFile("bigquery-destination-config", ".json");
            Files.writeString(bigqueryConfigPath, bqJsonContent);

            // 构建 gcloud 命令
            List<String> command = new ArrayList<>();
            command.add(GCLOUD_COMMAND_PATH);
            command.add("datastream");
            command.add("streams");
            command.add("create");
            command.add(actualStreamId);
            command.add("--location=" + region);
            command.add("--project=" + projectId);
            command.add("--source=" + sourceProfileId);
            command.add("--destination=" + destinationProfileId);
            command.add("--display-name=" + actualStreamId + "-stream");
            command.add("--backfill-all");
            command.add("--mysql-source-config=" + mysqlConfigPath.toAbsolutePath().toString());
            command.add("--bigquery-destination-config=" + bigqueryConfigPath.toAbsolutePath().toString());

            // 打印命令调试
            System.out.println("执行命令: " + String.join(" ", command));

            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            int exitCode = process.waitFor();

            if (exitCode == 0) {
                System.out.println("Datastream 流创建成功:\n" + output);
                return "成功";
            } else {
                System.err.println("Datastream 流创建失败，退出码: " + exitCode);
                System.err.println("错误输出:\n" + output);
                return "Error: Datastream 流创建失败: " + output;
            }

        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("发生异常: " + e.getMessage());
            e.printStackTrace();
            return "Error: 发生异常: " + e.getMessage();
        } finally {
            // 清理临时文件
            try {
                if (mysqlConfigPath != null) Files.deleteIfExists(mysqlConfigPath);
                if (bigqueryConfigPath != null) Files.deleteIfExists(bigqueryConfigPath);
            } catch (IOException e) {
                System.err.println("临时文件清理失败: " + e.getMessage());
            }
        }
    }

    private String ensureBigQueryDataset(String datasetId) {
        try {
            List<String> checkCommand = Arrays.asList(
                    BQ_COMMAND_PATH, "show", "--project_id=" + projectId, datasetId
            );

            Process checkProcess = new ProcessBuilder(checkCommand).redirectErrorStream(true).start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(checkProcess.getInputStream()));
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            int exitCode = checkProcess.waitFor();

            if (exitCode == 0) {
                System.out.println("Dataset 已存在: " + datasetId);
                return "Dataset 已存在";
            } else if (output.toString().contains("Not found")) {
                System.out.println("Dataset 不存在，正在创建: " + datasetId);
                List<String> createCommand = Arrays.asList(
                        BQ_COMMAND_PATH, "mk", "--project_id=" + projectId,
                        "--location=" + region, datasetId
                );

                Process createProcess = new ProcessBuilder(createCommand).redirectErrorStream(true).start();
                BufferedReader createReader = new BufferedReader(new InputStreamReader(createProcess.getInputStream()));
                StringBuilder createOutput = new StringBuilder();
                while ((line = createReader.readLine()) != null) {
                    createOutput.append(line).append("\n");
                }

                int createExitCode = createProcess.waitFor();
                if (createExitCode == 0) {
                    System.out.println("Dataset 创建成功: " + datasetId);
                    return "Dataset 创建成功";
                } else {
                    System.err.println("Dataset 创建失败: " + createOutput);
                    return "Error: 创建失败: " + createOutput;
                }
            } else {
                System.err.println("检查 dataset 失败: " + output);
                return "Error: 检查失败: " + output;
            }

        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return "Error: 检查/创建 dataset 异常: " + e.getMessage();
        }
    }
}
