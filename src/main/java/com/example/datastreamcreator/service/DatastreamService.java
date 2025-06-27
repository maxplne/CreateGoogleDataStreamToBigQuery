package com.example.datastreamcreator.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.datastreamcreator.config.DatastreamProperties;

@Service
public class DatastreamService {

	@Value("${gcp.project-id}")
	private String projectId;

	@Value("${gcp.region}")
	private String region;

	// 从配置文件读取 profile 名称
//	@Value("${datastream.source-profile-id}")
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

	// 正規表現パターンを定義
	// ^ : 文字列の先頭
	// [a-z0-9-]+ : 小文字の英字 (a-z)、数字 (0-9)、またはハイフン (-) が1回以上繰り返される
	// $ : 文字列の末尾
	private static final String ID_REGEX = "^[a-z0-9-]+$";
	private static final Pattern ID_PATTERN = Pattern.compile(ID_REGEX);

	public DatastreamService(DatastreamProperties datastreamProperties) {
		this.datastreamProperties = datastreamProperties;
	}

	public String createDatastream(String sourceProfileId, String streamId, String mysqlSchemaName) {
		String actualStreamId = streamId != null ? streamId : datastreamProperties.getStreamId();
		String actualMysqlSchemaName = mysqlSchemaName != null ? mysqlSchemaName
				: datastreamProperties.getMysqlSchemaName();
		this.sourceProfileId = sourceProfileId != null ? sourceProfileId : datastreamProperties.getSourceProfileId();
		List<String> actualTablesToSync = datastreamProperties.getTablesToSync();

		if (!isValidId(actualStreamId)) {
			return "Error: StreamId只能小写英数字或者'-'并且长度小于60";
		}
		if (actualTablesToSync == null || actualTablesToSync.isEmpty()) {
			return "Error: 无法创建 Datastream 流，因为要同步的表列表为空。请在 application.yml 中配置 datastream.tables-to-sync。";
		}

		Path mysqlConfigPath = null;
		Path bigqueryConfigPath = null;

		try {
			// 创建 BigQuery dataset（与 MySQL schema 同名）
			String datasetCreationResult = ensureBigQueryDataset(actualMysqlSchemaName);
			if (datasetCreationResult.startsWith("Error")) {
				return "创建 BigQuery Dataset 失败: " + datasetCreationResult;
			}

			// 生成 MySQL 源配置 JSON 文件
			StringBuilder tablesJson = new StringBuilder();
			for (int i = 0; i < actualTablesToSync.size(); i++) {
				tablesJson.append("{\"table\": \"").append(actualTablesToSync.get(i)).append("\"}");
				if (i < actualTablesToSync.size() - 1) {
					tablesJson.append(",");
				}
			}

			String mysqlJsonContent = "{\n" + "  \"includeObjects\": {\n" + "    \"mysqlDatabases\": [\n" + "      {\n"
					+ "        \"database\": \"" + actualMysqlSchemaName + "\",\n" + "        \"mysqlTables\": ["
					+ tablesJson.toString() + "]\n" + "      }\n" + "    ]\n" + "  }\n" + "}";

			mysqlConfigPath = Files.createTempFile("datastream-mysql-source-config", ".json");
			Files.writeString(mysqlConfigPath, mysqlJsonContent);

			// 生成 BigQuery 目标配置 JSON 文件
			// 使用 sourceHierarchyDatasets 来实现按 schema 分组，并设置 append-only 模式
			String bqJsonContent = "{\n" + "  \"sourceHierarchyDatasets\": {\n" + "    \"datasetTemplate\": {\n"
					+ "      \"location\": \"" + region + "\",\n" + "      \"datasetIdPrefix\": \"\"\n" + "    }\n"
					+ "  },\n" + "  \"appendOnly\": {}\n" + "}";

			bigqueryConfigPath = Files.createTempFile("datastream-bigquery-destination-config", ".json");
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
			command.add("--source=" + sourceProfileId); // 使用配置文件中的源 profile
			command.add("--destination=" + destinationProfileId); // 使用配置文件中的目标 profile
			command.add("--display-name=" + actualStreamId + "-stream");
			command.add("--backfill-all");
			command.add("--mysql-source-config=" + mysqlConfigPath.toAbsolutePath().toString());
			command.add("--bigquery-destination-config=" + bigqueryConfigPath.toAbsolutePath().toString());

			// 打印命令用于调试
			System.out.println("执行命令: " + String.join(" ", command));
			System.out.println("MySQL 源配置内容:");
			System.out.println(mysqlJsonContent);
			System.out.println("BigQuery 目标配置内容:");
			System.out.println(bqJsonContent);

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
				return "成功创建 Datastream 流: " + actualStreamId + "\n源 Profile: " + sourceProfileId + "\n目标 Profile: "
						+ destinationProfileId + "\nMySQL Schema: " + actualMysqlSchemaName + "\n同步表: "
						+ String.join(", ", actualTablesToSync) + "\n写入模式: Append-Only"
						+ "\nDataset 模式: Source Hierarchy (按 Schema 分组)";
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
				if (mysqlConfigPath != null)
					Files.deleteIfExists(mysqlConfigPath);
				if (bigqueryConfigPath != null)
					Files.deleteIfExists(bigqueryConfigPath);
			} catch (IOException e) {
				System.err.println("临时文件清理失败: " + e.getMessage());
			}
		}
	}

	// 如果您更倾向于使用 singleTargetDataset 模式（单个数据集），可以使用这个替代方法
	public String createDatastreamWithSingleDataset() {
		String actualStreamId = datastreamProperties.getStreamId();
		String actualMysqlSchemaName = datastreamProperties.getMysqlSchemaName();
		List<String> actualTablesToSync = datastreamProperties.getTablesToSync();

		if (actualTablesToSync == null || actualTablesToSync.isEmpty()) {
			return "Error: 无法创建 Datastream 流，因为要同步的表列表为空。";
		}

		Path mysqlConfigPath = null;
		Path bigqueryConfigPath = null;

		try {
			// 创建 BigQuery dataset
			String datasetCreationResult = ensureBigQueryDataset(actualMysqlSchemaName);
			if (datasetCreationResult.startsWith("Error")) {
				return "创建 BigQuery Dataset 失败: " + datasetCreationResult;
			}

			// 生成 MySQL 源配置
			StringBuilder tablesJson = new StringBuilder();
			for (int i = 0; i < actualTablesToSync.size(); i++) {
				tablesJson.append("{\"table\": \"").append(actualTablesToSync.get(i)).append("\"}");
				if (i < actualTablesToSync.size() - 1) {
					tablesJson.append(",");
				}
			}

			String mysqlJsonContent = "{\n" + "  \"includeObjects\": {\n" + "    \"mysqlDatabases\": [\n" + "      {\n"
					+ "        \"database\": \"" + actualMysqlSchemaName + "\",\n" + "        \"mysqlTables\": ["
					+ tablesJson.toString() + "]\n" + "      }\n" + "    ]\n" + "  }\n" + "}";

			mysqlConfigPath = Files.createTempFile("datastream-mysql-source-config", ".json");
			Files.writeString(mysqlConfigPath, mysqlJsonContent);

			// 使用 singleTargetDataset 和 appendOnly 模式
			String bqJsonContent = "{\n" + "  \"singleTargetDataset\": {\n" + "    \"datasetId\": \"" + projectId + ":"
					+ actualMysqlSchemaName + "\"\n" + "  },\n" + "  \"appendOnly\": {}\n" + "}";

			bigqueryConfigPath = Files.createTempFile("datastream-bigquery-destination-config", ".json");
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
			command.add("--source=" + sourceProfileId); // 使用配置文件中的源 profile
			command.add("--destination=" + destinationProfileId); // 使用配置文件中的目标 profile
			command.add("--display-name=" + actualStreamId + "-stream");
			command.add("--backfill-all");
			command.add("--mysql-source-config=" + mysqlConfigPath.toAbsolutePath().toString());
			command.add("--bigquery-destination-config=" + bigqueryConfigPath.toAbsolutePath().toString());

			System.out.println("执行命令: " + String.join(" ", command));
			System.out.println("BigQuery 目标配置内容:");
			System.out.println(bqJsonContent);

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
				return "成功创建 Datastream 流 (Single Dataset 模式): " + actualStreamId;
			} else {
				System.err.println("Datastream 流创建失败，退出码: " + exitCode);
				System.err.println("错误输出:\n" + output);
				return "Error: Datastream 流创建失败: " + output;
			}

		} catch (IOException | InterruptedException e) {
			Thread.currentThread().interrupt();
			return "Error: 发生异常: " + e.getMessage();
		} finally {
			try {
				if (mysqlConfigPath != null)
					Files.deleteIfExists(mysqlConfigPath);
				if (bigqueryConfigPath != null)
					Files.deleteIfExists(bigqueryConfigPath);
			} catch (IOException e) {
				System.err.println("临时文件清理失败: " + e.getMessage());
			}
		}
	}

	private String ensureBigQueryDataset(String datasetId) {
		try {
			// 检查 dataset 是否存在
			List<String> checkCommand = Arrays.asList(BQ_COMMAND_PATH, "show", "--project_id=" + projectId, datasetId);

			Process checkProcess = new ProcessBuilder(checkCommand).redirectErrorStream(true).start();

			BufferedReader reader = new BufferedReader(new InputStreamReader(checkProcess.getInputStream()));
			StringBuilder output = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				output.append(line).append("\n");
			}

			int exitCode = checkProcess.waitFor();

			if (exitCode == 0) {
				System.out.println("BigQuery Dataset 已存在: " + datasetId);
				return "Dataset 已存在";
			} else if (output.toString().contains("Not found") || output.toString().contains("404")) {
				System.out.println("BigQuery Dataset 不存在，正在创建与 MySQL Schema 同名的 dataset: " + datasetId);

				// 创建与 MySQL schema 同名的 dataset
				List<String> createCommand = Arrays.asList(BQ_COMMAND_PATH, "mk", "--project_id=" + projectId,
						"--location=" + region, "--description=Auto-created dataset for MySQL schema: " + datasetId,
						datasetId);

				Process createProcess = new ProcessBuilder(createCommand).redirectErrorStream(true).start();
				BufferedReader createReader = new BufferedReader(new InputStreamReader(createProcess.getInputStream()));
				StringBuilder createOutput = new StringBuilder();
				while ((line = createReader.readLine()) != null) {
					createOutput.append(line).append("\n");
				}

				int createExitCode = createProcess.waitFor();
				if (createExitCode == 0) {
					System.out.println("BigQuery Dataset 创建成功: " + datasetId + " (与 MySQL Schema 同名)");
					return "Dataset 创建成功";
				} else {
					System.err.println("BigQuery Dataset 创建失败: " + createOutput);
					return "Error: 创建失败: " + createOutput;
				}
			} else {
				System.err.println("检查 BigQuery dataset 失败: " + output);
				return "Error: 检查失败: " + output;
			}

		} catch (IOException | InterruptedException e) {
			Thread.currentThread().interrupt();
			return "Error: 检查/创建 dataset 异常: " + e.getMessage();
		}
	}

	/**
	 * 指定されたID文字列が、小文字の英字、数字、ハイフンのみで構成されているかをチェックします。
	 *
	 * @param id チェックするID文字列
	 * @return IDが有効な場合はtrue、それ以外の場合はfalse
	 */
	private boolean isValidId(String id) {
		// null または空文字列は無効とする
		if (id == null || id.isEmpty() || id.length() > 60) {
			return false;
		}

		// String.matches() メソッドは、文字列全体が正規表現にマッチするかをチェックします
		return id.matches(ID_REGEX);
	}

	public String validateConfiguration() {
		StringBuilder validation = new StringBuilder();
		validation.append("当前配置验证:\n");
		validation.append("项目 ID: ").append(projectId).append("\n");
		validation.append("区域: ").append(region).append("\n");
		validation.append("源 Profile: ").append(sourceProfileId).append("\n");
		validation.append("目标 Profile: ").append(destinationProfileId).append("\n");
		validation.append("Stream ID: ").append(datastreamProperties.getStreamId()).append("\n");
		validation.append("MySQL Schema: ").append(datastreamProperties.getMysqlSchemaName()).append("\n");
		validation.append("要同步的表: ").append(datastreamProperties.getTablesToSync()).append("\n");
		validation.append("BigQuery Dataset: 将创建与 MySQL Schema 同名的 dataset\n");
		validation.append("写入模式: Append-Only\n");
		validation.append("可用方法:\n");
		validation.append("- createDatastream(): 使用 sourceHierarchyDatasets 模式\n");
		validation.append("- createDatastreamWithSingleDataset(): 使用 singleTargetDataset 模式\n");

		return validation.toString();
	}
}