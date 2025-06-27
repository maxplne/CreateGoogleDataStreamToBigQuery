package com.example.datastreamcreator.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.datastreamcreator.service.DatastreamService;

@RestController
public class DatastreamController {

	@Autowired
	private DatastreamService datastreamService;

	@GetMapping("/create-datastream")

	public ResponseEntity<?> createDatastreamGet(@RequestParam("source-profile-id") String sourceProfileId,
			@RequestParam("stream-id") String streamId, @RequestParam("mysql-schema-name") String mysqlSchemaName) {
		try {
// 模拟处理过程
			Thread.sleep(2000); // 模拟执行时间

			String logMessage = String.format(
					"数据流创建成功!\n" + "源配置ID: %s\n" + "Stream ID: %s\n" + "MySQL Schema: %s\n" + "执行时间: %s\n" + "状态: 成功",
					sourceProfileId, streamId, mysqlSchemaName, new java.util.Date().toString());

			System.out.println("=== 单个Schema执行 ===");
			System.out.println(logMessage);
			System.out.println("等待结果");
			System.out.println(datastreamService.createDatastream( sourceProfileId, streamId,  mysqlSchemaName));
			System.out.println("===================");

			return ResponseEntity.ok(logMessage);
		} catch (Exception e) {
			String errorMessage = String.format(
					"数据流创建失败!\n" + "错误信息: %s\n" + "源配置ID: %s\n" + "Stream ID: %s\n" + "MySQL Schema: %s",
					e.getMessage(), sourceProfileId, streamId, mysqlSchemaName);

			return ResponseEntity.badRequest().body(errorMessage);
		}
	}

	@GetMapping("/vaildata")
	public String vaildata() {
		return datastreamService.validateConfiguration();
	}
}