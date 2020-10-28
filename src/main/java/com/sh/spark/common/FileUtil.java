/*******************************************************************************
 * Copyright � 2018 Impetus. All rights reserved. 
 * This software contains confidential information and trade secrets of Impetus. Use, disclosure, or reproduction is prohibited without the prior written consent of Impetus.
 *******************************************************************************/
package com.sh.spark.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileUtil.class);

	public static boolean writeFileToPath(String filePath, String content) throws Exception {
		boolean status = false;
		try {
			LOGGER.debug("##############\nWriting " + " filePath :" + filePath + "\n######################");
			if (StringUtils.isNotEmpty(filePath)) {
				FileUtils.writeByteArrayToFile(new File(filePath), content.getBytes());
				status = true;
			}
		} catch (IOException e) {
			LOGGER.error("unable to write file " + filePath + " on the filesystem", e);
			throw new Exception("unable to write file " + filePath + " on the filesystem", e);
		}
		return status;
	}

	public static String readFilefromPath(String filePath) throws IOException {
		byte[] content = null;
		try {
			File file = new File(filePath);
			FileInputStream inputStream = new FileInputStream(file);
			content = IOUtils.toByteArray(inputStream);
		} catch (FileNotFoundException e) {
			LOGGER.error("## Error in reading file :" + filePath, e);
		} catch (IOException e) {
			LOGGER.error("## Error in reading file :" + filePath, e);
		}
		return new String(content);
	}

	public static boolean createDirectory(String dirPath) {
		boolean status = false;
		try {
			if (StringUtils.isNotEmpty(dirPath)) {
				FileUtils.forceMkdir(new File(dirPath));
				status = true;
			}
		} catch (IOException e) {
			LOGGER.error("Not able to create  directory path  " + dirPath, e);

		}
		return status;
	}

	public static void removeDirectory(String directoryPath) {
		try {
			if (StringUtils.isNotEmpty(directoryPath)) {
				FileUtils.cleanDirectory(new File(directoryPath));
				FileUtils.deleteDirectory(new File(directoryPath));
				
			}

		} catch (IOException e) {
			LOGGER.error("Not able to clean directory  " + directoryPath, e);

		}
	}

	
	public static void copyDirectory(String inputDir,String outputDir) {
		try {
			if (StringUtils.isNotEmpty(inputDir) && StringUtils.isNotEmpty(outputDir)) {
				
				FileUtils.copyDirectory(new File(inputDir),new File(outputDir));
				
			}

		} catch (IOException e) {
			LOGGER.error("Copy  " + inputDir, e);

		}
	}

}
