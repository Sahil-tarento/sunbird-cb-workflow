package org.sunbird.workflow.service.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;
import org.sunbird.workflow.config.Configuration;
import org.sunbird.workflow.config.Constants;
import org.sunbird.workflow.models.SBApiResponse;
import org.sunbird.workflow.service.StorageService;
import org.sunbird.workflow.utils.ProjectUtil;

import scala.Option;

@Service
public class StorageServiceImpl implements StorageService {

    private Logger logger = LoggerFactory.getLogger(getClass().getName());
    private BaseStorageService storageService = null;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    private Configuration configuration;

    @PostConstruct
    public void init() {
        if (storageService == null) {
            storageService = StorageServiceFactory.getStorageService(new StorageConfig(
                    configuration.getCloudStorageTypeName(), configuration.getCloudStorageKey(),
                    configuration.getCloudStorageSecret().replace("\\n", "\n"),
                    Option.apply(configuration.getCloudStorageEndpoint()),
                    Option.empty()));
        }
    }

    public SBApiResponse uploadFile(MultipartFile mFile, String cloudFolderName, String containerName)
            throws IOException {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_FILE_UPLOAD);
        File file = null;
        try {
            file = new File(System.currentTimeMillis() + "_" + mFile.getOriginalFilename());
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(mFile.getBytes());
            fos.close();
            return uploadFile(file, cloudFolderName, containerName);
        } catch (Exception e) {
            logger.error("Failed to Upload File Exception", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to upload file. Exception: " + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        } finally {
            if (file != null) {
                file.delete();
            }
        }
    }

    @Override
    public SBApiResponse downloadFile(String fileName) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_FILE_DOWNLOAD);
        try {
            String objectKey = configuration.getUserBulkUpdateFolderName() + "/" + fileName;
            storageService.download(configuration.getWorkflowCloudContainerName(), objectKey, Constants.LOCAL_BASE_PATH,
                    Option.apply(Boolean.FALSE));
            return response;
        } catch (Exception e) {
            logger.error("Failed to Download File" + fileName + ", Exception : ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to download the file. Exception: " + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    public SBApiResponse uploadFile(File file, String cloudFolderName, String containerName) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_FILE_UPLOAD);
        try {
            String objectKey = cloudFolderName + "/" + file.getName();
            String url = storageService.upload(containerName, file.getAbsolutePath(),
                    objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty());
            Map<String, String> uploadedFile = new HashMap<>();
            uploadedFile.put("name", file.getName());
            uploadedFile.put("url", url);
            response.getResult().putAll(uploadedFile);
            return response;
        } catch (Exception e) {
            logger.error("Failed tp upload file", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to upload file. Exception: " + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        } finally {
            if (file != null) {
                file.delete();
            }
        }
    }

    protected void finalize() {
        try {
            if (storageService != null) {
                storageService.closeContext();
                storageService = null;
            }
        } catch (Exception e) {
        }
    }

}
