package com.apple.shopify.s3.helper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.ZipEntry;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.apple.shopify.aws.s3.enums.FileMimeType;
import com.apple.shopify.aws.s3.service.S3BucketStorageService;

import io.github.techgnious.IVCompressor;
import io.github.techgnious.dto.IVSize;
import io.github.techgnious.dto.ResizeResolution;
import io.github.techgnious.dto.VideoFormats;
import io.github.techgnious.exception.VideoException;

@Component
public class S3BucketStorageHelper {
	
	@Autowired
	private AmazonS3 amazonS3; 
	
	@Autowired
	private S3BucketStorageService service;
	
	private Logger logger = LoggerFactory.getLogger(service.getClass());

	public void compressMp4Video(S3Object s3Object1, ZipEntry entry, ByteArrayOutputStream outputStream) throws FileNotFoundException, IOException, VideoException {
		IVCompressor compressor = new IVCompressor();
	    IVSize customRes = new IVSize();
	    customRes.setWidth(400);
	    customRes.setHeight(300);
	    ObjectMetadata meta = new ObjectMetadata();
		

	    byte[] compressedVid = compressor.reduceVideoSize(outputStream.toByteArray(), VideoFormats.MP4, ResizeResolution.R480P);
	    meta.setContentLength(compressedVid.length);
	    logger.info("Video Length: "+compressedVid.length);
		String mimeType = FileMimeType.fromExtension(FilenameUtils.getExtension(entry.getName())).mimeType();

		meta.setContentType(mimeType);

	    amazonS3.putObject(s3Object1.getBucketName(), "resized/" + entry.getName(), new ByteArrayInputStream(compressedVid), meta);

		
	}
	
	

}
