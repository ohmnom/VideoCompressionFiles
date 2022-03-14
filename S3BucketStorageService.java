package com.apple.shopify.aws.s3.service;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.apple.shopify.aws.s3.enums.FileMimeType;
import com.apple.shopify.s3.helper.S3BucketStorageHelper;

@Service
public class S3BucketStorageService {

	private Logger logger = LoggerFactory.getLogger(S3BucketStorageService.class);

	private static final String JPG_TYPE = "jpg";

	@Autowired
	private AmazonS3 amazonS3;

	@Value("${application.bucket.name}")
	private String bucketName;
	
	@Autowired
	private S3BucketStorageHelper helper;

	/**
	 * Upload file into AWS S3
	 *
	 * @param keyName
	 * @param file
	 * @return String
	 */
	public String uploadFile(String keyName, MultipartFile file) {
		try {
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(file.getSize());
			amazonS3.putObject(bucketName, keyName, file.getInputStream(), metadata);
			return "File uploaded: " + keyName;
		} catch (IOException ioe) {
			logger.error("IOException: " + ioe.getMessage());
		} catch (AmazonServiceException serviceException) {
			logger.info("AmazonServiceException: " + serviceException.getMessage());
			throw serviceException;
		} catch (AmazonClientException clientException) {
			logger.info("AmazonClientException Message: " + clientException.getMessage());
			throw clientException;
		}
		return "File not uploaded: " + keyName;
	}

	/**
	 * Deletes file from AWS S3 bucket
	 *
	 * @param fileName
	 * @return
	 */
	public String deleteFile(final String fileName) {
		amazonS3.deleteObject(bucketName, fileName);
		return "Deleted File: " + fileName;
	}

	/**
	 * Downloads file using amazon S3 client from S3 bucket
	 *
	 * @param keyName
	 * @return ByteArrayOutputStream
	 */
	public ByteArrayOutputStream downloadFile(String keyName) {
		try {
			S3Object s3object = amazonS3.getObject(new GetObjectRequest(bucketName, keyName));

			InputStream is = s3object.getObjectContent();
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			int len;
			byte[] buffer = new byte[4096];
			while ((len = is.read(buffer, 0, buffer.length)) != -1) {
				outputStream.write(buffer, 0, len);
			}

			return outputStream;
		} catch (IOException ioException) {
			logger.error("IOException: " + ioException.getMessage());
		} catch (AmazonServiceException serviceException) {
			logger.info("AmazonServiceException Message:    " + serviceException.getMessage());
			throw serviceException;
		} catch (AmazonClientException clientException) {
			logger.info("AmazonClientException Message: " + clientException.getMessage());
			throw clientException;
		}

		return null;
	}

	/**
	 * Get all files from S3 bucket
	 *
	 * @return
	 */
	public List<String> listFiles() throws IOException {

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);

		List<String> keys = new ArrayList<>();

		ObjectListing objects = amazonS3.listObjects(listObjectsRequest);

		while (true) {
			List<S3ObjectSummary> objectSummaries = objects.getObjectSummaries();
			if (objectSummaries.isEmpty()) {
				break;
			}

			for (S3ObjectSummary item : objectSummaries) {
				if (!item.getKey().endsWith("/")) {
					keys.add(item.getKey());
					S3Object s3object = amazonS3.getObject(new GetObjectRequest(bucketName, item.getKey()));
					unzipFolder(s3object);
				}
			}

			objects = amazonS3.listNextBatchOfObjects(objects);

		}

		return keys;
	}

	public void unzipFile(S3Object s3object) throws IOException {

		try (ZipInputStream zipInputStream = new ZipInputStream(s3object.getObjectContent())) {
			ZipEntry zipEntry = zipInputStream.getNextEntry();
			while (zipEntry != null) {

				ObjectMetadata metadata = new ObjectMetadata();
				
				S3Object s3object1 = amazonS3.getObject(new GetObjectRequest(bucketName, zipEntry.getName()));
				metadata.setContentLength(zipEntry.getSize());

				amazonS3.putObject(bucketName, "compressed/" + zipEntry.getName(), s3object1.getObjectContent(),
						metadata);

				zipEntry = zipInputStream.getNextEntry();
			}
			zipInputStream.closeEntry();
		}
	}

	public void unzipFolder(S3Object s3object) throws IOException {
		byte[] buffer = new byte[1024];
		S3Object s3Object = amazonS3.getObject(new GetObjectRequest(s3object.getBucketName(), s3object.getKey()));
		ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent());
		ZipEntry entry = zis.getNextEntry();

		while (entry != null) {
			if (!entry.getName().endsWith("/")) {
				String fileName = entry.getName();
				String mimeType = FileMimeType.fromExtension(FilenameUtils.getExtension(fileName)).mimeType();
				logger.info("Extracting " + fileName + ", compressed: " + entry.getCompressedSize()
						+ " bytes, extracted: " + entry.getSize() + " bytes, mimetype: " + mimeType);
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				int len;
				while ((len = zis.read(buffer)) > 0) {
					outputStream.write(buffer, 0, len);
				}
				InputStream is = new ByteArrayInputStream(outputStream.toByteArray());
				ObjectMetadata meta = new ObjectMetadata();
				meta.setContentLength(outputStream.size());
				meta.setContentType(mimeType);
				compress(s3Object, entry, is, outputStream);
				is.close();
				outputStream.close();
			}
			entry = zis.getNextEntry();
		}
		zis.closeEntry();
		zis.close();

	}

	public String compress(S3Object s3Object1, final ZipEntry entry, final InputStream is, ByteArrayOutputStream outputStream) throws IOException {

		ByteArrayOutputStream os = null;
		ImageOutputStream ios = null;
		ByteArrayInputStream is10 = null;
		ByteArrayOutputStream os10 = null;
		InputStream is20 = null;
		ObjectMetadata meta = null;
		
		try {
			BufferedImage srcImage = ImageIO.read(is);
			final String extension = FilenameUtils.getExtension(entry.getName());
			if (!"mp4".equals(extension)) {
				Iterator<ImageWriter> writers = ImageIO
						.getImageWritersByFormatName("tif".equalsIgnoreCase(extension) ? JPG_TYPE : extension);
				if (!writers.hasNext())
					throw new IllegalStateException("No writers found");
				ImageWriter writer = writers.next();
				os = new ByteArrayOutputStream();
				ios = ImageIO.createImageOutputStream(os);
				writer.setOutput(ios);
				ImageWriteParam param = writer.getDefaultWriteParam();
				// compress to a given quality
				param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
				param.setCompressionQuality(0.5f);
				// appends a complete image stream containing a single image and
				// associated stream and image metadata and thumbnails to the output
				writer.write(null, new IIOImage(srcImage, null, null), param);
				logger.info("file is compressed");

				is10 = new ByteArrayInputStream(os.toByteArray());

				BufferedImage destImage = ImageIO.read(is10);

				os10 = new ByteArrayOutputStream();
				ImageIO.write(destImage, JPG_TYPE, os10);
				is20 = new ByteArrayInputStream(os10.toByteArray());
				// Set Content-Length and Content-Type
			    meta = new ObjectMetadata();
				meta.setContentLength(os10.size());
				logger.info("File size: " +os10.size());
				String mimeType = FileMimeType.fromExtension(FilenameUtils.getExtension(entry.getName())).mimeType();

				meta.setContentType(mimeType);
				
				logger.info("Writing to: " + "resized" + "/" + s3Object1.getKey());
				amazonS3.putObject(s3Object1.getBucketName(), "resized/" + entry.getName(), is20, meta);
				logger.info("Successfully resized " + s3Object1.getBucketName() + "/" + s3Object1.getBucketName()
						+ " and uploaded to " + "resized" + "/" + s3Object1.getKey());
			}
			else {
				helper.compressMp4Video(s3Object1, entry,outputStream);
			}
			
		} catch (Exception e) {
			logger.info("error:  ", e);
		} finally {
			if(is10 != null) 
				is10.close();
			if(os10 != null)
				os10.close();
			if(is20 != null)
				is20.close();
			if(os != null)
				os.close();
			if(ios != null)
				ios.close();
		}

		return "Ok";
	}

	public static Path zipSlipVulnerabilityProtect(ZipEntry zipEntry, Path targetDir) throws IOException {

		/**
		 * resolve(String other) method of java. nio. file.Path used to converts a given
		 * path string to a Path and resolves it against this Path in the exact same
		 * manner as specified by the resolve method
		 */
		Path dirResolved = targetDir.resolve(zipEntry.getName());

		/**
		 * Normalizing a path involves modifying the string that identifies a path or
		 * file so that it conforms to a valid path on the target operating system.
		 */
		// normalize the path on target directory or else throw exception
		Path normalizePath = dirResolved.normalize();
		if (!normalizePath.startsWith(targetDir)) {
			throw new IOException("Invalid zip: " + zipEntry.getName());
		}

		return normalizePath;
	}

}
