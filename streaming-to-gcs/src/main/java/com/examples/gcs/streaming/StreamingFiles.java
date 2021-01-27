package com.examples.gcs.streaming;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;

public class StreamingFiles {

	GoogleCredentials credentials;
	Storage storage;
	
	public void authenticate() {
		try {
			credentials = GoogleCredentials.fromStream(new FileInputStream("D:\\1Workspace\\GoogleCloud\\google-cloud\\streaming-to-gcs\\src\\main\\resources\\service-account-key.json"))
			        .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
		} catch (IOException e) {
			System.out.println("Exception when authenticating: " + e.getMessage());
		}
		storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
		System.out.println("Buckets:");
		Page<Bucket> buckets = storage.list();
		for (Bucket bucket : buckets.iterateAll()) {
		    System.out.println(bucket.toString());
		}		
	}

    public void uploadNow(String bucketName, String sourceFile, String blobName) {

    	BlobId blobId = BlobId.of(bucketName, blobName);
			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
			
	          try (WriteChannel writer = storage.writer(blobInfo)) {
	            byte[] buffer = new byte[1 * 1024 * 1024];
	            try (InputStream input = Files.newInputStream(Paths.get(sourceFile))) {
	              int limit;
	              while ((limit = input.read(buffer)) >= 0) {
	                try {
	                  writer.write(ByteBuffer.wrap(buffer, 0, limit));
	                } catch (Exception ex) {
	                  ex.printStackTrace();
	                }
	              }
	            } catch(Exception e) {
	            	System.out.println("Exception 1 "+e.getMessage());
	            	e.printStackTrace();
	            }
	          } catch (Exception e) {
	            	System.out.println("Exception 2 "+e.getMessage());
	            	e.printStackTrace();
	          }
	        System.out.println("Blob was created");
      }
	
	public void uploadFile(String bucketName, String sourceFile, String blobName) {

		//writer.setChunkSize(2*1024*1024);
		
		long startTime = (new Date()).getTime();
		try {
			Spliterator spliterator = Files.lines(Paths.get(sourceFile)).spliterator();
			Spliterator split1 = spliterator.trySplit();
			Spliterator split2 = spliterator.trySplit();
			Spliterator split3 = spliterator.trySplit();
			Spliterator split4 = spliterator.trySplit();
			List spliteratorList = List.of(split1, split2, split3, split4);
			
			int partNumber = 0;
			
			spliteratorList.parallelStream().forEach(ctr -> {
					BlobId blobId = BlobId.of(bucketName, blobName);
					BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
					WriteChannel writer = storage.writer(blobInfo);
	
					byte[] content = ((String)ctr).getBytes(StandardCharsets.UTF_8);
					try {
						writer.write(ByteBuffer.wrap(content, 0, content.length));
						writer.close();
					} catch (IOException ex) {
					  System.out.println("Exception when uploading: " + ex.getMessage());
					}
			});
			
			long endTime = (new Date()).getTime();
			
			System.out.println("Time taken -> " + (endTime - startTime));
		} catch(Exception e) {
			
		}
		
	}
	
	public void downloadFile(String bucketName, String blobName) {
		try (ReadChannel reader = storage.reader(bucketName, blobName)) {
		  ByteBuffer bytes = ByteBuffer.allocate(256 * 1024);
		  Path path = Paths.get("d://aes-downloaded.csv");
			try(BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"))){
				while (reader.read(bytes) > 0) {
					writer.write(new String(bytes.array()));
					bytes.clear();
			    }
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
	}
	
	public void storeDetails() {
		
	}
	
}
