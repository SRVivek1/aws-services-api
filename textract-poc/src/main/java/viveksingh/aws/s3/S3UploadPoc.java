/**
 * 
 */
package viveksingh.aws.s3;

import java.io.IOException;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * @author vivek
 *
 *         This application connects to AWS S3 using IAM credentials stored in
 *         .aws/credentials file.
 * 
 */
public class S3UploadPoc {

	/** S3 Bucket name. */
	private static final String BUCKET_NAME = "api-poc-s3-bucket";
	
	/** Key to store the content. */
	private static final String key = "test-key-1";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

		// Build S3 client.
		final Region region = Region.AP_SOUTH_1;
		S3Client s3Client = getS3Client(region);
		

		// Create S3 bucket.
		createS3Bucket(s3Client, BUCKET_NAME, region);

		
		System.out.println("**********Uploading object...");

		s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(key).build(),
				RequestBody.fromString("Testing with the {sdk-java}"));

		System.out.println("**********Upload completed.");
		System.out.printf("%n");

		// Remove Bukcet and key
		cleanUp(s3Client, BUCKET_NAME, key);

		System.out.println("Closing the connection to {S3}");
		s3Client.close();
		System.out.println("Connection closed");
		System.out.println("Exiting...");
	}

	/**
	 * Construct S3 client for provided region.
	 * 
	 * @param region
	 * @return
	 */
	private static S3Client getS3Client(Region region) {
		return S3Client.builder().region(region).build();

	}

	/**
	 * Create S3 bucket with provided name and region.
	 * 
	 * @param s3Client
	 * @param bucketName
	 * @param region
	 */
	public static void createS3Bucket(S3Client s3Client, String bucketName, Region region) {
		try {
			
			// Create S3 bucket
			s3Client.createBucket(
					CreateBucketRequest.builder().bucket(bucketName)
							.createBucketConfiguration(
									CreateBucketConfiguration.builder().locationConstraint(region.id()).build())
							.build());
			System.out.println("************Creating bucket: " + bucketName);
			
			// Wait till bucket is created
			s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build());
			
			System.out.println("************" + bucketName + " is ready.\n");
		} catch (S3Exception e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}

	/**
	 * Remove S3 bucket and all stored keys.
	 * 
	 * @param s3Client
	 * @param bucketName
	 * @param keyName
	 */
	public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
		System.out.println("Cleaning up...");
		
		try {
			System.out.println("Deleting object: " + keyName);
			DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName)
					.build();
			s3Client.deleteObject(deleteObjectRequest);
			System.out.println(keyName + " has been deleted.");
			
			System.out.println("Deleting bucket: " + bucketName);
			DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
			s3Client.deleteBucket(deleteBucketRequest);
			System.out.println(bucketName + " has been deleted.\n");
		} catch (S3Exception e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		System.out.println("Cleanup complete");
		System.out.printf("%n");
	}

}
