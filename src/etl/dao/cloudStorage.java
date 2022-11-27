package etl.dao;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;



public class cloudStorage {
	private Storage stcs;
	public cloudStorage ()
	{
		conexionCS();
	}
		
	public void to_cloudStorage(String csvFile, String bucketName, String objectCsv)
	{
		BlobId blobId = BlobId.of(bucketName, objectCsv);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		try
		{
			stcs.create(blobInfo, Files.readAllBytes(Paths.get(csvFile)));
		}
		catch (IOException e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void conexionCS()
	{
		try
		{
			this.stcs=StorageOptions.getDefaultInstance().getService();
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
}
