package etl.dao;


import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import etl.models.CloudStorageBigqueryTx;
import etl.utiles.constantes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


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
			this.stcs.create(blobInfo, Files.readAllBytes(Paths.get(csvFile)));
		}
		catch (IOException e)
		{
			System.out.println("Error en almacenamiento GCP CloudStorage: "+ e.getMessage());
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
			System.out.println("No fue posible conectar a GCP CloudStorage: " + e.getMessage());
		}
	}

	public void almacenCloudStorage(List<CloudStorageBigqueryTx> arrayCloudStorageBigqueryTx)
	{
		constantes ctemp=new constantes();

		arrayCloudStorageBigqueryTx.stream().forEach(x->{
			this.to_cloudStorage(ctemp.SALIDA_CSV+x.getCsv(),ctemp.getBUCKET_NAME(), x.getCsv());
		});
	}
}
