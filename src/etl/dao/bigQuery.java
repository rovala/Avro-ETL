package etl.dao;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;


public class bigQuery
{
	private BigQuery bq;
	public bigQuery()
	{
		conexionBQ();
	}
	
	public void to_bigQuery(String uri,String datasetName,String tableName)
	{
		CsvOptions csvOptions = CsvOptions.newBuilder().setAllowQuotedNewLines(true).setSkipLeadingRows(1).setFieldDelimiter("|").setQuote("\"").setEncoding("ISO-8859-1").build();

	    TableId tableId = TableId.of(datasetName, tableName);
	    LoadJobConfiguration loadConfig = LoadJobConfiguration.newBuilder(tableId,uri,csvOptions).setAutodetect(false).build();
	    // Load data from a GCS CSV file into the table
	    
		Job job = bq.create(JobInfo.of(loadConfig));
		
				
		// Blocks until this load table job completes its execution, either failing or succeeding.
		try
		{
			job = job.waitFor();
		}
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		if (job.isDone())
		{
			System.out.println("CSV from GCS successfully added during load append job");
		}
		else
		{
			System.out.println("BigQuery was unable to load into the table due to an error:" + job.getStatus().getError());
		}
	}
	
	public void conexionBQ()
	{
		try
		{
			this.bq=BigQueryOptions.getDefaultInstance().getService();
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	public String getUri(String nameFileCsv,String bucketName)
	{
		return "gs://"+ bucketName + "/" + nameFileCsv;
	}
	
	public void createTable(String datasetName, String tableName, Schema schema)
	{
		try
		{
			TableId tableId = TableId.of(datasetName, tableName);
			TableDefinition tableDefinition = StandardTableDefinition.of(schema);
			TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
			this.bq.create(tableInfo);
			System.out.println("Tabla: " + tableName + " creada exitosamente en el dataset: " + datasetName);
		}
		catch (BigQueryException e)
		{
			System.out.println("Tabla no puedo ser creada. \n" + e.toString());
		}
	}
	
	public void dropTable(String datasetName, String tableName)
	{
	    try
	    {
	      TableId tableId = TableId.of(datasetName, tableName);
	      bq.delete(tableId);
	      System.out.println("Tabla: "+tableName+ " eliminada exitosamente en el dataset: " + datasetName);
	    }
	    catch (BigQueryException e)
	    {
	    	System.out.println("Tabla no puedo ser eliminada. \n" + e.toString());
	    }
	}
}
