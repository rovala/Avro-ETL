package etl.ini;

import etl.dao.bigQuery;
import etl.dao.cloudStorage;
import etl.models.CloudStorageBigqueryTx;
import etl.models.DefinicionEntornos;
import etl.models.DefinicionExtraccion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;



public class Empezar
{
	static bigQuery bq;
	static cloudStorage cs;
	
	public static void main(String[] args)
	{
		/****INICIO DE LOS SERVICIOS CLOUD****/
		cs=new cloudStorage();
		bq=new bigQuery();
		/******SERVICIOS CLOUD INICIADOS******/
		List<String> arrayEntornos=Arrays.asList("boton_panico","rrhh_personal");
		DefinicionEntornos definicionEntornos=new DefinicionEntornos(arrayEntornos,bq);
		System.out.println("Iniciando entorno......");
		definicionEntornos.seteoEntornos();
		System.out.println("Terminando entorno......");

		DefinicionExtraccion definicionExtraccion=new DefinicionExtraccion(arrayEntornos);
		System.out.println("Iniciando extraccion......");
		List<CloudStorageBigqueryTx> arrayCloudStorageBigqueryTx=new ArrayList<>(definicionExtraccion.startExtraccion());
		System.out.println("Cantidad de archivos: " + arrayCloudStorageBigqueryTx.size());
		arrayCloudStorageBigqueryTx.stream().forEach(x->System.out.println("Cantidad de archivos: " + x.getCsv()));

		System.out.println("Terminando extraccion......");

		System.out.println("Iniciando CloudStorage......");
		cs.almacenCloudStorage(arrayCloudStorageBigqueryTx);
		System.out.println("Terminando CloudStorage......");

		System.out.println("Iniciando Bigquery......");
		bq.almacenBigquery(arrayCloudStorageBigqueryTx);
		System.out.println("Terminando Bigquery......");

	}

}