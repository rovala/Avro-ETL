package etl.ini;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import etl.utiles.constantes;
import etl.utiles.tiempo;
import etl.dao.EntornoBaseDatos;
import etl.dao.bigQuery;
import etl.dao.cloudStorage;
import etl.models.BotonPanico;
import etl.models.PersonalRrhh;


public class Empezar
{
	static bigQuery bq;
	static cloudStorage cs;
	
	public static void main(String[] args)
	{
		cs=new cloudStorage();
		bq=new bigQuery();
		String eleccion="";
		List <String> choose=new ArrayList<String>(Arrays.asList("1","2","3"));
		Scanner in=new Scanner(System.in);
		while (true)
		{
			do
			{
				System.out.print("Extraccion de informacion de BDs para carga en la nube por contexto\n");
				System.out.println("=================================================================");
				System.out.println("Menu de Ejecucion");		
				System.out.println("-----------------");
				System.out.println("Elija el numero correspondiente para su eleccion:");
				System.out.println("1. Boton de Panico");
				System.out.println("2. R.R.H.H.");
				System.out.println("3. Salir");
				eleccion=in.next();
			}
			while (!choose.contains(eleccion));
			
			switch (eleccion)
			{
				case "1":
					ejecutarBoton();
					break;
				case "2":
					ejecutarRRHH();
					break;
				case "3":
					in.close();
					System.exit(0);
					return;
			}
		
		}
	}
	
	public static void ejecutarBoton()
	{
		Connection cn;
		ResultSet rs;
		Statement st;
		StreamingCsvResultSetExtractor scrs;
		OutputStream os;
		EntornoBaseDatos entornoBaseDatos=new EntornoBaseDatos();
		String nameFileCsv;
		String uri;
		
		/**********************************/
		/************1º ETAPA: CONEXION ***/
		/**********************************/
		constantes ctemp=new constantes();
		BotonPanico bPanico=new BotonPanico();
		ctemp.setCLASS_BD("org.postgresql.Driver");
		entornoBaseDatos.prepararEnvironment(ctemp.getCLASS_BD());
		entornoBaseDatos.setConexion(bPanico.getCONEXION_POSTGRESQL(),bPanico.getUSER_BD_BOTON(),bPanico.getPASSWORD_BD_BOTON());
		cn=entornoBaseDatos.get_cn();
		if (cn==null) return;
		entornoBaseDatos.setStatement();
		st=entornoBaseDatos.get_st();
		if (st==null) return;

		/*********************************/
		/************2º ETAPA: ENTORNO ***/
		/*********************************/
		ctemp.setFILE_CONTEXTO("boton_panico");
		bq.dropTable(ctemp.getDATASET(),bPanico.getTABLA1_BOTON());
		bq.dropTable(ctemp.getDATASET(),bPanico.getTABLA2_BOTON());

		bq.createTable(ctemp.getDATASET(), bPanico.getTABLA1_BOTON(),bPanico.schemaTablaActivacion());
		bq.createTable(ctemp.getDATASET(), bPanico.getTABLA2_BOTON(),bPanico.schemaTablaMonitoreo());

		tiempo exec_time=new tiempo();
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
		
		try
		{
			nameFileCsv=ctemp.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
			os=new FileOutputStream(ctemp.SALIDA_CSV + nameFileCsv);
			scrs=new StreamingCsvResultSetExtractor(os);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +ctemp.SALIDA_CSV + ctemp.getFILE_CONTEXTO());
			return;
		}
				
		/*************************************/
		/************3º ETAPA: EXTRACCION  ***/
		/*************************************/
		System.out.println("Inicio....");
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		rs=entornoBaseDatos.leer_vista(bPanico.getSCRIPT_SQL_BOTON_ACTIVACION());
		if (rs==null) return;
		scrs.extractData(st,rs,0);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));

		/**************************************/
		/************4º ETAPA: CLOUDSTORAGE ***/
		/**************************************/
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						

		/**********************************/
		/************5º ETAPA: BIGQUERY ***/
		/**********************************/
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), bPanico.getTABLA1_BOTON());
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");

					
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
		try
		{
			nameFileCsv=ctemp.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
			os=new FileOutputStream(ctemp.SALIDA_CSV + nameFileCsv);
			scrs=new StreamingCsvResultSetExtractor(os);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +ctemp.SALIDA_CSV + ctemp.getFILE_CONTEXTO());
			return;
		}
					
		System.out.println("Inicio....");
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		rs=entornoBaseDatos.leer_vista(bPanico.getSCRIPT_SQL_BOTON_MONITOREO());
		if (rs==null) return;
		scrs.extractData(st,rs,1);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), bPanico.getTABLA2_BOTON());
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");
	}

	public static void ejecutarRRHH()
	{
		Connection cn;
		ResultSet rs;
		Statement st;
		StreamingCsvResultSetExtractor scrs;
		OutputStream os;
		EntornoBaseDatos entornoBaseDatos=new EntornoBaseDatos();
		String nameFileCsv;
		String uri;

		/**********************************/
		/************1º ETAPA: CONEXION ***/
		/**********************************/
		constantes ctemp=new constantes();
		PersonalRrhh personalRrhh =new PersonalRrhh();
		ctemp.setCLASS_BD("oracle.jdbc.driver.OracleDriver");
		entornoBaseDatos.prepararEnvironment(ctemp.getCLASS_BD());
		entornoBaseDatos.setConexion(personalRrhh.getCONEXION_ORACLE(),personalRrhh.getUSER_BD_RRHH(),personalRrhh.getPASSWORD_BD_RRHH());
		cn=entornoBaseDatos.get_cn();
		if (cn==null) return;
		entornoBaseDatos.setStatement();
		st=entornoBaseDatos.get_st();
		if (st==null) return;

		/*********************************/
		/************2º ETAPA: ENTORNO ***/
		/*********************************/
		ctemp.setFILE_CONTEXTO("rrhh_personal");
		bq.dropTable(ctemp.getDATASET(),personalRrhh.getTABLA1_RRHH());
		bq.dropTable(ctemp.getDATASET(),personalRrhh.getTABLA2_RRHH());
		bq.dropTable(ctemp.getDATASET(),personalRrhh.getTABLA3_RRHH());
		bq.dropTable(ctemp.getDATASET(),personalRrhh.getTABLA4_RRHH());
		bq.dropTable(ctemp.getDATASET(),personalRrhh.getTABLA5_RRHH());

		bq.createTable(ctemp.getDATASET(), personalRrhh.getTABLA1_RRHH(),personalRrhh.schemaTablaPersonal());
		bq.createTable(ctemp.getDATASET(), personalRrhh.getTABLA2_RRHH(),personalRrhh.schemaTablaInasistencia());
		bq.createTable(ctemp.getDATASET(), personalRrhh.getTABLA3_RRHH(),personalRrhh.schemaTablaVacaciones());
		bq.createTable(ctemp.getDATASET(), personalRrhh.getTABLA4_RRHH(),personalRrhh.schemaTablaLicSup());
		bq.createTable(ctemp.getDATASET(), personalRrhh.getTABLA5_RRHH(),personalRrhh.schemaTablaTardanza());
		
		tiempo exec_time=new tiempo();
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
		
		try
		{
			nameFileCsv=ctemp.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
			os=new FileOutputStream(ctemp.SALIDA_CSV + nameFileCsv);
			scrs=new StreamingCsvResultSetExtractor(os);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +ctemp.SALIDA_CSV + ctemp.getFILE_CONTEXTO());
			return;
		}
				
		/*************************************/
		/************3º ETAPA: EXTRACCION  ***/
		/*************************************/
		System.out.println("Inicio....");
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		rs=entornoBaseDatos.leer_vista(personalRrhh.getSCRIPT_SQL_RRHH_PERSONAL());
		if (rs==null) return;
		scrs.extractData(st,rs,0);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));

		/**************************************/
		/************4º ETAPA: CLOUDSTORAGE ***/
		/**************************************/
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						

		/**********************************/
		/************5º ETAPA: BIGQUERY ***/
		/**********************************/
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), personalRrhh.getTABLA1_RRHH());
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");
	

		/*******************2da TABLA */
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
		try
		{
			nameFileCsv=ctemp.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
			os=new FileOutputStream(ctemp.SALIDA_CSV + nameFileCsv);
			scrs=new StreamingCsvResultSetExtractor(os);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +ctemp.SALIDA_CSV + ctemp.getFILE_CONTEXTO());
			return;
		}
					
		System.out.println("Inicio....");
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		rs=entornoBaseDatos.leer_vista(personalRrhh.getSCRIPT_SQL_RRHH_INASISTENCIA());
		if (rs==null) return;
		scrs.extractData(st,rs,1);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), personalRrhh.getTABLA2_RRHH());
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");

		/*******************3ra TABLA */
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
		try
		{
			nameFileCsv=ctemp.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
			os=new FileOutputStream(ctemp.SALIDA_CSV + nameFileCsv);
			scrs=new StreamingCsvResultSetExtractor(os);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +ctemp.SALIDA_CSV + ctemp.getFILE_CONTEXTO());
			return;
		}
					
		System.out.println("Inicio....");
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		rs=entornoBaseDatos.leer_vista(personalRrhh.getSCRIPT_SQL_RRHH_VACACIONES());
		if (rs==null) return;
		scrs.extractData(st,rs,1);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), personalRrhh.getTABLA3_RRHH());
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");

		/*******************4ta TABLA */
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
		try
		{
			nameFileCsv=ctemp.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
			os=new FileOutputStream(ctemp.SALIDA_CSV + nameFileCsv);
			scrs=new StreamingCsvResultSetExtractor(os);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +ctemp.SALIDA_CSV + ctemp.getFILE_CONTEXTO());
			return;
		}
					
		System.out.println("Inicio....");
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		rs=entornoBaseDatos.leer_vista(personalRrhh.getSCRIPT_SQL_RRHH_LICSUP());
		if (rs==null) return;
		scrs.extractData(st,rs,1);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), personalRrhh.getTABLA4_RRHH());
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");

		/*******************5ta TABLA */
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
		try
		{
			nameFileCsv=ctemp.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
			os=new FileOutputStream(ctemp.SALIDA_CSV + nameFileCsv);
			scrs=new StreamingCsvResultSetExtractor(os);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +ctemp.SALIDA_CSV + ctemp.getFILE_CONTEXTO());
			return;
		}
					
		System.out.println("Inicio....");
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		rs=entornoBaseDatos.leer_vista(personalRrhh.getSCRIPT_SQL_RRHH_TARDANZA());
		if (rs==null) return;
		scrs.extractData(st,rs,1);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), personalRrhh.getTABLA5_RRHH());
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");

	}
	
}








