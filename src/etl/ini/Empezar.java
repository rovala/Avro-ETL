package etl.ini;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import javax.swing.JOptionPane;

import etl.utiles.constantes;
import etl.utiles.tiempo;
import etl.dao.DatabasePostgreSQL;
import etl.dao.bigQuery;
import etl.dao.cloudStorage;
import etl.models.BotonPanico;


public class Empezar
{
	static Connection cn;
	static ResultSet rs;
	static Statement st;
	static StreamingCsvResultSetExtractor scrs;
	static OutputStream os;
	static DatabasePostgreSQL dbPSql=new DatabasePostgreSQL();
	static String nameFileCsv;
	static cloudStorage cs;
	static bigQuery bq;
	static String uri;
	
	
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
				System.out.print("Extraccion de informacion de BDs para carga en la nuve por contexto\r");
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
		constantes ctemp=new constantes();
		ctemp.setFILE_CONTEXTO("boton_panico");
		bq.dropTable(ctemp.getDATASET(),ctemp.TABLA1_BOTON);
		bq.dropTable(ctemp.getDATASET(),ctemp.TABLA2_BOTON);

		BotonPanico bPanico=new BotonPanico();
		bq.createTable(ctemp.getDATASET(), ctemp.TABLA1_BOTON,bPanico.schemaTablaActivacion());
		bq.createTable(ctemp.getDATASET(), ctemp.TABLA2_BOTON,bPanico.schemaTablaMonitoreo());

		tiempo exec_time=new tiempo();
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
			
		try
		{
			ctemp.setFILE_CONTEXTO("boton_panico");
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
		cn=dbPSql.get_cn_boton();
		if (cn==null) return;
		crearStatement();
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		if (leer_vista(ctemp.getSCRIPT_SQL_ACTIVACION())==false) return;
						
		scrs.extractData(st,rs);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), ctemp.TABLA1_BOTON);
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");

					
		exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
		try
		{
			ctemp.setFILE_CONTEXTO("boton_panico");
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
		if (cn==null) return;
		System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
		if (leer_vista(ctemp.getSCRIPT_SQL_MONITOREO())==false) return;
		scrs.extractData(st,rs);
		System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		cs.to_cloudStorage(ctemp.SALIDA_CSV + nameFileCsv, ctemp.getBUCKET_NAME(), nameFileCsv);
		System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
		uri=bq.getUri(nameFileCsv,ctemp.getBUCKET_NAME());
		System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		bq.to_bigQuery(uri, ctemp.getDATASET(), ctemp.TABLA2_BOTON);
		System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
		System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");
	}

	public static boolean leer_vista(String sql)
	{
		try
		{
			rs = st.executeQuery(sql);
			return true;
		}
		catch (SQLException e)
		{
			JOptionPane.showMessageDialog(null, "POSTGRESQL " + e.getMessage());
			return false;
		}
	}
	
	public static boolean crearStatement()
	{
		try
		{
			st = cn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			return true;
		}
		catch (SQLException e)
		{
			JOptionPane.showMessageDialog(null, "POSTGRESQL " + e.getMessage());
			return false;
		}
	}
}








