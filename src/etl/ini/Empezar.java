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
		/*CAMBIO 1*/
		cs=new cloudStorage();
		bq=new bigQuery();
		String eleccion="";
		List <String> choose=new ArrayList<String>(Arrays.asList("1","2","3","4"));
		Scanner in=new Scanner(System.in);
		while (true)
		{
			do
			{
				System.out.print("Extraccion de informacion de BDs para carga en la nube\r");
				System.out.println("======================================================");
				System.out.println("Menu de Ejecucion");		
				System.out.println("-----------------");
				System.out.println("Elija el numero correspondiente para su eleccion:");
				System.out.println("1. Borrar tablas bigquery");
				System.out.println("2. Crear tablas bigquery");
				System.out.println("3. Extraccion ETL de datos");
				System.out.println("4. Salir");
				eleccion=in.next();
			}
			while (!choose.contains(eleccion));
			
			switch (eleccion)
			{
				case "1":
					constantes ctemp=new constantes();
					ctemp.setFILE_CONTEXTO("boton_panico");
					bq.dropTable(ctemp.getDATASET(),ctemp.TABLA1_BOTON);
					bq.dropTable(ctemp.getDATASET(),ctemp.TABLA2_BOTON);
					ctemp=null;
					System.gc();
					break;
				case "2":
					constantes ctemp1=new constantes();
					BotonPanico bPanico=new BotonPanico();
					ctemp1.setFILE_CONTEXTO("boton_panico");
					bq.createTable(ctemp1.getDATASET(), ctemp1.TABLA1_BOTON,bPanico.schemaTablaActivacion());
					bq.createTable(ctemp1.getDATASET(), ctemp1.TABLA2_BOTON,bPanico.schemaTablaMonitoreo());
					ctemp1=null;
					bPanico=null;
					System.gc();
					break;
				case "3":
					tiempo exec_time=new tiempo();
					constantes constante=new constantes();
					exec_time.setFormatDateTime(constante.FORMATO_DATE_TIME);
					
					try
					{
						constante.setFILE_CONTEXTO("boton_panico");
						nameFileCsv=constante.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
						os=new FileOutputStream(constante.SALIDA_CSV + nameFileCsv);
						scrs=new StreamingCsvResultSetExtractor(os);
						
					}
					catch (FileNotFoundException e)
					{
						System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +constante.SALIDA_CSV + constante.getFILE_CONTEXTO());
						return;
					}
					
					System.out.println("Inicio....");
					cn=dbPSql.get_cn_boton();
					if (cn==null) return;
					//try
					//{
						crearStatement();
						System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
						if (leer_vista(constante.getSCRIPT_SQL_ACTIVACION())==false) return;
						
						scrs.extractData(st,rs);
						System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						cs.to_cloudStorage(constante.SALIDA_CSV + nameFileCsv, constante.getBUCKET_NAME(), nameFileCsv);
						System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
						uri=bq.getUri(nameFileCsv,constante.getBUCKET_NAME());
						System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						bq.to_bigQuery(uri, constante.getDATASET(), constante.TABLA1_BOTON);
						System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
						//rs.close();
						//cn.close();
						//st.close();
						
						System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");
					//}
					//catch (SQLException e)
					//{
						//JOptionPane.showMessageDialog(null, "POSTGRESQL " + e.getMessage());
					//}
					
					
					exec_time.setFormatDateTime(constante.FORMATO_DATE_TIME);
					try
					{
						constante.setFILE_CONTEXTO("boton_panico");
						nameFileCsv=constante.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") +".csv";
						os=new FileOutputStream(constante.SALIDA_CSV + nameFileCsv);
						scrs=new StreamingCsvResultSetExtractor(os);
						
					}
					catch (FileNotFoundException e)
					{
						System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +constante.SALIDA_CSV + constante.getFILE_CONTEXTO());
						return;
					}
					
					System.out.println("Inicio....");
					//cn=dbPSql.get_cn_boton();
					if (cn==null) return;
					//try
					//{
						System.out.println("Intentando ejecutar vista...."+exec_time.getPrimitiveDateTime().replace("T"," "));
						if (leer_vista(constante.getSCRIPT_SQL_MONITOREO())==false) return;
						
						scrs.extractData(st,rs);
						System.out.println("Termino de extraccion...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						System.out.println("\nInicio cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						cs.to_cloudStorage(constante.SALIDA_CSV + nameFileCsv, constante.getBUCKET_NAME(), nameFileCsv);
						System.out.println("Fin cloudstorage...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
						uri=bq.getUri(nameFileCsv,constante.getBUCKET_NAME());
						System.out.println("\nInicio carga a bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						bq.to_bigQuery(uri, constante.getDATASET(), constante.TABLA2_BOTON);
						System.out.println("Fin carga Bigquery...." + exec_time.getPrimitiveDateTime().replace("T"," "));
						
						//rs.close();
						//cn.close();
						
						System.out.println("Todo OK Cerrando...." + exec_time.getPrimitiveDateTime().replace("T"," ")+"\n");
					//}
					//catch (SQLException e)
					//{
						//JOptionPane.showMessageDialog(null, "POSTGRESQL " + e.getMessage());
					//}
					break;
				case "4":
					in.close();
					System.exit(0);
					return;
			}
		
		}


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








