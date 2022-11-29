package etl.dao;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import etl.utiles.constantes; 

public class EntornoBaseDatos {
	constantes k=new constantes();
	private Connection cn;
	private Statement st;
	
	public EntornoBaseDatos()
	{
		
	}
	
	public boolean prepararEnvironment(String classBD)
	{
		try
		{
			try
			{
				Class.forName(classBD).getDeclaredConstructor().newInstance();
				return true;
			} 
			catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
			{
				System.out.println("Error de creacion de clase " + e.getMessage());
				return false;
			}
		}
		catch (IllegalArgumentException | InvocationTargetException | NoSuchMethodException	| SecurityException e)
		{

			System.out.println("Error de creacion de clase " + e.getMessage());
			return false;
		}
	}
	
	public boolean setConexion(String cadena_conexion,String usuario,String password)
	{
		try
		{
			this.cn=DriverManager.getConnection(cadena_conexion,usuario,password);
			return true;
		}
		catch (SQLException e)
		{
			System.out.println("Error de creacion de conexion " +e.getMessage());
			return false;
		}
	}
	
	public Connection get_cn()
	{
		return this.cn; 
	}

	public boolean setStatement()
	{
		try
		{
			this.st=this.cn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			return true;
		}
		catch (SQLException e)
		{
			System.out.println("Error de creacion Objeto statement " + e.getMessage());
			return false;
		}
	}

	public Statement get_st()
	{
		return st;
	}

	public ResultSet leer_vista(String sql,Statement st,ResultSet rs)
	{
		try
		{
			rs = st.executeQuery(sql);
			return rs;
		}
		catch (SQLException e)
		{
			System.out.println("Error de consulta " + e.getMessage());
			return null;
		}
	}
}
