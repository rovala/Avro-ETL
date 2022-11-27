package etl.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import etl.utiles.constantes; 

public class DatabasePostgreSQL {
	constantes k=new constantes();
	Connection cn;
	
	public DatabasePostgreSQL()
	{
		this.prepararEnvironment();
		this.getConexion_boton();
	}
	
	private boolean prepararEnvironment()
	{
		try
		{
			Class.forName(k.CLASS_POSTGRESQL).newInstance();
			return true;
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
		{
			System.out.println(e.getMessage());
			return false;
		} 
		
	}
	
	private boolean getConexion_boton()
	{
		try
		{
			cn=DriverManager.getConnection(k.CONEXION_POSTGRESQL,k.USER_BD_BOTON,k.PASSWORD_BD_BOTON);
			return true;
		}
		catch (SQLException e)
		{
			System.out.println(e.getMessage());
			return false;
		}
 
		
	}
	
	public Connection get_cn_boton()
	{
		return cn; 
	}
}
