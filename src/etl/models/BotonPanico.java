package etl.models;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

import java.util.Arrays;
import java.util.List;

public class BotonPanico {
	
	private Schema schemaActivacion;
	private Schema schemaMonitoreo;
	/**CLASES DE BASES DE DATOS POSTGRESQL**/
	private String CONEXION_POSTGRESQL;
	private String USER_BD_BOTON;
	private String PASSWORD_BD_BOTON;
	/**TABLAS BOTON**/
	private String TABLA1_BOTON;
	private String TABLA2_BOTON;
	/**LLAMADAS A RUTINAS**/
	private String SCRIPT_SQL_BOTON_ACTIVACION;
	private String SCRIPT_SQL_BOTON_MONITOREO;

	public BotonPanico()
	{
		this.CONEXION_POSTGRESQL="jdbc:postgresql://127.0.0.1:5432/db_etl_boton";
		this.USER_BD_BOTON="user_etl_boton";
		this.PASSWORD_BD_BOTON="123456";
		
		this.SCRIPT_SQL_BOTON_ACTIVACION="SELECT * FROM sc_etl_boton.uvw_bqactivacion";
		this.SCRIPT_SQL_BOTON_MONITOREO="SELECT * FROM sc_etl_boton.uvw_bqmonitoreo";
		this.TABLA1_BOTON="BQ_ETL_BOTON_ACTIVACION";
		this.TABLA2_BOTON="BQ_ETL_BOTON_MONITOREO";
	}
	
	public Schema schemaTablaActivacion()
	{
				schemaActivacion = Schema.of(
				Field.of("F_FECHAHORACONSULTA", StandardSQLTypeName.TIMESTAMP),
				Field.of("X_CORTE", StandardSQLTypeName.STRING),
				Field.of("C_DNI", StandardSQLTypeName.STRING),
				Field.of("L_CIERRE", StandardSQLTypeName.STRING),
				Field.of("F_CIERRE", StandardSQLTypeName.TIMESTAMP),
				Field.of("X_MOTIVOCIERRE", StandardSQLTypeName.STRING),
				Field.of("X_USERATIENDE", StandardSQLTypeName.STRING),
				Field.of("N_CONTEO", StandardSQLTypeName.INT64),
				Field.of("F_FECHAHORA", StandardSQLTypeName.TIMESTAMP),
				Field.of("N_LATITUD", StandardSQLTypeName.FLOAT64),
				Field.of("N_LONGITUD", StandardSQLTypeName.FLOAT64),
				Field.of("N_PRES", StandardSQLTypeName.FLOAT64),
				Field.of("C_INS", StandardSQLTypeName.STRING));
		return schemaActivacion;
	}
	
	public Schema schemaTablaMonitoreo()
	{
				schemaMonitoreo = Schema.of(
				Field.of("F_FECHAHORACONSULTA", StandardSQLTypeName.TIMESTAMP),
				Field.of("X_DISTRITO", StandardSQLTypeName.STRING),
				Field.of("C_DNIVICTIMA", StandardSQLTypeName.STRING),
				Field.of("X_NOMBREVICTIMA", StandardSQLTypeName.STRING),
				Field.of("C_CELULARVICTIMA", StandardSQLTypeName.STRING),
				Field.of("X_OPERADORVICTIMA", StandardSQLTypeName.STRING),
				Field.of("C_DNIAGRESOR", StandardSQLTypeName.STRING),
				Field.of("X_NOMBREAGRESOR", StandardSQLTypeName.STRING),
				Field.of("C_CELULARAGRESOR", StandardSQLTypeName.STRING),
				Field.of("X_OPERADORAGRESOR", StandardSQLTypeName.STRING),
				Field.of("F_FREGISTRO", StandardSQLTypeName.TIMESTAMP),
				Field.of("X_MEDIDAPROTECCION", StandardSQLTypeName.STRING));
		return schemaMonitoreo;
	}

	public String getTABLA1_BOTON() {
		return TABLA1_BOTON;
	}
	
	public String getTABLA2_BOTON() {
		return TABLA2_BOTON;
	}

	public String getSCRIPT_SQL_BOTON_ACTIVACION() {
		return SCRIPT_SQL_BOTON_ACTIVACION;
	}

	public String getSCRIPT_SQL_BOTON_MONITOREO() {
		return SCRIPT_SQL_BOTON_MONITOREO;
	}

	public String getUSER_BD_BOTON() {
		return USER_BD_BOTON;
	}

	public String getPASSWORD_BD_BOTON() {
		return PASSWORD_BD_BOTON;
	}

	public String getCONEXION_POSTGRESQL() {
		return CONEXION_POSTGRESQL;
	}

	public List<String> getTablas()
	{
		List<String> listaTablas=Arrays.asList(this.TABLA1_BOTON,this.TABLA2_BOTON);
		return listaTablas;
	}

	public List<Schema> getSchemas()
	{
		List<Schema> listaSchemas=Arrays.asList(this.schemaTablaActivacion(),this.schemaTablaMonitoreo());
		return listaSchemas;
	}

	public List<String> getConsultas()
	{
		List<String> listaSql=Arrays.asList(this.SCRIPT_SQL_BOTON_ACTIVACION,this.SCRIPT_SQL_BOTON_MONITOREO);
		return listaSql;
	}
}


