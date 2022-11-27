package etl.utiles;

public class constantes {
	public String FORMATO_DATE_TIME;
	public String SALIDA_CSV;
	public String FILE_CONTEXTO;
	
	/**CLASES DE BASES DE DATOS**/
	public String CLASS_POSTGRESQL;
	public String CONEXION_POSTGRESQL;
	public String USER_BD_BOTON;
	public String PASSWORD_BD_BOTON;
	/**OBJETOS DE LA NUBE GCP**/
	public String PROYECTO_BQ;
	public String BUCKET_NAME;
	       /**DATASET**/
	public String DATASET;
       /**TABLAS BOTON**/
	public String TABLA1_BOTON;
	public String TABLA2_BOTON;
	/**LLAMADAS A RUTINAS**/
	public String SCRIPT_SQL_BOTON_ACTIVACION;
	public String SCRIPT_SQL_BOTON_MONITOREO;
	
	public constantes()
	{
		FORMATO_DATE_TIME="yyyy-MM-dd HH:mm:ss.ms";
		SALIDA_CSV="C:\\Users\\personal\\Desktop\\ETL\\";
		CLASS_POSTGRESQL="org.postgresql.Driver";
		CONEXION_POSTGRESQL="jdbc:postgresql://127.0.0.1:5432/db_etl_boton";
		USER_BD_BOTON="user_etl_boton";
		PASSWORD_BD_BOTON="123456";
		FILE_CONTEXTO="";
		SCRIPT_SQL_BOTON_ACTIVACION="SELECT * FROM sc_etl_boton.uvw_bqactivacion";
		SCRIPT_SQL_BOTON_MONITOREO="SELECT * FROM sc_etl_boton.uvw_bqmonitoreo";
		PROYECTO_BQ="pe-pjp-cld-01";
		BUCKET_NAME="data_pj_reportes";
		DATASET="DATA_";
		TABLA1_BOTON="BQ_ETL_BOTON_ACTIVACION";
		TABLA2_BOTON="BQ_ETL_BOTON_MONITOREO";
	}

	public String getFILE_CONTEXTO() {
		return FILE_CONTEXTO;
	}

	public void setFILE_CONTEXTO(String contexto) {
		FILE_CONTEXTO = contexto;
	}

	public String getSCRIPT_SQL_ACTIVACION() {
		return SCRIPT_SQL_BOTON_ACTIVACION;
	}

	public void setSCRIPT_SQL_ACTIVACION(String sql) {
		SCRIPT_SQL_BOTON_ACTIVACION = sql;
	}
	
	public String getSCRIPT_SQL_MONITOREO() {
		return SCRIPT_SQL_BOTON_MONITOREO;
	}

	public void setSCRIPT_SQL_MONITOREO(String sql) {
		SCRIPT_SQL_BOTON_MONITOREO = sql;
	}
	
	public String getBUCKET_NAME() {
		return BUCKET_NAME;
	}
	
	public void setBUCKET_NAME(String bn) {
		BUCKET_NAME=bn;
	}

	/********NO REQUIERE SETTER********/
	public String getDATASET() {
		return DATASET + FILE_CONTEXTO.toUpperCase();
	}

	
}
