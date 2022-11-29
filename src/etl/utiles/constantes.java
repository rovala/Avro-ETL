package etl.utiles;

public class constantes
{
	/**GENERALES **/
	public String FORMATO_DATE_TIME;
	public String SALIDA_CSV;
	public String FILE_CONTEXTO;

	/**OBJETOS DE LA NUBE GCP**/
	public String PROYECTO_BQ;
	public String BUCKET_NAME;
	/**CLASES DE BASES DE DATOS POSTGRESQL**/
	public String CLASS_BD;
	
	/**CLASES DE BASES DE DATOS ORACLE**/
	public String CLASS_ORACLE;
	public String CONEXION_ORACLE;
	public String USER_BD_RRHH;
	public String PASSWORD_BD_RRHH;

	/**TABLAS RRHH**/
	public String TABLA1_RRHH;
	public String TABLA2_RRHH;
	public String TABLA3_RRHH;
	public String TABLA4_RRHH;
	public String TABLA5_RRHH;
	/**LLAMADAS A RUTINAS**/
	public String SCRIPT_SQL_PERSONAL;
	public String SCRIPT_SQL_INASISTENCIA;
	public String SCRIPT_SQL_LICENCIASUSPENSION;
	public String SCRIPT_SQL_TARDANZA;
	public String SCRIPT_SQL_VACACIONES;

	/**DATASET**/
	public String DATASET;

	
	public constantes()
	{
		FORMATO_DATE_TIME="yyyy-MM-dd HH:mm:ss.ms";
		SALIDA_CSV="C:\\Users\\personal\\Desktop\\ETL\\";
		FILE_CONTEXTO="";
		PROYECTO_BQ="pe-pjp-cld-01";
		BUCKET_NAME="data_pj_reportes";
		DATASET="DATA_";
	}


	public String getFILE_CONTEXTO() {
		return FILE_CONTEXTO;
	}

	public void setFILE_CONTEXTO(String contexto) {
		FILE_CONTEXTO = contexto;
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

	public String getCLASS_BD() {
		return CLASS_BD;
	}

	public void setCLASS_BD(String CLASS_BD) {
		this.CLASS_BD = CLASS_BD;
	}
	
}
