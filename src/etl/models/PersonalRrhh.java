package etl.models;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

import java.util.Arrays;
import java.util.List;

public class PersonalRrhh
{
    /**ESTRUCTURA DE TABLAS**/
    private Schema schemaPersonal;
	private Schema schemaInasistencia;
    private Schema schemaLicSup;
	private Schema schemaTardanza;
    private Schema schemaVacaciones;

	/**CLASES DE BASES DE DATOS ORACLE**/
	private String CONEXION_ORACLE;
	private String USER_BD_RRHH;
	private String PASSWORD_BD_RRHH;

    /**TABLAS RRHH**/
	private String TABLA1_RRHH;
	private String TABLA2_RRHH;
    private String TABLA3_RRHH;
    private String TABLA4_RRHH;
    private String TABLA5_RRHH;
	
    /**LLAMADAS A RUTINAS**/
	private String SCRIPT_SQL_RRHH_PERSONAL;
	private String SCRIPT_SQL_RRHH_INASISTENCIA;
    private String SCRIPT_SQL_RRHH_VACACIONES;
    private String SCRIPT_SQL_RRHH_LICSUP;
    private String SCRIPT_SQL_RRHH_TARDANZA;
    
    public PersonalRrhh()
	{
		this.CONEXION_ORACLE="jdbc:oracle:thin:@172.34.0.120:7520:DBPRUEBAS";
		this.USER_BD_RRHH="ADMINPJ";
		this.PASSWORD_BD_RRHH="789456";
		
		this.SCRIPT_SQL_RRHH_PERSONAL="SELECT * FROM ADMINPJ.UVW_BQ_PERSONAL";
	    this.SCRIPT_SQL_RRHH_INASISTENCIA="SELECT * FROM ADMINPJ.UVW_BQ_INASISTENCIA";
        this.SCRIPT_SQL_RRHH_VACACIONES="SELECT * FROM ADMINPJ.UVW_BQ_VACACIONES";
        this.SCRIPT_SQL_RRHH_LICSUP="SELECT * FROM ADMINPJ.UVW_BQ_PER_LICSUSP";
        this.SCRIPT_SQL_RRHH_TARDANZA="SELECT * FROM ADMINPJ.UVW_BQ_TARDANZA";
        
        this.TABLA1_RRHH="BQ_ETL_RRHH_PERSONAL";
		this.TABLA2_RRHH="BQ_ETL_RRHH_INASISTENCIA";
        this.TABLA3_RRHH="BQ_ETL_RRHH_VACACIONES";
        this.TABLA4_RRHH="BQ_ETL_RRHH_LICSUP";
		this.TABLA5_RRHH="BQ_ETL_RRHH_TARDANZA";
        
	}

    public Schema schemaTablaPersonal()
	{
                this.schemaPersonal = Schema.of(
                Field.of("F_FECHAHORACONSULTA", StandardSQLTypeName.TIMESTAMP),
                Field.of("C_IDANOPROC", StandardSQLTypeName.STRING),
                Field.of("C_IDUEJECOD", StandardSQLTypeName.STRING),
                Field.of("C_CO_EMPLEADO", StandardSQLTypeName.STRING),
                Field.of("C_DNI", StandardSQLTypeName.STRING),
                Field.of("X_MPPIAPPATER", StandardSQLTypeName.STRING),
                Field.of("X_MPPIAPMATER", StandardSQLTypeName.STRING),            
                Field.of("X_MPPINOMBRES", StandardSQLTypeName.STRING),            
                Field.of("X_NOMBRE_COMPLETO", StandardSQLTypeName.STRING),
                Field.of("C_REGIMEN", StandardSQLTypeName.STRING),
                Field.of("X_CARGO", StandardSQLTypeName.STRING),
                Field.of("X_SERVICIO", StandardSQLTypeName.STRING),
                Field.of("X_CORREO_INST", StandardSQLTypeName.STRING),
                Field.of("X_NACIONALIDAD", StandardSQLTypeName.STRING),
                Field.of("C_COD_DISTRITO", StandardSQLTypeName.STRING),
                Field.of("X_DES_DISTRITO", StandardSQLTypeName.STRING),
                Field.of("C_COD_TIPODEPENDENCIA", StandardSQLTypeName.STRING),
                Field.of("X_DES_TIPODEPENDENCIA", StandardSQLTypeName.STRING),
                Field.of("C_COD_DEPENDENCIA", StandardSQLTypeName.STRING),
                Field.of("X_DES_DEPENDENCIA", StandardSQLTypeName.STRING),
                Field.of("C_COD_ESPECIALIDAD", StandardSQLTypeName.STRING),
                Field.of("X_DES_ESPECIALIDAD", StandardSQLTypeName.STRING),
                Field.of("C_COD_SUBESPECIALIDAD", StandardSQLTypeName.STRING),
                Field.of("X_DES_SUBESPECIALIDAD" , StandardSQLTypeName.STRING),   
                Field.of("C_COD_UBIGEO", StandardSQLTypeName.STRING),
                Field.of("X_DES_UDEPARTAMENTO", StandardSQLTypeName.STRING),
                Field.of("X_DES_UPROVINCIA", StandardSQLTypeName.STRING),
                Field.of("X_DES_UDISTRITO", StandardSQLTypeName.STRING),
                Field.of("C_COD_MODTIPO", StandardSQLTypeName.STRING),
                Field.of("X_DES_MODULO", StandardSQLTypeName.STRING),             
                Field.of("C_COD_OFICINA_EQUIVALENTE", StandardSQLTypeName.STRING),
                Field.of("X_ESTADO", StandardSQLTypeName.STRING),
                Field.of("F_PERFECNAC", StandardSQLTypeName.TIMESTAMP),
                Field.of("X_PERSEXO", StandardSQLTypeName.STRING),
                Field.of("N_EDAD", StandardSQLTypeName.INT64),
                Field.of("X_UNIDAD_EJECUTORA", StandardSQLTypeName.STRING),
                Field.of("F_INICIO", StandardSQLTypeName.TIMESTAMP),
                Field.of("X_CONDICION", StandardSQLTypeName.STRING),
                Field.of("X_TIP_MOV", StandardSQLTypeName.STRING),
                Field.of("X_UBICA_UEJEC", StandardSQLTypeName.STRING),
                Field.of("X_UBICA_DISJU", StandardSQLTypeName.STRING),
                Field.of("X_UBICA_LOCAL", StandardSQLTypeName.STRING),
                Field.of("X_UBICA_DEPEND", StandardSQLTypeName.STRING),
                Field.of("M_SUELDO", StandardSQLTypeName.FLOAT64),
                Field.of("X_GRUPO", StandardSQLTypeName.STRING));
		return this.schemaPersonal;
	}

    public Schema schemaTablaInasistencia()
	{
                this.schemaInasistencia = Schema.of(
                Field.of("F_FECHAHORACONSULTA", StandardSQLTypeName.TIMESTAMP),
                Field.of("N_IDTRABAJADOR", StandardSQLTypeName.INT64),
                Field.of("C_DNI", StandardSQLTypeName.STRING),
                Field.of("X_APELLIDOPATERNO", StandardSQLTypeName.STRING),
                Field.of("X_APELLIDOMATERNO", StandardSQLTypeName.STRING),
                Field.of("X_NOMBRE", StandardSQLTypeName.STRING),
                Field.of("N_TOTAL_INASISTENCIA", StandardSQLTypeName.INT64),
                Field.of("X_PERIODO", StandardSQLTypeName.STRING),
                Field.of("C_ID_MES", StandardSQLTypeName.STRING),
                Field.of("X_ANIO", StandardSQLTypeName.STRING));
		return this.schemaInasistencia;
	}

    public Schema schemaTablaVacaciones()
	{
                this.schemaVacaciones = Schema.of(
                Field.of("F_FECHAHORACONSULTA", StandardSQLTypeName.TIMESTAMP),
                Field.of("C_IDPPICODIGO", StandardSQLTypeName.STRING),
                Field.of("X_MPPIAPPATER", StandardSQLTypeName.STRING),
                Field.of("X_MPPIAPMATER", StandardSQLTypeName.STRING),
                Field.of("X_MPPINOMBRES", StandardSQLTypeName.STRING),
                Field.of("C_DNI", StandardSQLTypeName.STRING),
                Field.of("X_PERIODO_VACACIONAL", StandardSQLTypeName.STRING),
                Field.of("N_PVDDIAPROG", StandardSQLTypeName.INT64),
                Field.of("F_PVDFECHINI", StandardSQLTypeName.TIMESTAMP),
                Field.of("F_PVDFECHFIN", StandardSQLTypeName.TIMESTAMP),
                Field.of("X_VACDESCRIP", StandardSQLTypeName.STRING),
                Field.of("X_PVDDOCANNRES", StandardSQLTypeName.STRING),
                Field.of("X_PVDDOCSIGRES", StandardSQLTypeName.STRING),      
                Field.of("C_IDVACCODIGO", StandardSQLTypeName.STRING),
                Field.of("F_PVFCHINGR", StandardSQLTypeName.TIMESTAMP),
                Field.of("N_PVDDIAXREP", StandardSQLTypeName.INT64),
                Field.of("X_PVDESTAPROG", StandardSQLTypeName.STRING));
		return this.schemaVacaciones;
	}

    public Schema schemaTablaLicSup()
	{
                this.schemaLicSup = Schema.of(
                Field.of("F_FECHAHORACONSULTA", StandardSQLTypeName.TIMESTAMP),
                Field.of("C_IDPPICODIGO", StandardSQLTypeName.STRING),
                Field.of("C_IDTMLCODIGO", StandardSQLTypeName.STRING),
                Field.of("X_TMLDESCRIP", StandardSQLTypeName.STRING),
                Field.of("X_DOCUMENTO", StandardSQLTypeName.STRING), 
                Field.of("F_FEC_INICIO", StandardSQLTypeName.TIMESTAMP), 
                Field.of("X_VLPOBSERVA", StandardSQLTypeName.STRING), 
                Field.of("X_TIPO", StandardSQLTypeName.STRING));
		return this.schemaLicSup;
	}

    public Schema schemaTablaTardanza()
	{
                this.schemaTardanza = Schema.of(
                Field.of("F_FECHAHORACONSULTA", StandardSQLTypeName.TIMESTAMP),
                Field.of("N_IDTRABAJADOR", StandardSQLTypeName.INT64),
                Field.of("C_DNI", StandardSQLTypeName.STRING),              
                Field.of("X_APELLIDOPATERNO", StandardSQLTypeName.STRING),  
                Field.of("X_APELLIDOMATERNO", StandardSQLTypeName.STRING), 
                Field.of("X_NOMBRE", StandardSQLTypeName.STRING), 
                Field.of("N_MINUTOS_TARDANZA", StandardSQLTypeName.INT64),
                Field.of("X_PERIODO", StandardSQLTypeName.STRING), 
                Field.of("C_ID_MES", StandardSQLTypeName.STRING), 
                Field.of("X_ANIO", StandardSQLTypeName.STRING)); 
		return this.schemaTardanza;
	}

    public String getCONEXION_ORACLE() {
        return CONEXION_ORACLE;
    }

    public String getUSER_BD_RRHH() {
        return USER_BD_RRHH;
    }

    public String getPASSWORD_BD_RRHH() {
        return PASSWORD_BD_RRHH;
    }

    public String getTABLA1_RRHH() {
        return TABLA1_RRHH;
    }

    public String getTABLA2_RRHH() {
        return TABLA2_RRHH;
    }

    public String getTABLA3_RRHH() {
        return TABLA3_RRHH;
    }

    public String getTABLA4_RRHH() {
        return TABLA4_RRHH;
    }

    public String getTABLA5_RRHH() {
        return TABLA5_RRHH;
    }

    public String getSCRIPT_SQL_RRHH_PERSONAL() {
        return SCRIPT_SQL_RRHH_PERSONAL;
    }

    public String getSCRIPT_SQL_RRHH_INASISTENCIA() {
        return SCRIPT_SQL_RRHH_INASISTENCIA;
    }

    public String getSCRIPT_SQL_RRHH_LICSUP() {
        return SCRIPT_SQL_RRHH_LICSUP;
    }

    public String getSCRIPT_SQL_RRHH_TARDANZA() {
        return SCRIPT_SQL_RRHH_TARDANZA;
    }

    public String getSCRIPT_SQL_RRHH_VACACIONES() {
        return SCRIPT_SQL_RRHH_VACACIONES;
    }

    public List<String> getTablas()
    {
        List<String> listaTablas= Arrays.asList(this.TABLA1_RRHH,this.TABLA2_RRHH,TABLA3_RRHH,TABLA4_RRHH,TABLA5_RRHH);
        return listaTablas;
    }

    public List<Schema> getSchemas()
    {
        List<Schema> listaSchemas=Arrays.asList(this.schemaTablaPersonal(),this.schemaTablaInasistencia(),this.schemaTablaVacaciones(),this.schemaTablaLicSup(),this.schemaTablaTardanza());
        return listaSchemas;
    }

    public List<String> getConsultas()
    {
        List<String> listaSql=Arrays.asList(this.SCRIPT_SQL_RRHH_PERSONAL,this.SCRIPT_SQL_RRHH_INASISTENCIA,this.SCRIPT_SQL_RRHH_VACACIONES,this.SCRIPT_SQL_RRHH_LICSUP,this.SCRIPT_SQL_RRHH_TARDANZA);
        return listaSql;
    }
}
