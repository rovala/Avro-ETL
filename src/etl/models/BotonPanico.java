package etl.models;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

public class BotonPanico {
	
	private Schema schemaActivacion;
	private Schema schemaMonitoreo;
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
	
}


