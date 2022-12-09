package etl.models;

import etl.dao.EntornoBaseDatos;
import etl.ini.StreamingCsvResultSetExtractor;
import etl.utiles.constantes;
import etl.utiles.tiempo;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DefinicionExtraccion
{
    private List<String> arrayEntornos;
    private List<CloudStorageBigqueryTx> arrayCloudStorageBigqueryTx;

    public DefinicionExtraccion(List<String> arrayEntornos)
    {
        this.arrayEntornos=new ArrayList<>(arrayEntornos);
        this.arrayCloudStorageBigqueryTx=new ArrayList<>();
    }

    public List<CloudStorageBigqueryTx> startExtraccion()
    {
        EntornoBaseDatos entornoBaseDatos=new EntornoBaseDatos();
        constantes ctemp=new constantes();

        this.arrayEntornos.stream().forEach(x->{
            List<String> strSql=new ArrayList<>();
            List<String> arrayTablas=new ArrayList<>();
            Connection cn=null;
            Statement st=null;
            ctemp.setFILE_CONTEXTO(x);
            switch (x)
            {
                case "boton_panico":
                    BotonPanico bPanico=new BotonPanico();
                    arrayTablas=bPanico.getTablas();
		            ctemp.setCLASS_BD("org.postgresql.Driver");
		            entornoBaseDatos.prepararEnvironment(ctemp.getCLASS_BD());
                    entornoBaseDatos.setConexion(bPanico.getCONEXION_POSTGRESQL(),bPanico.getUSER_BD_BOTON(),bPanico.getPASSWORD_BD_BOTON());
		            cn=entornoBaseDatos.get_cn();
		            if (cn==null) return;
		            entornoBaseDatos.setStatement();
		            st=entornoBaseDatos.get_st();
		            if (st==null) return;
                    strSql=bPanico.getConsultas();

                    break;
                case "rrhh_personal":
                    //PersonalRrhh prh=new PersonalRrhh();
                    //arrayTablas=prh.getTablas();


                    break;
            }
            this.executeSql(entornoBaseDatos,st,strSql,ctemp,arrayTablas);
            //cn.close();
            //st.close();
        });
        return arrayCloudStorageBigqueryTx;
    }

    public void executeSql(EntornoBaseDatos entornoBaseDatos,Statement st,List<String> strSql,constantes ctemp,List<String> arrayTablas)
    {
        tiempo exec_time=new tiempo();
        AtomicInteger i=new AtomicInteger();
        exec_time.setFormatDateTime(ctemp.FORMATO_DATE_TIME);
        i.set(0);
        strSql.stream().forEach(x->{
            CloudStorageBigqueryTx cloudStorageBigqueryTx=new CloudStorageBigqueryTx();
            StreamingCsvResultSetExtractor streamingCsvResultSetExtractor;
            String nameFileCsv=ctemp.getFILE_CONTEXTO() + exec_time.getDateTime().replace(":","_") + i.get() +".csv";
            /********INFO PARA LA TRANSFERENCIA A LA NUBE********/
            cloudStorageBigqueryTx.setCsv(nameFileCsv);
            cloudStorageBigqueryTx.setTabla(arrayTablas.get(i.get()));
            cloudStorageBigqueryTx.setDataset(ctemp.getDATASET());
            cloudStorageBigqueryTx.setContexto(ctemp.getFILE_CONTEXTO());
            this.arrayCloudStorageBigqueryTx.add(cloudStorageBigqueryTx);
            /********INFO PARA LA TRANSFERENCIA A LA NUBE********/
            i.set(i.get()+1);
            OutputStream os;
            try
            {
                os = new FileOutputStream(ctemp.SALIDA_CSV + nameFileCsv);
                streamingCsvResultSetExtractor=new StreamingCsvResultSetExtractor(os);
                streamingCsvResultSetExtractor.extractData(st,entornoBaseDatos.leer_vista(x),1);
            }
            catch (FileNotFoundException e)
            {
                System.out.println("Apertura de Archivo " + e.getMessage() + "\n " +ctemp.SALIDA_CSV + ctemp.getFILE_CONTEXTO());
            }
        });
    }
}
