package etl.models;

import com.google.cloud.bigquery.Schema;
import etl.dao.bigQuery;
import etl.utiles.constantes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DefinicionEntornos
{
    private List<String> arrayEntornos;
    private bigQuery bq;
    public DefinicionEntornos(List<String> arrayEntornos,bigQuery bq)
    {
        this.arrayEntornos=new ArrayList<>(arrayEntornos);
        this.bq=bq;
    }

    public boolean seteoEntornos()
    {
        /*********************************/
        /*************SETEO DE ENTORNO ***/
        /*********************************/
        constantes ctemp=new constantes();
        arrayEntornos.stream().forEach(x->
                {
                    List<String> arrayTablas=new ArrayList<>();
                    List<Schema> arraySchemas=new ArrayList<>();
                    ctemp.setFILE_CONTEXTO(x);
                    switch (x)
                    {
                        case "boton_panico":
                            BotonPanico bp=new BotonPanico();
                            arrayTablas=bp.getTablas();
                            arraySchemas=bp.getSchemas();
                            break;
                        case "rrhh_personal":
                            PersonalRrhh prh=new PersonalRrhh();
                            arrayTablas=prh.getTablas();
                            arraySchemas=prh.getSchemas();
                            break;
                    }
                    this.borrarTablas(arrayTablas,ctemp);
                    this.crearTablas(arrayTablas,arraySchemas,ctemp);
                }
        );
        return true;
    }

    public void borrarTablas(List<String> arrayTablas,constantes ctemp)
    {
        arrayTablas.stream().forEach(t->this.bq.dropTable(ctemp.getDATASET(),t));
    }

    public void crearTablas(List<String> arrayTablas,List<Schema> arraySchemas,constantes ctemp)
    {
        AtomicInteger i = new AtomicInteger();
        i.set(0);
        arrayTablas.stream().forEach(t->
        {
            this.bq.createTable(ctemp.getDATASET(), t,arraySchemas.get(i.get()));
            i.set(i.get() + 1);
        });
    }
}
