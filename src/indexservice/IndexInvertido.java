package indexservice;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.io.Serializable;
import java.util.ArrayList;

public class IndexInvertido implements Serializable {
    
    String palabra;
    ArrayList<ClaveValorDatos> docFrec;

    public String getPalabra() {
        return palabra;
    }

    public void setPalabra(String palabra) {
        this.palabra = palabra;
    }

    public ArrayList<ClaveValorDatos> getDocFrec() {
        return docFrec;
    }

    public void setDocFrec(ArrayList<ClaveValorDatos> docFrec) {
        this.docFrec = docFrec;
    }

    public IndexInvertido(String palabra, ArrayList<ClaveValorDatos> docFrec) {
        this.palabra = palabra;
        this.docFrec = docFrec;
    }
    
    public IndexInvertido(BasicDBObject dBObjectIndex) {
        this.palabra = dBObjectIndex.getString("palabra");

	BasicDBList listDatos = (BasicDBList) dBObjectIndex.get("datos");
        this.docFrec = new ArrayList<ClaveValorDatos>();
	for (Object keyVal : listDatos) {
            this.docFrec.add(new ClaveValorDatos((BasicDBObject) keyVal));
	}        
    }
   
    public BasicDBObject toDBObjectIndexInvertido(ArrayList<ClaveValorDatos> arrayKeyValue) {
        
        // Creamos una instancia BasicDBObject
        BasicDBObject dBObjectIndexInvertido = new BasicDBObject();
        dBObjectIndexInvertido.append("palabra", this.getPalabra());
        ArrayList<BasicDBObject> clavesValores = new ArrayList<BasicDBObject>();
        for (int i=0; i<arrayKeyValue.size();i++) {
            ClaveValorDatos keyValuePal = new ClaveValorDatos(arrayKeyValue.get(i).idDocumento,arrayKeyValue.get(i).frecuencia);            
            clavesValores.add(keyValuePal.toDBObjectKeyValues());
	}
        dBObjectIndexInvertido.append("datos", clavesValores);        
        return dBObjectIndexInvertido;
    }

    
    
}