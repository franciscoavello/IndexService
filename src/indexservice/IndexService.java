package indexservice;

import com.mongodb.*;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;


public class IndexService extends Thread{
    
    String query;
    
    private IndexService(String query) {
        this.query = query;
    }
    
    @Override
    public void run(){        
        try {                   
            Mongo mongo = new Mongo("localhost",27017);
            DB db = mongo.getDB("wikipediaIndex");
            DBCollection tablaDatos = db.getCollection("datosWiki");    
            DBCollection indiceInvertido = db.getCollection("indiceInvertido");
            System.out.println("(Index Service) Soy el thread: " + getName() + ". Recibi la query '" + query + "'. Buscando en el indice invertido");
            System.out.println("Buscado en el índice invertido...");
            DBObject queryDB = new BasicDBObject("palabra", query);
            DBCursor cursor = indiceInvertido.find(queryDB);
            if(cursor.hasNext()){ // Se busca la palabra en el indice invertido
                DBObject objetoPresente = cursor.next();
                IndexInvertido ind = new IndexInvertido((BasicDBObject) objetoPresente);
                for (int i = 0; i < ind.docFrec.size(); i++) {
                    System.out.println("--------------------");
                    System.out.println("Está en el documento: " + ind.docFrec.get(i).idDocumento);
                    System.out.println("Con una frecuencia de: " + ind.docFrec.get(i).frecuencia);
                    System.out.println("En la URL de wikipedia: " + obtenerURL(ind.docFrec.get(i).idDocumento,mongo));
                }
                
            }
            else{ // No existe en el indice
                System.out.println("No se han encontrado resultados en el indice invertido");
            }
            System.out.println("");
        } catch (Exception ex) {
            Logger.getLogger(IndexService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String obtenerURL(String documentoURL, Mongo mongo){
        // Abro las conexiones con MongoDB
        DB db = mongo.getDB("wikipediaIndex");
        DBCollection tablaDatos = db.getCollection("datosWiki");    
        String URL="";
        DBObject queryURL= new BasicDBObject("idDoc", documentoURL);                
        DBCursor cursorURL = tablaDatos.find(queryURL);
        if(cursorURL.hasNext()){
           DBObject objetoPresente = cursorURL.next();
           URL = objetoPresente.get("direccion").toString();
        }
        return URL;
    }

    public static void main(String[] args) throws IOException {
        while(true){
            String query;
            BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
            query = inFromUser.readLine();
            IndexService hilo = new IndexService(query);
            hilo.start();
        }
    }
    
}
