package indexservice;

import com.mongodb.*;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;


public class IndexService extends Thread{
    
    String query;
    static String ipFrontService="localhost";
    static String ipCachingService="localhost";
    
    private IndexService(String query) {
        this.query = query;
    }
    
    // Para recibir desde el FrontService
    
    public static void socketServidorIndexServiceParaFrontService() throws Exception{    
        
        //Variables
        String desdeFrontService;        
        //Socket para el servidor en el puerto
        ServerSocket socketDesdeFrontService = new ServerSocket(5003);
        
        //Socket listo para recibir 
        Socket connectionSocket = socketDesdeFrontService.accept();
        //Buffer para recibir desde el cliente
        BufferedReader inDesdeFrontService = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
        //Buffer para enviar al cliente
            
        //Recibimos el dato del cliente y lo mostramos en el server
        desdeFrontService =inDesdeFrontService.readLine();
        System.out.println("Recibidos: " + desdeFrontService);
        IndexService hilo = new IndexService(desdeFrontService);
        hilo.start();
        socketDesdeFrontService.close();
    }
        
    // Para enviar al FrontService
    
    public static void socketClienteDesdeIndexServiceHaciaFrontService(String respuestaAFrontService) throws Exception{        
        //Socket para el cliente (host, puerto)
        Socket socketHaciaFrontService = new Socket(ipFrontService, 5004);
        
        //Buffer para enviar el dato al server
        DataOutputStream haciaElFrontService = new DataOutputStream(socketHaciaFrontService.getOutputStream());
        
        haciaElFrontService.writeBytes(respuestaAFrontService + '\n');
        
        socketHaciaFrontService.close();  
    }
    
    // Para enviar al CachingService
    
    public static void socketClienteDesdeIndexServiceHaciaCachingService(String respuestaACachingService) throws Exception{        
        //Socket para el cliente (host, puerto)
        Socket socketHaciaCachingService = new Socket(ipCachingService, 5005);
        
        //Buffer para enviar el dato al server
        DataOutputStream haciaElCachingService = new DataOutputStream(socketHaciaCachingService.getOutputStream());
        
        haciaElCachingService.writeBytes(respuestaACachingService + '\n');
        
        socketHaciaCachingService.close();  
    }
    
    @Override
    public void run(){        
        try {                   
            Mongo mongo = new Mongo("localhost",27017);
            DB db = mongo.getDB("wikipediaIndex");
            DBCollection tablaDatos = db.getCollection("datosWiki");    
            DBCollection indiceInvertido = db.getCollection("indiceInvertido");
            System.out.println("(Index Service) Soy el thread: " + getName() + ". Recibi la query '" + query + "'. Buscando en el indice invertido");
            System.out.println("(Index Service) Buscado en el índice invertido...");
            String[] palabrasConsulta = query.split(" ");
            String paraEnviar= new String();
            String paraEnviarCaching = new String();
            int cuantasCache=0;
            for(int k=0; k<palabrasConsulta.length;k++){
                query = palabrasConsulta[k];
                DBObject queryDB = new BasicDBObject("palabra", query);
                DBCursor cursor = indiceInvertido.find(queryDB);
                if(cursor.hasNext()){// Se busca la palabra en el indice invertido
                    cuantasCache++;
                    DBObject objetoPresente = cursor.next();
                    IndexInvertido ind = new IndexInvertido((BasicDBObject) objetoPresente);
                    paraEnviarCaching = paraEnviarCaching.concat(query +",");
                    for (int i = 0; i < ind.docFrec.size(); i++) {
                        System.out.println("--------------------");
                        System.out.println("Está en el documento: " + ind.docFrec.get(i).idDocumento);
                        System.out.println("Con una frecuencia de: " + ind.docFrec.get(i).frecuencia);
                        System.out.println("En la URL de wikipedia: " + obtenerURL(ind.docFrec.get(i).idDocumento,mongo));
                        paraEnviar = paraEnviar.concat(ind.docFrec.get(i).idDocumento);
                        paraEnviarCaching = paraEnviarCaching.concat(ind.docFrec.get(i).idDocumento);
                        paraEnviar = paraEnviar.concat("#");
                        paraEnviarCaching = paraEnviarCaching.concat("#");
                        paraEnviar = paraEnviar.concat(ind.docFrec.get(i).frecuencia.toString());
                        paraEnviarCaching = paraEnviarCaching.concat(ind.docFrec.get(i).frecuencia.toString());
                        paraEnviar = paraEnviar.concat("#");
                        paraEnviarCaching = paraEnviarCaching.concat("#");
                        paraEnviar = paraEnviar.concat(obtenerURL(ind.docFrec.get(i).idDocumento,mongo));
                        paraEnviarCaching = paraEnviarCaching.concat(obtenerURL(ind.docFrec.get(i).idDocumento,mongo));
                        if(i != ind.docFrec.size()-1){
                            paraEnviar = paraEnviar.concat(",");
                            paraEnviarCaching = paraEnviarCaching.concat(",");
                        }                    
                    }
                    paraEnviarCaching = paraEnviarCaching.concat(",*");
                    if(k != palabrasConsulta.length-1){
                        paraEnviarCaching = paraEnviarCaching.concat(",");
                    }  
                    if(k==palabrasConsulta.length-1){
                        paraEnviarCaching = paraEnviarCaching.concat("," + cuantasCache); 
                        socketClienteDesdeIndexServiceHaciaCachingService(paraEnviarCaching);
                        socketClienteDesdeIndexServiceHaciaFrontService(paraEnviar);
                    }
                    else{
                        paraEnviar = paraEnviar.concat(",");
                    }     /*  
                    String paraEnviarCaching = paraEnviar.concat(","+query); 
                    socketClienteDesdeIndexServiceHaciaCachingService(paraEnviarCaching);
                    socketClienteDesdeIndexServiceHaciaFrontService(paraEnviar);*/
                }
                else{ // No existe en el indice
                    if(k==palabrasConsulta.length-1 || cuantasCache==0){
                        System.out.println("(Index Service) No se han encontrado resultados en el indice invertido");
                        String respuestaAFrontService = "MISS!!";
                        socketClienteDesdeIndexServiceHaciaCachingService("NO");
                        socketClienteDesdeIndexServiceHaciaFrontService(respuestaAFrontService);
                    }/*
                    System.out.println("(Index Service) No se han encontrado resultados en el indice invertido");
                    String respuestaAFrontService = "MISS!!";
                    socketClienteDesdeIndexServiceHaciaCachingService("NO");
                    socketClienteDesdeIndexServiceHaciaFrontService(respuestaAFrontService);*/

                }
                System.out.println("");
            }
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

    public static void main(String[] args) throws IOException, Exception {
        while(true){
            socketServidorIndexServiceParaFrontService();            
        }
    }
    
}
