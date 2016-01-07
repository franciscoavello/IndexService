package indexservice;

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
            System.out.println("(Index Service) Soy el thread: " + getName() + ". Recibi la query '" + query + "'. Buscando en el indice invertido");
        } catch (Exception ex) {
            Logger.getLogger(IndexService.class.getName()).log(Level.SEVERE, null, ex);
        }
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
