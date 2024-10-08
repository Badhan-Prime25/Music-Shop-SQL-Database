/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: 2614405
 *
 */

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.



public class RecordsDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for artist's name and one for recordshop's name.
    private ResultSet outcome   = null;

    //JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public RecordsDatabaseService(Socket aSocket){

        //TO BE COMPLETED
        serviceSocket = aSocket;
        this.start();

    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For artist
        this.requestStr[1] = ""; //For recordshop

        String tmp = "";
        try {
            //TO BE COMPLETED
            InputStream socketStream = this.serviceSocket.getInputStream();
            InputStreamReader reader = new InputStreamReader(socketStream);
            StringBuilder buffer = new StringBuilder();
            char x;
            while (true){
                x = (char) reader.read();
                if(x =='#')
                    break;
                buffer.append(x);
            }
            String[] inputs = buffer.toString().split(";");
            if(inputs.length == 2){
                this.requestStr[0] = inputs[0];
                this.requestStr[1] = inputs[1];
            }
            else{
                System.out.println("Service thread: Incorrect message formant");
            }
        }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;

        this.outcome = null;

        String sql = "SELECT Title, Label, Genre, RRP, NumCopies " +
                "FROM ( " +
                "    SELECT record.title AS Title, " +
                "           record.label AS Label, " +
                "           record.genre AS Genre, " +
                "           record.rrp AS RRP, " +
                "           COUNT(recordcopy.copyId) AS NumCopies, " +
                "           ROW_NUMBER() OVER (PARTITION BY record.title ORDER BY COUNT(recordcopy.copyID) DESC) AS RowNum " +
                "    FROM record" +
                "    INNER JOIN artist ON record.artistID = artist.artistID " +
                "    INNER JOIN recordcopy ON record.recordID = recordcopy.recordID " +
                "    INNER JOIN recordshop ON recordcopy.recordshopID = recordshop.recordshopID " +
                "    WHERE artist.lastname = ? AND recordshop.city = ? " +
                "    GROUP BY record.title, record.label, record.genre, record.rrp " +
                ") AS Subquery " +
                "WHERE RowNum = 1 AND NumCopies > 0";//TO BE COMPLETED- Update this line as needed.


        try {
            //Connet to the database
            //TO BE COMPLETED
            Connection con = DriverManager.getConnection(URL,USERNAME,PASSWORD);
            //Make the query
            //TO BE COMPLETED
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crs = aFactory.createCachedRowSet();

            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.setString(1,requestStr[0]);
            stmt.setString(2,requestStr[1]);
            ResultSet rs = stmt.executeQuery();
            crs.populate(rs);

            //Process query
            //TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
            crs.beforeFirst();
            while(crs.next()){
                String title = crs.getString("Title");
                String label = crs.getString("Label");
                String genre = crs.getString("Genre");
                double rrp = crs.getDouble("RRP");
                int numCopies = crs.getInt("NumCopies");
                System.out.println(title+" | "+label+" | "+genre+" | "+rrp+" | "+numCopies);

            }
            outcome = crs;
            //Clean up
            //TO BE COMPLETED
            con.close();

        } catch (Exception e)
        { System.out.println(e);
            flagRequestAttended = false;}

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
            //Return outcome
            //TO BE COMPLETED

            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream(serviceSocket.getOutputStream());
            outcomeStreamWriter.writeObject(outcome);
            outcomeStreamWriter.flush();

            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

            //Terminating connection of the service socket
            //TO BE COMPLETED
            outcomeStreamWriter.close();
            this.serviceSocket.close();

        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run()
    {
        try {
            System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
                    + "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
