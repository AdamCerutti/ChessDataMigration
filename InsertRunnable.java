package chess;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertRunnable implements Runnable {
    private String query;
    
    public InsertRunnable(String q) {
        this.query = q;
    }
    
    public void run() {
        Connection tmp = new ConnectToDB().EstablishConnection();
	try {
	    Statement insert = tmp.createStatement();
	    insert.execute(this.query);
	    insert.close();
	    tmp.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }
}
