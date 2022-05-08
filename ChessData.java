package chess;

import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import ChessLib.*;
import ChessLib.game.*;
import ChessLib.move.*;
import ChessLib.pgn.*;
import ChessLib.util.*;

public class ChessData {

	public static void main(String[] args) throws Exception {
		// Open file for read
		Date t_start = new Date();
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(t_start) + " | Reading data into array...");
		PgnHolder pgn = new PgnHolder("C:\\Users\\adamc\\eclipse-workspace\\ChessDataMigration\\data\\lichess_db_standard_rated_2014-01.pgn");
		pgn.loadPgn();
		
		// Build up list of unique game states
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | Eliminating duplicate game states...");
		List<String> fen_states = new ArrayList<>();
		int prog = 0;
		int i = 0;
		for (Game game: pgn.getGames()) {
			i++;
			// Check progress, get percentage processed
			if ((i * 100) / pgn.getGames().size() > prog) {
				prog = (i * 100) / pgn.getGames().size();
				System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | " + prog + "%");
			}
			
			// Add to list of unique fen states
			game.loadMoveText();
			Board board = new Board();
	        for (Move move: game.getHalfMoves()) {
	            board.doMove(move);
	            String[] fenSplit = board.getFen().split(" ");
	            String fen = fenSplit[0] + " " + fenSplit[1] + " " + fenSplit[2];	//Cut off everything after castling
				if (!fen_states.contains(fen)) {
					fen_states.add(fen);
				}
	        }
		}

		// Store games
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(t_start) + " | Storing games...");
		Connection c = new ConnectToDB().EstablishConnection();	// Establish connection to the DB
		Statement s = c.createStatement();
		prog = 0;
		Date t_proc = new Date();
		i = 0;
		for (Game game: pgn.getGames()) {
			i++;
			// Check progress, get percentage processed
			if ((i * 100) / pgn.getGames().size() > prog) {
				prog = (i * 100) / pgn.getGames().size();
				System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | " + prog + "%");
			}
			if (i % 100 == 0) {    //Time it takes to process 100 games
				long diff = new Date().getTime() - t_proc.getTime();
				long diffSeconds = diff / 1000 % 60;
				long diffMinutes = diff / (60 * 1000) % 60;
				long diffHours = diff / (60 * 60 * 1000) % 24;
				System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | " + diffHours + ":" + diffMinutes + ":" + diffSeconds + " to process 100 games");
				t_proc = new Date();
			}

			int outcome = -1;
			if (Objects.equals(game.getResult().getDescription(), "1/2-1/2")) {		//Stalemate
				outcome = 0;
			} else if (Objects.equals(game.getResult().getDescription(), "1-0")) {	//White wins
				outcome = 1;
			} else if (Objects.equals(game.getResult().getDescription(), "0-1")) {	//Black wins
				outcome = 2;
			}
			String q = "INSERT INTO Games";
			q += "              SET site      = " + sqlQuote(game.getRound().getEvent().getSite()) + ",";
			q += "                  outcome   = " + outcome + ",";
			q += "                  elo_white = " + game.getWhitePlayer().getElo() + ",";
			q += "                  elo_black = " + game.getBlackPlayer().getElo();
			s.execute(q);
		}

		// Store moves
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(t_start) + " | Storing game states...");
		prog = 0;
		t_proc = new Date();
		i = 0;
		for (String fen: fen_states) {
			i++;
			// Check progress, get percentage processed
			if ((i * 100) / fen_states.size() > prog) {
				prog = (i * 100) / fen_states.size();
				System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | " + prog + "%");
			}
			if (i % 100 == 0) {    //Time it takes to process 100 games
				long diff = new Date().getTime() - t_proc.getTime();
				long diffSeconds = diff / 1000 % 60;
				long diffMinutes = diff / (60 * 1000) % 60;
				long diffHours = diff / (60 * 60 * 1000) % 24;
				System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | " + diffHours + ":" + diffMinutes + ":" + diffSeconds + " to process 100 games");
				t_proc = new Date();
			}

			String q = " INSERT INTO Fen_States";
			q += "               SET fen = " + sqlQuote(fen);
			s.execute(q);
		}

		c.close();
		pgn.cleanUp();
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | Done!...");
		System.out.println("Time elapsed: " + new SimpleDateFormat("hh:mm:ss").format(t_start.getTime() - new Date().getTime()));
	}
	
	private static String sqlQuote(String str) {
		return "'" + str + "'";
	}
}
