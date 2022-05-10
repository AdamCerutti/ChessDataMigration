package chess;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import ChessLib.*;
import ChessLib.game.*;
import ChessLib.move.*;
import ChessLib.pgn.*;
import ChessLib.util.*;

public class ChessData {
	private static PgnHolder pgn;
	private static int i = 0, prog = 0;
	private static Date t_proc;
	private static Connection c;
	private static Statement s;
	private static String q;
	private static String game_batch, fen_batch;
	private static int fen_batch_size;
	
	public static void main(String[] args) throws Exception {
		// Open file for read
		Date t_start = new Date();
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(t_start) + " | Reading data into array...");
		pgn = new PgnHolder("C:\\Users\\adamc\\eclipse-workspace\\ChessDataMigration\\data\\lichess_db_standard_rated_2014-01.pgn");
		pgn.loadPgn();
		
		t_proc = new Date();
		
		// Store unique games and unique game states
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | Storing games and unique FEN states...");
		c = new ConnectToDB().EstablishConnection();	//Establish connection to the DB
		//s = c.createStatement();
		game_batch = "";
		fen_batch = "";
		fen_batch_size = 0;
		for (Game game: pgn.getGames()) {
			i++;
			InsertGameAndFen(game);
		}
		
//		pgn.getGames().stream().forEach(sellerNames -> {
//	        Stream<Game> stream = StreamSupport.stream(pgn.getGames().spliterator(), true); // true means use parallel stream
//	        stream.forEach(game -> {
//	        	i++;
//				InsertGameAndFen(game);
//	        });
//	    });
		
		// Clean up
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | Done!");
		s.close();
        c.close();
		pgn.cleanUp();
		
		// Calculate time elapsed
		long diff = new Date().getTime() - t_start.getTime();
		long diffSeconds = diff / 1000 % 60, diffMinutes = diff / (60 * 1000) % 60, diffHours = diff / (60 * 60 * 1000) % 24;
		String strSec = String.valueOf(diffSeconds), strMin = String.valueOf(diffMinutes), strHr = String.valueOf(diffHours);
		if (diffSeconds < 10) strSec = "0" + diffSeconds;
		if (diffMinutes < 10) strMin = "0" + diffMinutes;
		if (diffHours < 10) strHr = "0" + diffHours;
		System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | Time elapsed: " + strHr + ":" + strMin + ":" + strSec);
	}
	
	private static String sqlQuote(String str) {
		return "'" + str + "'";
	}
	
	private static void InsertGameAndFen(Game game) throws Exception {
		MoveList moves;	//List of moves from game
		Board board;	//Game board for move iteration
		
		if ((i * 100) / pgn.getGames().size() < 74)	//Skip first given %
			return;
		
		// Check progress, get percentage processed
		if ((i * 100) / pgn.getGames().size() > prog) {
			prog = (i * 100) / pgn.getGames().size();
			System.out.println(new SimpleDateFormat("MMM dd hh:mm:ssa").format(new Date()) + " | " + prog + "%");
		}
		
		InsertGameBatch(game);	//Store game
		
		// Store unique fen states
		game.loadMoveText();
		moves = game.getHalfMoves();
		board = new Board();
        for (Move move: moves) {
            board.doMove(move);
            InsertFenBatch(board);
        }
	}
	
	private static void InsertGameBatch(Game game) throws SQLException {
		int outcome;	//0 = stalemate, 1 = white wins, 2 = black wins
		
		// Determine outcome from PGN result string
		outcome = -1;
		if (game.getResult().getDescription() == "1/2-1/2") {		//Stalemate
			outcome = 0;
		} else if (game.getResult().getDescription() == "1-0") {	//White wins
			outcome = 1;
		} else if (game.getResult().getDescription() == "0-1") {	//Black wins
			outcome = 2;
		}
		
		// Add game to SQL batch string
		if (game_batch != "") game_batch += ",";
		game_batch += "(" + sqlQuote(game.getRound().getEvent().getSite()) + "," + outcome + "," + game.getWhitePlayer().getElo() + "," + game.getBlackPlayer().getElo() + ")";
		
		// Insert game if batch has 10000 games, or we are at the end of processing
		if (i % 10000 == 0 || i == pgn.getGames().size()) {
	        q = " INSERT INTO Games (site, outcome, elo_white, elo_black)";
	        q += "            VALUES " + game_batch;
	        q += " ON DUPLICATE KEY UPDATE site=site";
	        //s.execute(q);
	        game_batch = "";
	        InsertRunnable r = new InsertRunnable(q);
	    	Thread t = new Thread(r);
	    	t.start();
		}
	}
	
	private static void InsertFenBatch(Board board) throws SQLException {
		// Get fen string, cut off everything past castling
		String[] fenSplit = board.getFen().split(" ");
		String fen = fenSplit[0] + " " + fenSplit[1] + " " + fenSplit[2];
		
		// Add fen to SQL batch string
		if (fen_batch != "") fen_batch += ",";
		fen_batch += "(" + sqlQuote(fen) + ")";
		fen_batch_size += 1;
		
		// Insert fen if batch has 10000 fens, or we are at the end of processing
		if (fen_batch_size >= 20000 || i == pgn.getGames().size()) {
			q = " INSERT INTO Fen_States (fen)";
			q += "            VALUES " + fen_batch;
			q += " ON DUPLICATE KEY UPDATE fen=fen";
			//s.execute(q);
			fen_batch = "";
			fen_batch_size = 0;
			InsertRunnable r = new InsertRunnable(q);
	    	Thread t = new Thread(r);
	    	t.start();
		}
	}
