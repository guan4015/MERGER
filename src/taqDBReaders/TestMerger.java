package taqDBReaders;

import java.util.ArrayList;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;

import taqDBReaders.DATGeneration;
import taqDBReaders.GZFileUtils;
// import taqDBReaders.TAQQuotesDBReader;
import taqDBReaders.TAQTradesDBReader;

import dbReaderFramework.DBManager;
import dbReaderFramework.I_DBProcessor;
import dbReaderFramework.I_DBReader;
import dbReaderFramework.MergeClock;
import dbReaderFramework.TimeClock;

public class TestMerger {
	
	public static void main(String[] args) throws IOException {
		
		String BASE_INPUT_DIR = "/Users/leonmaclin/Documents/sampleTAQ";
		String BASE_OUTPUT_DIR = "/Users/leonmaclin/Documents";
		
		String USER_INPUT_DIR = "C:\\Users\\xiaog\\Documents\\sampleTAQ";
		String USER_OUTPUT_DIR = "C:\\Users\\xiaog\\Documents\\mergerfile";
		
		String dataDir = USER_INPUT_DIR;
		String outDir = USER_OUTPUT_DIR;
		
		DATGeneration datfile = new DATGeneration( dataDir, outDir );
		String substring1 = "\\trades\\20070620\\MSFT_trades.binRQ";
		String substring2 = "\\trades\\20070620\\IBM_trades.binRQ";
		datfile.AddInputFileDirectory(substring1);
		datfile.AddInputFileDirectory(substring2);
		
		ArrayList<String> test =  datfile.getInputFileFDirectory();
		ArrayList<TAQTradesDBReader> readers = new ArrayList<TAQTradesDBReader>();
		String curDir = System.getProperty("user.dir");
		System.out.println(curDir);
		FileInputStream fileInputStream = new FileInputStream( "sampleTAQ/trades/20070620/IBM_trades.binRT" );
		
		String f = "sampleTAQ/trades/20070620/IBM_trades.binRT";
		ReadGZippedTAQTradesFile taqTrades = new ReadGZippedTAQTradesFile( f );
	}

}
