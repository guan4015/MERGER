package dbReader;

import org.junit.Test;
import java.util.ArrayList;

import junit.framework.TestCase;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;

import taqDBReaders.DATGeneration;
import taqDBReaders.GZFileUtils;
// import taqDBReaders.TAQQuotesDBReader;
import taqDBReaders.TAQTradesDBReader;
import taqDBReaders.Merger;

import dbReaderFramework.DBManager;
import dbReaderFramework.I_DBProcessor;
import dbReaderFramework.I_DBReader;
import dbReaderFramework.MergeClock;
import dbReaderFramework.TimeClock;

/**
 * This class shows examples of how to merge or join two files 
 * using the DBReader framework. The first example merges two 
 * TAQ quotes files and puts them into a binary compressed file.
 * The second example joins a trades and a quotes file and 
 * outputs them as text to the console.
 *  
 * @author Lee
 * @editor xiaog
 * 
 * In what follows, we extends the test units given by Prof. Lee Maclin. 
 * The first unit is about to generate .dat files and second one is to test
 * the merge
 * 
 */
public class Test_MergerAndJoin extends junit.framework.TestCase {
	
	//protected static final String BASE_INPUT_DIR = "/Users/leonmaclin/Documents/sampleTAQ";
	//protected static final String BASE_OUTPUT_DIR = "/Users/leonmaclin/Documents";
	
	protected String USER_INPUT_DIR = "sampleTAQ";
	protected String USER_OUTPUT_DIR1 = "mergerfile1/";
	protected String USER_OUTPUT_DIR2 = "mergerfile2/";
	
	/**
	 * The following test method is to test the generation of
	 * .dat files. We applied the simplest algorithm here.
	 * @throws Exception
	 */
	
	public void test_DATGeneration() throws Exception{
		// This is the data directory on my machine
		// In that directory, there is a quotes directory and
		// a trades directory, and in each of those there is
		// one directory per day, and in each of those, there
		// are individual data files for one stock for one
		// day.
		String dataDir = USER_INPUT_DIR;
		String outDir = USER_OUTPUT_DIR1;
		
		// The day of 20070620
		DATGeneration datfile = new DATGeneration( dataDir, outDir );
		String substring1 = "/trades/20070620/IBM_trades.binRT";
		String substring2 = "/trades/20070620/MSFT_trades.binRT";
		datfile.AddInputFileDirectory(substring1);
		datfile.AddInputFileDirectory(substring2);
		
		// generate readers list
		datfile.generateReadersList();
		// generate default output list
		datfile.generateDefaultOutputList();
		
		ArrayList<String> test =  datfile.getOutFileDirectory();
		for (String s : test) {
			System.out.println(s);
		}
		
		datfile.lauch();
		
		// The day of 20070621
		
		String outDir2 = USER_OUTPUT_DIR2;
		
		DATGeneration datfile2 = new DATGeneration( dataDir, outDir2 );
		String substring3 = "/trades/20070621/IBM_trades.binRT";
		String substring4 = "/trades/20070621/MSFT_trades.binRT";
		datfile2.AddInputFileDirectory(substring3);
		datfile2.AddInputFileDirectory(substring4);

		datfile2.generateReadersList();
		// generate default output list
		datfile2.generateDefaultOutputList();
		
		ArrayList<String> test1 =  datfile2.getOutFileDirectory();
		for (String s : test1) {
			System.out.println(s);
		}
		
		datfile2.lauch();
    	// Generate a DATGeneration class
		
	}
	/**
	 * The following test method tests the merge of two files
	 * The inputs are the directories of the files that are supposed to be merged.
	 * The output is the merged file.
	 * The merger class is an extension of the one mentioned in class since
	 * we do not restrict the number of files that are going to be merged. In fact,
	 * we can have more than two inputs files.
	 * 
	 * @throws Exception
	 */
	public void test_mergeTwoQuotesFiles() throws Exception {
		
		// Define the input directories
		LinkedList<String> inputDirectories1 = new LinkedList<String>();
		String address1 = "mergerfile1/0_1.dat";
		String address2 = "mergerfile1/1_1.dat";
		String outputDirectory = "mergerfile1/";
		String filename = "20070620merge_2.dat";
		inputDirectories1.add( address1 );
		inputDirectories1.add( address2 );
		
		Merger merger1 = new Merger( inputDirectories1, outputDirectory );
		merger1.launch( filename );
		
		LinkedList<String> inputDirectories2 = new LinkedList<String>();
		String address3 = "mergerfile2/0_1.dat";
		String address4 = "mergerfile2/1_1.dat";
		String outputDirectory1 = "mergerfile2/";
		String filename1 = "20070621merge_2.dat";
		inputDirectories2.add( address3 );
		inputDirectories2.add( address4 );
		
		Merger merger2 = new Merger( inputDirectories2, outputDirectory1 );
		merger2.launch( filename1 );
		
	}
//	public void test_mergeTwoQuotesFiles() throws Exception {
//		
//		// This is the data directory on my machine
//		// In that directory, there is a quotes directory and
//		// a trades directory, and in each of those there is
//		// one directory per day, and in each of those, there
//		// are individual data files for one stock for one
//		// day.
//		String dataDir = BASE_INPUT_DIR;
//		
//		// Create an IBM quotes reader
//		String quotesFilePathName1 = dataDir + "/quotes/20070620/IBM_quotes.binRQ";
//		final TAQTradesDBReader reader1 = new TAQTradesDBReader( quotesFilePathName1 );
//		final int id1 = 100; // This is the id of the first stock
//		
//		// Create a GE quotes reader
//		String quotesFilePathName2 = dataDir + "/quotes/20070620/MSFT_quotes.binRQ";
//		final TAQTradesDBReader reader2 = new TAQTradesDBReader( quotesFilePathName2 );
//		final int id2 = 253; // This is the id of the second stock
//		
//		// Put both quotes readers into a list
//		LinkedList<I_DBReader> readers = new LinkedList<I_DBReader>();
//		readers.add( reader1 );
//		readers.add( reader2 );
//			
//		// Define a compressed output stream for merged data
//		String outputFilePathName = BASE_OUTPUT_DIR + "/quotes1_1.gz";
//		final DataOutputStream outputFile = GZFileUtils.getGZippedFileOutputStream( outputFilePathName );
//			
//		// Instantiate merge processor, telling it where to write merged output of two quotes files
//		I_DBProcessor mergeProcessor = new I_DBProcessor() {
//			
//			/**
//			 * This is where we do something with both readers
//			 */
//			@Override
//			public boolean processReaders(
//				long sequenceNumber,
//				int  numReadersWithNewData,
//				LinkedList<I_DBReader> readers
//			) {
//				// Note that, unlike the join example that follows
//				// we do not need to have data for both readers. We
//				// need data for one, the other, or both.
//				if( reader1.getLastSequenceNumberRead() == sequenceNumber ) {
//					int nRecs = reader1.getNRecsRead();
//					try {
//						for( int i = 0; i < nRecs; i++ ) {
//							outputFile.writeInt( id1 );
//							outputFile.writeLong( reader1.getSequenceNum( i ) );
//							//outputFile.writeInt( reader1.getBidSize( i ) );
//							//outputFile.writeFloat( reader1.getBidPrice( i ) );
//							//outputFile.writeInt( reader1.getAskSize( i ) );
//							//outputFile.writeFloat( reader1.getAskPrice( i ) );
//						}
//					} catch (IOException e) {
//						return false;
//					}
//				}
//				if( reader2.getLastSequenceNumberRead() == sequenceNumber ) {
//					int nRecs = reader2.getNRecsRead();
//					try {
//						for( int i = 0; i < nRecs; i++ ) {
//							outputFile.writeInt( id2 );
//							outputFile.writeLong( reader2.getSequenceNum( i ) );
//							//outputFile.writeInt( reader2.getBidSize( i ) );
//							//outputFile.writeFloat( reader2.getBidPrice( i ) );
//							//outputFile.writeInt( reader2.getAskSize( i ) );
//							//outputFile.writeFloat( reader2.getAskPrice( i ) );
//						}
//					} catch (IOException e) {
//						return false;
//					}
//				}
//				return true;
//			}
//			
//			/**
//			 * Close output files
//			 * @throws IOException 
//			 */
//			@Override
//			public void stop() throws Exception {
//				outputFile.flush();
//				outputFile.close();
//				
//			}
//			
//		}; // End of new I_DBProcessor(...) {...}
//		
//		// Create a list of processors, which, in this case, will
//		// contain only one processor, the one we created above
//		LinkedList<I_DBProcessor> processors = new LinkedList<I_DBProcessor>();
//		processors.add( mergeProcessor );
//			
//		// Create a merge clock
//		MergeClock clock = new MergeClock( readers, processors );
//		
//		// Hand all of the readers, processors, and clock to the DBManager
//		DBManager dbm = new DBManager( readers, processors, clock );
//	
//		// Launch the DBManager
//		dbm.launch();
//
//	} // End of mergeTwoQuotesFiles
	
	/**
	 * This is an example of what I said we shouldn't do, take
	 * two different record types and join them to make one
	 * uniform record of the same type. The problem is data
	 * replication. If we have many quotes and one trade, the
	 * last trade will get replicated in each record. In this 
	 * case, I'm sending the output to the console. Note that 
	 * the time stamp on the clock and the time stamp on each 
	 * of the records that are joined may not agree. The 
	 * records can be the records that came just before the 
	 * clock's two minute tick.
	 * 
	 * @throws Exception
	 */
	/*public void test_joinQuotesAndTradesFiles() throws Exception {
		
		// Specify a base directory
		String baseDir = BASE_INPUT_DIR;
		
		// Create a list of readers
		LinkedList<I_DBReader> readers = new LinkedList<I_DBReader>();
		
		// Add IBM trades reader to the readers list
		String tradesFilePathName = baseDir + "/trades/20070620/IBM_trades.binRT";
		final TAQTradesDBReader taqTradesReader = new TAQTradesDBReader( tradesFilePathName );
		readers.add( taqTradesReader );

		// Add IBM quotes reader to the readers list
		String quotesFilePathName = baseDir + "/quotes/20070620/IBM_quotes.binRQ";
		final TAQQuotesDBReader taqQuotesReader = new TAQQuotesDBReader( quotesFilePathName );
		readers.add( taqQuotesReader );
		
		I_DBProcessor processor = new I_DBProcessor() {
	
			@Override
			public boolean processReaders(
					long sequenceNumber,
					int numReadersWithNewData, 
					LinkedList<I_DBReader> readers
			) {
				// If we have data in both readers, output data
				// We are outputting data from both readers on the 
				// same line of the console, so we need data for
				// both readers or the line will not be uniform.
				
				boolean isDataInFirstReader  = taqQuotesReader.getLastSequenceNumberRead() == sequenceNumber;
				boolean isDataInSecondReader = taqTradesReader.getLastSequenceNumberRead() == sequenceNumber;
				if( isDataInFirstReader && isDataInSecondReader ) {
					
					// Output time stamp from the clock.
					// Note that the time stamp from the readers could be same
					// or earlier than the clock. So if the clock ticks at 2
					// minute intervals, we may see data from the readers that
					// occurred before the two minute tick but not exactly on
					// it.
					System.out.format( "%d", sequenceNumber );
					
					// Output the last quote read
					int nRecs = taqQuotesReader.getNRecsRead();
					System.out.format(
						"q,%d,%d,%f,%d,%f", 
						taqQuotesReader.getSequenceNum(nRecs - 1),
						taqQuotesReader.getBidSize(nRecs - 1),
						taqQuotesReader.getBidPrice(nRecs - 1),
						taqQuotesReader.getAskSize(nRecs - 1),
						taqQuotesReader.getAskPrice(nRecs - 1)
					);
						
					// Output the last trade read
					nRecs = taqTradesReader.getNRecsRead();
					System.out.format(
						",t,%d,%d,%f", 
						taqTradesReader.getSequenceNum(nRecs - 1),
						taqTradesReader.getSize(nRecs - 1),
						taqTradesReader.getPrice(nRecs - 1)
					);

					// Output an end-of-line
					System.out.println("");
				}
				
				return true; // This processor is not finished
			}
	
			@Override
			public void stop() { }
			
		}; // End of new I_DBProcessor 
		
		// Put the processor created above into a list of processors
		LinkedList<I_DBProcessor> processors = new LinkedList<I_DBProcessor>();
		processors.add( processor );
		
		// Create a two minute clock
		
		// Compute 930AM on the day that the quotes reader starts.
		long startTime = 
			((long)taqQuotesReader.getSecondsFromEpoch()) * 1000L + 
			( TimeClock.ONE_HOUR_IN_MILLIS * 9 ) +
			( TimeClock.ONE_HOUR_IN_MILLIS / 2 );
		
		// Compute 4PM on the same day as above
		long endTime = startTime + ( 6 * TimeClock.ONE_HOUR_IN_MILLIS ) + ( TimeClock.ONE_HOUR_IN_MILLIS / 2 );
		
		// Compute two minute interval
		long interval = TimeClock.ONE_MINUTE_IN_MILLIS * 2;
		
		// Instantiate clock
		TimeClock clock = new TimeClock(
			startTime,
			endTime,
			interval
		);
		
		// Hand all of the readers, processors, and clock to the DBManager
		DBManager dbm = new DBManager( readers, processors, clock );
	
		// Launch the DBManager
		dbm.launch();
	
	}*/
	
}

