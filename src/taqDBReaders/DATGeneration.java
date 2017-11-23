package taqDBReaders;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;

import dbReaderFramework.DBManager;
import dbReaderFramework.I_DBProcessor;
import dbReaderFramework.I_DBReader;
import dbReaderFramework.MergeClock;
// Import the array list
import java.util.ArrayList;
import java.util.HashMap;
/***
 * This class rewrite the files we have on hand. Basically, it will read the original
 * files and output .dat typed files. To initialize the class, we have to specify
 * the base input directory and output directory as well as the input subdirectories which
 * specify the concrete locations of the file
 * @author xiaog
 *
 */
public class DATGeneration {
	
	// member variables
	// Specify the input and output directory
	protected String _BASE_INPUT_DIRECTORY;
	protected String _BASE_OUTPUT_DIRECTORY;
	// Specify the input and output concrete directories 
	protected ArrayList<String> _inputFiles;
	protected ArrayList<String> _outputFiles;
	protected LinkedList<TAQTradesDBReader> _readers;
	protected HashMap<TAQTradesDBReader, String> _readerDirectory;
	protected HashMap<String, Short> _directoryStockID;
	protected final HashMap<String, DataOutputStream> _directoryOutput;
	
	
	// constructor
	public DATGeneration( String base_input_directory, String base_out_directory ) {
		_BASE_INPUT_DIRECTORY = base_input_directory;
		_BASE_OUTPUT_DIRECTORY = base_out_directory;
		_inputFiles = new ArrayList<String>();
		_outputFiles = new ArrayList<String>();
		_readers = new LinkedList<TAQTradesDBReader>();
		_readerDirectory = new HashMap<TAQTradesDBReader, String>();
		_directoryStockID = new HashMap<String, Short>();
		_directoryOutput = new HashMap<String, DataOutputStream>();
		
	}
	
	// Add file directories
	public void AddInputFileDirectory( String subdirectory ) {
		String temp = _BASE_INPUT_DIRECTORY + subdirectory;
		_inputFiles.add( temp );
	}
	
	public void AddOutputFileDirectory( String subdirectory ) {
		String temp = _BASE_OUTPUT_DIRECTORY + subdirectory;
		_outputFiles.add( temp );
	}
	
	public ArrayList<String> getInputFileFDirectory(){
		return _inputFiles;
	}
	
	public ArrayList<String> getOutFileDirectory(){
		return _outputFiles;
	}
	
	// generate reader list
	public void generateReadersList() throws IOException {
		// This function is specified to read TAQ file
		for (int i = 0; i < _inputFiles.size(); i++ ) {
			// Add the reader into readers
			_readers.add( new TAQTradesDBReader( _inputFiles.get(i) ) );	
		}
	}
	
	public void generateDefaultOutputList() throws IOException {
		// This function is specified to convert the reader ID to stock ID 
		// This is method is default (so you can manually add the output directory)
		// It uses the _BASE_OUT_DIRECTORY
		short index = 0;
		for ( TAQTradesDBReader reader : _readers ) {
			Short index_stock = new Short( index );
			String directory = _BASE_OUTPUT_DIRECTORY + index_stock.toString() + "_1.dat";
			_outputFiles.add( directory );
			_directoryStockID.put( directory, index );
			_readerDirectory.put( reader, directory );
			DataOutputStream outputFile = GZFileUtils.getGZippedFileOutputStream( directory );
			_directoryOutput.put( directory, outputFile );
			index += 1;
		}	
	}
	
	/**
	 * The following comments area suggest alternative implementation
	 */
	
/*	protected I_DBProcessor _writeProcessor = new I_DBProcessor() {
		
		*//**
		 * This is where we do something with both readers
		 *//*
		@Override
		public boolean processReaders(
				long sequenceNumber,
				int  numReadersWithNewData,
				LinkedList<I_DBReader> readers
				) {
			// process the readers
			for ( TAQTradesDBReader reader : _readers ) {
				
				if ( reader.getLastSequenceNumberRead() == sequenceNumber ) {
					// Obtain the number of records
					int nRecs = reader.getNRecsRead();
					// retrieve the address
					String address = _readerDirectory.get( reader );
					Short id = _directoryStockID.get( address );
		
					try {
						for( int i = 0; i < nRecs; i++ ) {
							//System.out.format("%d, %d, %d, %f\n", i,reader1.getSequenceNum( i ),reader1.getSize( i ),reader1.getPrice( i ) );
							_directoryOutput.get(address).writeLong( reader.getMillisFromMidnight(i) );
							_directoryOutput.get(address).writeShort( id );
							_directoryOutput.get(address).writeInt( reader.getSize(i) );
							_directoryOutput.get(address).writeFloat( reader.getPrice(i) );
						}
					} catch (IOException e) {
						return false;
					}			
				}
			}
			return true;
		}*/
/*		
		@Override
		public void stop() throws Exception {
			
			for ( DataOutputStream outputFile : _directoryOutput.values() ) {
				outputFile.flush();
				outputFile.close();
			}
			
		}
	};  // End of new I_DBProcessor(...) {...}
*/	
	// Launch the whole process
	/**
	 * The following method is to rewrite the whole file.
	 * @throws Exception
	 */
	public void lauch() throws Exception {
		// The comments area suggests alternative implementation
//		LinkedList<I_DBProcessor> processors = new LinkedList<I_DBProcessor>();
//		processors.add( _writeProcessor );
//		
//		LinkedList<I_DBReader> readers = new LinkedList<I_DBReader>();
//		for ( TAQTradesDBReader reader : _readers ) {
//			readers.add( reader );
//		}
//		
//		MergeClock clock = new MergeClock( readers, processors );
//		DBManager dbm = new DBManager( readers, processors, clock );
//		dbm.launch();
		for(TAQTradesDBReader reader : _readers) {
			Short id = _directoryStockID.get( _readerDirectory.get(reader) );
			DataOutputStream outputFile = _directoryOutput.get(  _readerDirectory.get(reader) );
			for(int j = 0; j < reader._taq.getNRecs(); j++) {
				outputFile.writeLong( reader.getMillisFromMidnight(j) );
				outputFile.writeShort( id );
				outputFile.writeInt( reader.getSize(j) );
				outputFile.writeFloat(reader.getPrice(j) );
			}
		}
	}

}

