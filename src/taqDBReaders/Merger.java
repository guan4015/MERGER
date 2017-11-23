package taqDBReaders;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;

import dbReaderFramework.DBManager;
import dbReaderFramework.I_DBProcessor;
import dbReaderFramework.I_DBReader;
import dbReaderFramework.MergeClock;
import taqDBReaders.GZFileUtils;

/**
 * This class deals with the merger business
 * @author xiaog
 *
 */
public class Merger {
	
	protected LinkedList<String> _inputDirectories;
	protected String _outputDirectory;
	protected LinkedList<DATReader> _readers;
	protected LinkedList<I_DBReader> _readersDB;
	
	public Merger( LinkedList<String> inputDirectories,
			       String outputDirectory ) {
		_inputDirectories = inputDirectories;
		_outputDirectory = outputDirectory;
		_readers = new LinkedList<DATReader>();
		_readersDB = new LinkedList<I_DBReader>();
	}
	
	// The following method launch the merger algorithm
	// It receives the name of file
	public void launch ( String name ) throws Exception {
		String outputFilePathName = _outputDirectory + name;
		DataOutputStream outputFile = GZFileUtils.getGZippedFileOutputStream( outputFilePathName );

		for ( String directory : _inputDirectories ) {
			DATReader reader1 = new DATReader( directory );
			_readers.add( reader1 );
			_readersDB.add( reader1 );
		}
		// create the processor
		DATProcessor mergeProcessor = new DATProcessor( outputFile, _readers ) ;
        // add the processor
		LinkedList<I_DBProcessor> processors = new LinkedList<I_DBProcessor>();
		processors.add( mergeProcessor );
			
		// Create a merge clock
		MergeClock clock = new MergeClock( _readersDB, processors );
		
		// Hand all of the readers, processors, and clock to the DBManager
		DBManager dbm = new DBManager( _readersDB, processors, clock );
	
		// Launch the DBManager
		dbm.launch();
		
	}

}
