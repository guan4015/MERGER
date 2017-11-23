package taqDBReaders;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;

import dbReaderFramework.I_DBProcessor;
import dbReaderFramework.I_DBReader;

/**
 * The following classes define a merger file processor. It receives a linkedlist
 * containing the DATReaders and then merge them together as well as a output file
 * @author xiaog
 *
 */
public class DATProcessor implements I_DBProcessor{
	
	DataOutputStream _outputFile;
	LinkedList<DATReader> _readers;
	
	public DATProcessor( DataOutputStream _outputFile, 
			             LinkedList<DATReader> readers) {
		this._outputFile = _outputFile;
		this._readers = readers;
	}

	@Override
	public boolean processReaders(
			long sequenceNumber, 
			int numReadersWithNewData, 
			LinkedList<I_DBReader> readers
		) {
		
		// Now we defines a loop and iterates it
		for ( DATReader reader : _readers ) {
			
			if ( reader.getLastSequenceNumberRead() == sequenceNumber ) {
				int nRecs = reader.getNRecsRead();
				try {
					for (int i = 0; i < nRecs; ++i) {
						_outputFile.writeLong( reader.getMillisecondsFromMidnight(i) );
						_outputFile.writeShort( reader.getStockId(i) );
						_outputFile.writeInt( reader.getSize(i) );
						_outputFile.writeFloat( reader.getPrice(i) );
					}
				} catch (IOException e) {
					return false;
				}
			}
			
		}
		return true;
	}

	@Override
	public void stop() throws Exception {
		_outputFile.flush();
		_outputFile.close();		
	}

}

