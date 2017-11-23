package taqDBReaders;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import dbReaderFramework.I_DBReader;
import taqDBReaders.GZFileUtils;

/**
 * The following class allows us to read the .dat file without pulling them
 * all into the memory
 * 
 * @author xiaog
 *
 */
public class DATReader implements I_DBReader{
	
	protected int              _recCount;     
	protected int              _nRecsRead;    
	protected boolean          _isFinished;
	protected long             _lastSequenceNumberRead;
	protected long             _nextSequenceNumber; 
    public DataInputStream     _input;
    
    protected ArrayList<Long>    _MillisecondsFromMidnight;
    protected ArrayList<Short>   _id;
    protected ArrayList<Integer> _size;
    protected ArrayList<Float>   _price;
     
    
    // The following initialize the class
    public DATReader( String filePathName ) throws IOException {
    	_input =  GZFileUtils.getGZippedFileInputStream( filePathName ); 
    	_recCount = 0;
    	_nRecsRead = 0;
    	_lastSequenceNumberRead = 0;
    	_nextSequenceNumber = _input.readLong();
    	_isFinished = false;
    	
		_MillisecondsFromMidnight = new ArrayList<Long>();
		_id = new ArrayList<Short>();
		_size = new ArrayList<Integer>();
		_price = new ArrayList<Float>();
    }
    
    
    
	@Override
	public int readChunk( long sequenceNum ) {
		
		// Every time we read the file up to a sequence number and
		// we generate the array list for the following four fields
		if(_isFinished) return 0;
		
		// clear the elements in the original lists
		_MillisecondsFromMidnight.clear();
		_id.clear();
		_size.clear();
		_price.clear();
		int count = 0;
        
		// Iterate over records until we hit last record in data or we hit
		// a sequence number that is higher than the target sequence number
		while(!_isFinished ) {
			try {				
				if( _nextSequenceNumber <= sequenceNum ){
					// if the next sequence number is less than given one,
					// we iterate this process
					try {
						short id = _input.readShort();
					    int size = _input.readInt();
					    float price = _input.readFloat();
					
						_MillisecondsFromMidnight.add( _nextSequenceNumber );
	                    _id.add( id );
	                    _size.add( size );
	                    _price.add( price );
	                    ++count;
	                    _nextSequenceNumber = _input.readLong();
	                
	                } catch( EOFException e ){	
	                	// catch the tail of the document
	                	_isFinished = true;
	                	break;
	                }
		        } else { 
		         	
					break;
				}
			} 			
			catch (IOException e1) {
				e1.printStackTrace();
			} 
		
		}
		
		_nRecsRead = count;
		_recCount += count;
		
		// Save the sequence number that was just read
		_lastSequenceNumberRead = sequenceNum;
		
		// Save number of records red and return it
		return _nRecsRead;
    }

	@Override
	public long getSequenceNumber() {
		return _nextSequenceNumber;
	}
	
	
	/**
     Stop this process if all the elements have been read
	 */
	@Override
	public void stop() {
		try {
			_input.close();
			_isFinished = true;
		} catch ( IOException e ) {
			e.printStackTrace();
		}
        		
	}

	@Override
	public boolean isFinished() { 
		return _isFinished; 
	}
    
	public long getLastSequenceNumberRead() { 
		return _lastSequenceNumberRead; 
	}
	
	public int getNRecsRead() { 
		return _nRecsRead; 
	}
    
    public long getNextSequenceNum(){ 
    	return  _nextSequenceNumber; 
    }
	
	public long getMillisecondsFromMidnight( int i ) { 
		return _MillisecondsFromMidnight.get(i); 
	}
	
	public short getStockId( int i ) { 
		return _id.get(i); 
	} 
	
	public int getSize( int i ) { 
		return _size.get(i); 
	}
	
	public float getPrice( int i ) { 
		return _price.get(i); 
	}

}
