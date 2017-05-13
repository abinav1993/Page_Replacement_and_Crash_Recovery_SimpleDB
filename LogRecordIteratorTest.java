

import static simpledb.tx.recovery.LogRecord.CHECKPOINT;
import static simpledb.tx.recovery.LogRecord.COMMIT;

import java.util.ArrayList;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.RecoveryMgr;
import simpledb.tx.recovery.LogRecord;
import simpledb.tx.recovery.LogRecordIterator;

public class LogRecordIteratorTest {
	public static void main(String[] args) {
		SimpleDB.init("SimpleDBtest2");
		Block b[] = new Block[8];
		for(int i = 0; i<b.length;i++)
			b[i] = new Block("LogIteratorRecoveryTest",i);
		
		Buffer buff[] = new Buffer[8];
		BufferMgr basicBufferMgr = SimpleDB.bufferMgr();
		int txid=2;
		RecoveryMgr rm = new RecoveryMgr(2);
		int lsn=-1;
		try {
			
			buff[0] = basicBufferMgr.pin(b[0]);
			lsn = rm.setInt(buff[0], 4, 1234);
			buff[0].setInt(4, 1234, txid, lsn);
			
			buff[1] = basicBufferMgr.pin(b[1]);
			lsn = rm.setInt(buff[1], 4, 12345);
			buff[1].setInt(4, 12345, txid, lsn);
			
			buff[2] = basicBufferMgr.pin(b[2]);
			lsn = rm.setString(buff[2],1,"SimpleDB");
			buff[2].setString(1,"SimpleDB", txid, lsn);
				
			buff[3] = basicBufferMgr.pin(b[3]);
			lsn = rm.setString(buff[3],1,"LogIterator");
			buff[3].setString(1,"LogIterator", txid, lsn);
			
			LogRecordIterator iter = new LogRecordIterator(true);
			
			while (iter.hasNextForward()) {
				System.out.println(iter.nextForward());
	        }
		}
		catch (BufferAbortException e) {
			System.out.println(e.getMessage());
		}
	}

}
