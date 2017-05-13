

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.RecoveryMgr;

public class RecoveryTest {
	public static void main(String args[]){
		SimpleDB.init("simpleDB");
		
		Block block1 = new Block("RecoveryTest", 1);
		Block block2 = new Block("RecoveryTest", 2);
		
		BufferMgr bm = new BufferMgr(3);
		Buffer buffer1 = new Buffer();
		Buffer buffer2 = new Buffer();
		
		try
		{
		    buffer1=bm.pin(block1);
			buffer2=bm.pin(block2);
		}
		catch(BufferAbortException e)
		{	
			System.out.println("\nBuffer Abort Exception: " + e.getStackTrace());
		}
		
		RecoveryMgr rm1 = new RecoveryMgr(2);
		
		int lsn1 = rm1.setInt(buffer1, 4, 6);
		buffer1.setInt(4, 6, 2, lsn1);
		int lsn2 = rm1.setInt(buffer1, 4, 10);
		buffer1.setInt(4, 10, 2, lsn2);
		
		RecoveryMgr rm2 = new RecoveryMgr(3);
		
		int lsn3 = rm2.setString(buffer2, 5, "Hello");
		buffer2.setString(5, "Hello", 3, lsn3);
		int lsn4 = rm2.setString(buffer2, 5, "World");
		buffer2.setString(5, "World", 3, lsn4);
		
		rm1.commit();
		
		rm1.recover();
		rm2.recover();
	}
}
