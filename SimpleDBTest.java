

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

public class SimpleDBTest {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
				SimpleDB.init("SimpleDB");
				Block b[] = new Block[8];
				Buffer buff[] = new Buffer[8];
				for(int i = 0; i<b.length;i++)
					b[i] = new Block("junk",i);

				
				BufferMgr basicBufferMgr = SimpleDB.bufferMgr();
				try {
					
					for(int i=0; i<b.length; i++)
						buff[i] = basicBufferMgr.pin(b[i]);
					basicBufferMgr.unpin(buff[3]);
					basicBufferMgr.unpin(buff[1]);
					basicBufferMgr.unpin(buff[2]);
					basicBufferMgr.unpin(buff[0]);
					basicBufferMgr.pin(new Block("junk",9));
					basicBufferMgr.pin(new Block("junk",11));
				}
				catch (BufferAbortException e) {
					System.out.println(e.getMessage());
				}
	}

}
