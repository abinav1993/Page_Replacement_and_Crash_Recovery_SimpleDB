package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import simpledb.file.*;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecord;

import java.util.Stack;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse and forward order.
 * 
 * @author Edward Sciore
 */
public class LogIterator implements Iterator<BasicLogRecord> {
   private Block blk;
   private Page pg = new Page();
   private int currentrec;
   private ArrayList<LogRecord> records = new ArrayList<>();
   
   /**
    * Creates an forward and backward iterator for the records in the log file,
    * positioned after the last log record.
    * This constructor is called exclusively by
    * {@link LogMgr#iterator()}.
    * @param blk The block to start the iterator at
    */
   LogIterator(Block blk) {
      this.blk = blk;
      pg.read(blk);
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
	

   /**
    * Determines if the current log record
    * is the earliest record in the log file.
    * @return true if there is an earlier record
    */
   public boolean hasNext() {
      return currentrec>0 || blk.number()>0;
   }
   
   	/**
	 * Determines if the current log record
	 * is the earliest record in the log file.
	 * @return true if there is an earlier record
	 */
	public boolean hasNextForward() {
		return records.size() > 0;
	}
   
   /**
    * Moves to the next log record in reverse order.
    * If the current log record is the earliest in its block,
    * then the method moves to the next oldest block,
    * and returns the log record from there.
    * @return the next earliest log record
    */
   public BasicLogRecord next() {
      if (currentrec == 0) 
         moveToNextBlock();
      currentrec = pg.getInt(currentrec);
      return new BasicLogRecord(pg, currentrec+INT_SIZE);
   }
   
   public void setArrayList(ArrayList<LogRecord> records){
	   this.records.addAll(records);
   }
   
   	/**
	 * Moves to the next log record in forward order.
	 * @return the next earliest log record in forward direction
	 */
	public LogRecord nextForward() {
		LogRecord lr = records.get(0);
		records.remove(0);
		return lr;
	}
	
	public Block currentBlock()
   {
	   return blk;
   }
   
   public int currentRec()
   {
	   return currentrec;
   }
   public void remove() {
      throw new UnsupportedOperationException();
   }
   
   /**
    * Moves to the next log block in reverse order,
    * and positions it after the last record in that block.
    */
   private void moveToNextBlock() {
      blk = new Block(blk.fileName(), blk.number()-1);
      pg.read(blk);
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
   
   /**
    * Moves to the next log block in forward order,
    * and positions at the first record in that block.
    */
   private void moveToNextForwardBlock() {
      blk = new Block(blk.fileName(), blk.number()+1);
      pg.read(blk);
      currentrec = INT_SIZE;
   }
}
