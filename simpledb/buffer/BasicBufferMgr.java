package simpledb.buffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	private HashMap<Block, Buffer> bufferPoolMap;
	private int numbuffs;
	private ArrayList<Buffer> bufferList;

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		this.numbuffs = numbuffs;
		bufferPoolMap = new HashMap<Block, Buffer>();
		bufferList = new ArrayList<>();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		Iterator<Buffer> iterator = bufferPoolMap.values().iterator();
		while (iterator.hasNext()) {
			Buffer buff = iterator.next();
			if (buff.isModifiedBy(txnum))
				buff.flush();
		}
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null) {
				return null;
			}
			if (buff.block() != null) {
				bufferPoolMap.remove(buff.block());
				bufferList.remove(buff);
			}
			buff.assignToBlock(blk);
			bufferList.add(buff);
			bufferPoolMap.put(blk, buff);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		for(Buffer entry : bufferList){
			System.out.print(" > " + entry.block());
		}
		System.out.println("");
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null) {
			return null;
		}
		if (buff.block() != null) {
			bufferPoolMap.remove(buff.block());
			bufferList.remove(buff);
		}
		buff.assignToNew(filename, fmtr);
		bufferList.add(buff);
		bufferPoolMap.put(buff.block(), buff);
		numAvailable--;
		buff.pin();
		for(Buffer entry : bufferList){
			System.out.print(" > " + entry.block());
		}
		System.out.println("");

		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {
		if (bufferPoolMap.containsKey(blk))
			return bufferPoolMap.get(blk);
		return null;
	}

	private Buffer chooseUnpinnedBuffer() {
		Buffer newBuffer = null;
		if (bufferList.size() < numbuffs)
			newBuffer = new Buffer();
		else {
			for (Buffer buffer : bufferList) {
				if (!buffer.isPinned()) {
					newBuffer = buffer;
					break;
				}
			}
		}
		return newBuffer;
	}
}