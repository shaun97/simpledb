package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.record.*;

/**
 * The Scan class for the multi-buffer version of the
 * <i>product</i> operator.
 * 
 * @author Edward Sciore
 */
public class MultibufferJoinScan implements Scan {
   private Transaction tx;
   private Scan lhsscan, rhsscan = null, prodscan, prodselectscan;
   private String filename;
   private Layout layout;
   private int chunksize, nextblknum, filesize;
   private Predicate pred;

   public MultibufferJoinScan(Transaction tx, Scan lhsscan, String tblname, Layout layout, Predicate pred) {
      this.tx = tx;
      this.lhsscan = lhsscan;
      this.filename = tblname + ".tbl";
      this.layout = layout;
      this.pred = pred;
      filesize = tx.size(filename);
      int available = tx.availableBuffs();
      chunksize = BufferNeeds.bestFactor(available, filesize);
      beforeFirst();
   }

   public void beforeFirst() {
      nextblknum = 0;
      useNextChunk();
   }

   public boolean next() {
      // empty table
      if (prodselectscan == null)
         return false;
      while (!prodselectscan.next())
         if (!useNextChunk())
            return false;
      return true;
   }

   public void close() {
      if (prodselectscan == null)
         return;
      prodselectscan.close();
   }

   public Constant getVal(String fldname) {
      return prodselectscan.getVal(fldname);
   }

   public int getInt(String fldname) {
      return prodselectscan.getInt(fldname);
   }

   public String getString(String fldname) {
      return prodselectscan.getString(fldname);
   }

   public boolean hasField(String fldname) {
      return prodselectscan.hasField(fldname);
   }

   private boolean useNextChunk() {
      if (nextblknum >= filesize) // first tuple of the block
         return false;
      if (rhsscan != null)
         rhsscan.close();
      int end = nextblknum + chunksize - 1; // getting the end tuple of the block
      if (end >= filesize) // if
         end = filesize - 1;
      rhsscan = new ChunkScan(tx, filename, layout, nextblknum, end); // outer that gets chunked
      lhsscan.beforeFirst(); // smaller table 1 buffer one
      prodscan = new ProductScan(lhsscan, rhsscan);
      prodselectscan = new SelectScan(prodscan, pred);
      nextblknum = end + 1;
      return true;
   }
}
