package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.record.*;
import simpledb.multibuffer.MultibufferProductScan;
import simpledb.materialize.*;
import java.util.*;
import simpledb.plan.Plan;

/**
 * The Scan class for the multi-buffer version of the
 * <i>product</i> operator.
 * 
 * @author Edward Sciore
 */
public class MultibufferHashJoinScan implements Scan {
   private Transaction tx;
   private Scan prodscan;
   private Predicate pred;
   private List<TempTable> lhsPartitions, rhsPartitions;
   private UpdateScan lhsscan;
   private int currentBucket;

   public MultibufferHashJoinScan(Transaction tx, List<TempTable> lhsPartitions, List<TempTable> rhsPartitions,
         Predicate pred) {
      this.tx = tx;
      this.pred = pred;
      this.lhsPartitions = lhsPartitions;
      this.rhsPartitions = rhsPartitions;
      beforeFirst();
   }

   /**
    * Positions the scan before the first record.
    * That is, the LHS scan is positioned at its first record,
    * and the RHS scan is positioned before the first record of the first chunk.
    *
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      currentBucket = 0;
      useNextHashPartition();
   }

   /**
    * Moves to the next record in the current scan.
    * If there are no more records in the current chunk,
    * then move to the next LHS record and the beginning of that chunk.
    * If there are no more LHS records, then move to the next chunk
    * and begin again.
    *
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      while (!prodscan.next())
         if (!useNextHashPartition())
            return false;
      return true;
   }

   /**
    * Closes the current scans.
    *
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      prodscan.close();
   }

   /**
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    *
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      return prodscan.getVal(fldname);
   }

   /**
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * 
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      return prodscan.getInt(fldname);
   }

   /**
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * 
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      return prodscan.getString(fldname);
   }

   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * 
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return prodscan.hasField(fldname);
   }

   private boolean useNextHashPartition() {
      if (lhsPartitions.size() > currentBucket && rhsPartitions.size() > currentBucket) {
         if (lhsscan != null) {
            lhsscan.close();
            // prodscan.close();
         }
         lhsscan = lhsPartitions.get(currentBucket).open();
         TempTable rhstable = rhsPartitions.get(currentBucket);

         prodscan = new MultibufferJoinScan(tx, lhsscan, rhstable.tableName(), rhstable.getLayout(), pred);
         currentBucket++;
         return true;
      } else {
         return false;
      }
   }
}
