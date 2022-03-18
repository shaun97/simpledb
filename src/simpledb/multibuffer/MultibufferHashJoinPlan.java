package simpledb.multibuffer;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.materialize.*;
import simpledb.plan.Plan;
import java.lang.*;

/**
 * The Plan class for the multi-buffer version of the
 * <i>product</i> operator.
 * 
 * @author Edward Sciore
 */
public class MultibufferHashJoinPlan implements Plan {
   private Transaction tx;
   private Plan lhs, rhs;
   private Schema schema = new Schema();
   private Predicate pred;
   private int k;

   /**
    * Creates a product plan for the specified queries.
    * 
    * @param lhs the plan for the LHS query
    * @param rhs the plan for the RHS query
    * @param tx  the calling transaction
    */
   public MultibufferHashJoinPlan(Transaction tx, Plan lhs, Plan rhs, Predicate pred) {
      this.tx = tx;
      this.lhs = new MaterializePlan(tx, lhs);
      this.rhs = rhs;
      this.pred = pred;
      schema.addAll(lhs.schema());
      schema.addAll(rhs.schema());
   }

   /**
    * A scan for this query is created and returned, as follows.
    * First, the method materializes its LHS and RHS queries.
    * It then determines the optimal chunk size,
    * based on the size of the materialized RHS file and the
    * number of available buffers.
    * It creates a chunk plan for each chunk, saving them in a list.
    * Finally, it creates a multiscan for this list of plans,
    * and returns that scan.
    * 
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan leftscan = lhs.open();
      // TODO Will incur i/o might need to change
      TempTable ttr = copyRecordsFrom(rhs);
      int filesize = tx.size(ttr.tableName() + ".tbl");
      k = tx.availableBuffs() - 1;
      // Check if k rhs is no more than k blocks
      if (filesize < k) {
         return new MultibufferJoinScan(tx, leftscan, ttr.tableName(), ttr.getLayout(), pred);
      }

      // TempTable ttl = copyRecordsFrom(lhs);
      // filesize = tx.size(ttl.tableName() + ".tbl");
      // if (filesize < k) {
      // System.out.println("no need buffer filesize:" + filesize);
      // Scan prodscan = new MultibufferProductScan(tx, leftscan, ttl.tableName(),
      // ttl.getLayout());
      // return new SelectScan(prodscan, pred);
      // }

      // Split lhs
      List<TempTable> lhsPartitions = splitIntoPartitions(leftscan, lhs.schema());

      // Split rhs
      List<TempTable> rhsPartitions = splitIntoPartitions(rhs.open(), rhs.schema());

      // For each i between 0 and k-1:
      // a) Let Vi be the ith temporary table of T1.
      // b) Let Wi be the ith temporary table of T2.
      // c) Recursively perform the hashjoin of Vi and Wi.
      return new MultibufferHashJoinScan(tx, lhsPartitions, rhsPartitions, pred);
   }

   private List<TempTable> splitIntoPartitions(Scan src, Schema hashSch) {
      List<TempTable> temps = new ArrayList<>();

      // Create all the buckets
      for (int i = 0; i < k; i++) {
         TempTable currenttemp = new TempTable(tx, hashSch);
         temps.add(currenttemp);
      }

      src.beforeFirst();
      boolean hasMore = src.next();
      if (!hasMore)
         return temps;

      // Find the field in the join pred that is from this table
      String hashFldName = getHashFldName(hashSch, pred);
      if (hashFldName == null) {
         // Throw error
      }

      while (hasMore) {
         int valToHash = src.getVal(hashFldName).asInt();
         int hash = hashFn(valToHash, k);
         // System.out.println("hashing to bucket: " + hash);
         UpdateScan tempScan = temps.get(hash).open();
         copy(src, tempScan, hashSch);
         tempScan.close();
         hasMore = src.next();
      }

      return temps;
   }

   private String getHashFldName(Schema hashSch, Predicate hashPred) {
      for (String fldname : hashSch.fields()) {
         if (hashPred.equatesWithField(fldname) != null) {
            return fldname;
         }
      }
      return null;
   }

   private int hashFn(int value, int k) {
      return (int) Math.floor(value / 3) % (k - 1);
   }

   /**
    * Returns an estimate of the number of block accesses
    * required to execute the query. The formula is:
    * 
    * <pre>
    * B(product(p1, p2)) = B(p2) + B(p1) * C(p2)
    * </pre>
    * 
    * where C(p2) is the number of chunks of p2.
    * The method uses the current number of available buffers
    * to calculate C(p2), and so this value may differ
    * when the query scan is opened.
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // this guesses at the # of chunks
      int avail = tx.availableBuffs();
      int size = new MaterializePlan(tx, rhs).blocksAccessed();
      int numchunks = size / avail;
      return rhs.blocksAccessed() +
            (lhs.blocksAccessed() * numchunks);
   }

   /**
    * Estimates the number of output records in the product.
    * The formula is:
    * 
    * <pre>
    * R(product(p1, p2)) = R(p1) * R(p2)
    * </pre>
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return lhs.recordsOutput() * rhs.recordsOutput();
   }

   /**
    * Estimates the distinct number of field values in the product.
    * Since the product does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * 
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (lhs.schema().hasField(fldname))
         return lhs.distinctValues(fldname);
      else
         return rhs.distinctValues(fldname);
   }

   /**
    * Returns the schema of the product,
    * which is the union of the schemas of the underlying queries.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }

   private TempTable copyRecordsFrom(Plan p) {
      Scan src = p.open();
      Schema sch = p.schema();
      TempTable t = new TempTable(tx, sch);
      UpdateScan dest = (UpdateScan) t.open();
      while (src.next()) {
         dest.insert();
         for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
      }
      src.close();
      dest.close();
      return t;
   }

   private void copy(Scan src, UpdateScan dest, Schema scanSch) {
      dest.insert();
      for (String fldname : scanSch.fields()) {
         dest.setVal(fldname, src.getVal(fldname));
      }
   }
}
