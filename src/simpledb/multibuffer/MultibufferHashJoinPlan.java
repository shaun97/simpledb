package simpledb.multibuffer;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.materialize.*;
import simpledb.plan.Plan;

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

   public MultibufferHashJoinPlan(Transaction tx, Plan lhs, Plan rhs, Predicate pred) {
      this.tx = tx;
      this.lhs = lhs;
      this.rhs = rhs;
      this.pred = pred;
      schema.addAll(lhs.schema());
      schema.addAll(rhs.schema());
   }

   public Scan open() {
      Scan leftscan = lhs.open();
      TempTable ttr = copyRecordsFrom(rhs);
      int filesize = tx.size(ttr.tableName() + ".tbl");
      k = tx.availableBuffs() - 1;
      // Check if k rhs is no more than k blocks
      if (filesize < k) {
         return new MultibufferJoinScan(tx, leftscan, ttr.tableName(), ttr.getLayout(), pred);
      }

      // Split lhs
      List<TempTable> lhsPartitions = splitIntoPartitions(leftscan, lhs.schema());

      // Split rhs
      List<TempTable> rhsPartitions = splitIntoPartitions(rhs.open(), rhs.schema());

      // need to check the schema for this one
      TempTable result = new TempTable(tx, schema);
      for (int i = 0; i < k; i++) {
         getJoinTable(lhsPartitions.get(i), rhsPartitions.get(i), result);
      }
      // For each i between 0 and k-1:
      // a) Let Vi be the ith temporary table of T1.
      // b) Let Wi be the ith temporary table of T2.
      // c) Recursively perform the hashjoin of Vi and Wi.
      return new MultibufferHashJoinScan(tx, result.open());
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
         hasMore = copy(src, tempScan, hashSch);
         tempScan.close();
      }

      return temps;
   }

   private void getJoinTable(TempTable p1, TempTable p2, TempTable result) {
      Scan src1 = p1.open();
      Scan prodscan = new MultibufferJoinScan(tx, src1, p2.tableName(), p2.getLayout(), pred);
      UpdateScan dest = result.open();

      boolean hasmore = prodscan.next();

      while (hasmore) {
         hasmore = copy(prodscan, dest, schema);
      }

      // src1.close();
      prodscan.close();
      dest.close();
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
    * Uses 2(M+N) for partitioning
    * Uses M + N for matching
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      int size1 = new MaterializePlan(tx, rhs).blocksAccessed();
      int size2 = new MaterializePlan(tx, lhs).blocksAccessed();
      return 3 * (size1 + size2);
   }

   public int recordsOutput() {
      return lhs.recordsOutput() * rhs.recordsOutput();
   }

   public int distinctValues(String fldname) {
      if (lhs.schema().hasField(fldname))
         return lhs.distinctValues(fldname);
      else
         return rhs.distinctValues(fldname);
   }

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

   private boolean copy(Scan src, UpdateScan dest, Schema scanSch) {
      dest.insert();
      for (String fldname : scanSch.fields()) {
         dest.setVal(fldname, src.getVal(fldname));
      }
      return src.next();
      
   }

   @Override
   public String toString() {
      return "Hash Join";
   }
}
