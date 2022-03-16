package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * The Scan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
/**
 * @author sciore
 *
 */
public class DistinctScan implements Scan {
   private Scan src;
   private List<String> fields;
   private List<Constant> previous = new ArrayList<Constant>();

   /**
    * Create a sort scan, given a list of 1 or 2 runs.
    * If there is only 1 run, then s2 will be null and
    * hasmore2 will be false.
    * 
    * @param runs the list of runs
    * @param comp the record comparator
    */
   public DistinctScan(Scan src, List<String> fields) {
      this.src = src;
      this.fields = fields;
   }

   /**
    * Position the scan before the first record in sorted order.
    * Internally, it moves to the first record of each underlying scan.
    * The variable currentscan is set to null, indicating that there is
    * no current scan.
    * 
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      src.beforeFirst();
   }

   /**
    * Move to the next record in sorted order.
    * First, the current scan is moved to the next record.
    * Then the lowest record of the two scans is found, and that
    * scan is chosen to be the new current scan.
    * 
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      boolean isDistinct = false;
      while (src.next()) { 
         if (previous.size() == 0) {
            for (String field : fields) {
               Constant val = src.getVal(field);
               previous.add(val);
            }
            return true;
         }
         int i = 0;
         for (String field : fields) {
            Constant val = src.getVal(field);
            if (!val.equals(previous.get(i))) {
               isDistinct = true;
            }
            previous.set(i, val);
            i++;
         }
         if (isDistinct) return true;
      }
      return false;
   }

   /**
    * Close the two underlying scans.
    * 
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      src.close();
   }

   /**
    * Get the Constant value of the specified field
    * of the current scan.
    * 
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      return src.getVal(fldname);
   }

   /**
    * Get the integer value of the specified field
    * of the current scan.
    * 
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      return src.getInt(fldname);
   }

   /**
    * Get the string value of the specified field
    * of the current scan.
    * 
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      return src.getString(fldname);
   }

   /**
    * Return true if the specified field is in the current scan.
    * 
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return src.hasField(fldname);
   }

   /**
    * Save the position of the current record,
    * so that it can be restored at a later time.
    */
   // public void savePosition() {
   // RID rid1 = s1.getRid();
   // RID rid2 = (s2 == null) ? null : s2.getRid();
   // savedposition = Arrays.asList(rid1,rid2);
   // }

   /**
    * Move the scan to its previously-saved position.
    */
   // public void restorePosition() {
   // RID rid1 = savedposition.get(0);
   // RID rid2 = savedposition.get(1);
   // s1.moveToRid(rid1);
   // if (rid2 != null)
   // s2.moveToRid(rid2);
   // }
}
