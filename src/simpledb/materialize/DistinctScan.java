package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

public class DistinctScan implements Scan {
   private Scan src;
   private List<String> fields;
   private List<Constant> previous = new ArrayList<Constant>();

   public DistinctScan(Scan src, List<String> fields) {
      this.src = src;
      this.fields = fields;
   }

   public void beforeFirst() {
      src.beforeFirst();
   }

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

   public void close() {
      src.close();
   }

   public Constant getVal(String fldname) {
      return src.getVal(fldname);
   }

   public int getInt(String fldname) {
      return src.getInt(fldname);
   }

   public String getString(String fldname) {
      return src.getString(fldname);
   }

   public boolean hasField(String fldname) {
      return src.hasField(fldname);
   }
}
