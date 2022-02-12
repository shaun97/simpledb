package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private List<OrderInfo> orderInfos;
   
   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param fields a list of field names
    */
   public RecordComparator(List<OrderInfo> orderInfos) {
      this.orderInfos = orderInfos;
   }
   
   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   public int compare(Scan s1, Scan s2) {
      for (OrderInfo orderInfo : orderInfos) {
         Constant val1 = s1.getVal(orderInfo.getField());
         Constant val2 = s2.getVal(orderInfo.getField());
         /**
          * If val1 > val2 then its > 0
          * If val2 < val 1 then its < 0
          *
          * Add a conditional to flip result if desc
          */
         int result;
         if (orderInfo.isAsc())
            result = val1.compareTo(val2);
         else
            result = val2.compareTo(val1);
         if (result != 0)
            return result;
      }
      return 0;
   }
}
