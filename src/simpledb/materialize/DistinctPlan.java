package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.plan.Plan;
import simpledb.query.*;

public class DistinctPlan implements Plan {
   private Transaction tx;
   private Plan p;
   private Schema sch;
   private List<String> fields;
   
   public DistinctPlan(Transaction tx, Plan p, List<String> fields) {
      this.tx = tx;
      sch = p.schema();
      this.fields = fields;
      List<OrderInfo> orderInfo = new ArrayList<OrderInfo>();
      for (String field : fields){
    	  orderInfo.add(new OrderInfo(field, "asc"));
      }
      this.p = new SortPlan(tx, p, orderInfo);
      System.out.println("Distinct Plan");
   }
   
   public Scan open() {
      Scan src = p.open();
      return new DistinctScan(src, fields);
   }

   public int blocksAccessed() {
      // does not include the one-time cost of sorting
      Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
      return mp.blocksAccessed();
   }
   
   public int recordsOutput() {
      return p.recordsOutput();
   }

   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   public Schema schema() {
      return sch;
   }
}
