package simpledb.opt;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.MergeJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.multibuffer.MultibufferJoinPlan;
import simpledb.multibuffer.MultibufferHashJoinPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * 
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String, IndexInfo> indexes;
   private Transaction tx;
   private String tblname;

   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * 
    * @param tblname the name of the table
    * @param mypred  the query predicate
    * @param tx      the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred = mypred;
      this.tx = tx;
      myplan = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes = mdm.getIndexInfo(tblname, tx);
      this.tblname = tblname;
   }

   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * 
    * @return a select plan for the table.
    */

   public Plan makeSelectPlan() {
      if (mypred.selectSubPred(myschema) == null) {
         Plan p = makeIndexSelect();
         if (p == null) {
            p = myplan;
         }
         return addSelectPred(p);
      } else {
         return addSelectPred(myplan);
      }
   }

   /**
    * Constructs a join plan of the specified plan
    * and the table. The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    *
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null)
         return null;
      System.out.println("Index Cond: (" + joinpred.toString() + ")");
      // TODO heuristics for choosing
      Plan p = makeMultibufferHashJoin(current, currsch);
      // Plan p = makeMultibufferJoin(current, currsch);
      if (p == null)
         p = makeIndexJoin(current, currsch);
      if (p == null)
         p = makeMergeJoin(current, currsch);
      if (p == null)
         p = makeProductJoin(current, currsch);
      return p;
   }

   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * 
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }

   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            // System.out.println("Index using " + fldname + " on " + tblname);
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }

   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }

   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }

   private Plan makeMergeJoin(Plan current, Schema currsch) {
      for (String fldname : this.myschema.fields()) {
         String fldname2 = mypred.equatesWithField(fldname);
         if (fldname != null && currsch.hasField(fldname2)) {
            Plan p = new MergeJoinPlan(tx, myplan, current, fldname, fldname2);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }

   private Plan makeMultibufferJoin(Plan current, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      System.out.println("Join Cond: (" + joinpred.toString() + ")");
      Plan p = addSelectPred(myplan); // TODO do we need?
      return new MultibufferJoinPlan(tx, current, p, joinpred);
   }

   private Plan makeMultibufferHashJoin(Plan current, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      Plan p = addSelectPred(myplan);
      return new MultibufferHashJoinPlan(tx, current, p, joinpred);
   }

   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null) {
         System.out.println("Select Cond: (" + selectpred.toString() + ")");
         return new SelectPlan(p, selectpred);
      } else
         return p;
   }

   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null) {
         System.out.println("Join Cond: (" + joinpred.toString() + ")");
         return new SelectPlan(p, joinpred);
      }

      else
         return p;
   }
}
