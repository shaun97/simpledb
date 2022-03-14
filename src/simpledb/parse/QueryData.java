package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * 
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<OrderInfo> orderInfos;
   private List<String> groupFields;
   private List<AggregationFn> aggFns;

   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.groupFields = new ArrayList<String>();
      this.orderInfos = new ArrayList<OrderInfo>();
      this.aggFns = new ArrayList<AggregationFn>();
   }

   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<OrderInfo> orderInfos,
         List<String> groupFields, List<AggregationFn> aggFns) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.orderInfos = orderInfos;
      this.groupFields = groupFields;
      this.aggFns = aggFns;
   }

   /**
    * Returns the fields mentioned in the select clause.
    *
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }

   /**
    * Returns the tables mentioned in the from clause.
    *
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }

   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * 
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

   public List<OrderInfo> orderInfos() {
      return orderInfos;
   }

   public List<String> groupFields() {
      return groupFields;
   }

   public List<AggregationFn> aggFns() {
      return aggFns;
   }

   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
