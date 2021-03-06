package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.AvgFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.materialize.MinFn;
import simpledb.materialize.SumFn;
import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * 
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;

   public Parser(String s) {
      lex = new Lexer(s);
   }

   // Methods for parsing predicates, terms, expressions, constants, and fields

   public String field() {
      return lex.eatId();
   }

   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }

   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }

   // public Term term() {
   // Expression lhs = expression();
   // lex.eatDelim('=');
   // Expression rhs = expression();
   // return new Term(lhs, rhs);
   // }

   //// Modified method (Lab 1)
   public Term term() {
      Expression lhs = expression();
      String opr = lex.eatOpr();
      Expression rhs = expression();
      return new Term(lhs, rhs, opr);
   }

   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }

   // Methods for parsing queries

   public QueryData query() {
      lex.eatKeyword("select");
      boolean isDistinct = false;
      if (lex.matchKeyword("distinct")) {
         lex.eatKeyword("distinct");
         isDistinct = true;
      }
      List<String> fields = selectList();
      lex.eatKeyword("from");
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      List<OrderInfo> orderInfos = new ArrayList<OrderInfo>();
      List<String> groupByInfos = new ArrayList<String>();
      // Process and get aggregate function
      List<AggregationFn> aggFnsInfo = new ArrayList<AggregationFn>();
      for (int i = 0; i < fields.size(); i++) {
         String field = fields.get(i);
         if (fields.get(i).contains("(")) {
            String aggFnName = field.substring(0, field.indexOf("("));
            String fieldName = field.substring(field.indexOf("(") + 1, field.indexOf(")"));
            AggregationFn fn = getAggregationFn(fieldName, aggFnName);
            aggFnsInfo.add(fn);
            fields.set(i, fn.fieldName());
         }
      }

      if (lex.matchKeyword("join")) {
         lex.eatKeyword("join");
         tables.addAll(tableList());
         lex.eatKeyword("on");
         pred = (predicate());
      }
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      if (lex.matchKeyword("group")) {
         lex.eatKeyword("group");
         if (lex.matchKeyword("by")) {
            lex.eatKeyword("by");
         }
         groupByInfos = selectList();
      }
      if (lex.matchKeyword("order")) {
         lex.eatKeyword("order");
         if (lex.matchKeyword("by")) {
            lex.eatKeyword("by");
         }
         orderInfos = orderList();
      }

      return new QueryData(fields, tables, pred, orderInfos, groupByInfos, aggFnsInfo, isDistinct);

   }

   private AggregationFn getAggregationFn(String fieldName, String aggFnName) {
      switch (aggFnName.toUpperCase()) {
         case "COUNT":
            return new CountFn(fieldName);
         case "MAX":
            return new MaxFn(fieldName);
         case "MIN":
            return new MinFn(fieldName);
         case "AVG":
            return new AvgFn(fieldName);
         case "SUM":
            return new SumFn(fieldName);
         default:
            // TODO Throw error later
            return new SumFn(fieldName);
      }
   }

   private List<String> selectList() {
      List<String> L = new ArrayList<String>();
      String temp = field();

      // check if got bracket in next token
      if (lex.matchDelim('(')) {
         lex.eatDelim('(');
         temp = temp + '(' + field() + ')';
         lex.eatDelim(')');
      }
      L.add(temp);
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      }
      return L;
   }

   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }

   private List<OrderInfo> orderList() {
      List<OrderInfo> L = new ArrayList<OrderInfo>();
      String field = lex.eatId();
      String orderType = "asc";
      if (lex.matchKeyword("asc") || lex.matchKeyword("desc"))
         orderType = lex.eatOrderType();
      L.add(new OrderInfo(field, orderType));

      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(orderList());
      }
      return L;
   }

   // Methods for parsing the various update commands

   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }

   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }

   // Method for parsing delete commands

   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }

   // Methods for parsing insert commands

   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }

   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }

   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }

   // Method for parsing modify commands

   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }

   // Method for parsing create table commands

   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }

   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }

   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }

   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      } else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }

   // Method for parsing create view commands

   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }

   // Method for parsing create index commands

   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');

      // modified lines
      if (lex.matchKeyword("using")) {
         lex.eatKeyword("using");
         String idxtype = lex.eatIdxType();
         return new CreateIndexData(idxname, tblname, fldname, idxtype);
      }

      return new CreateIndexData(idxname, tblname, fldname);
   }
}
