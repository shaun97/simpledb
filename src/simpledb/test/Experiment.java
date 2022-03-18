package simpledb.test;

import java.sql.Types;

import simpledb.jdbc.embedded.EmbeddedMetaData;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Experiment {
     

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("studentdblarge");
        Transaction tx = db.newTx();
        Planner planner = db.planner();
        String query = "select title, deptid, dname from course, dept on deptid = did";
        int numRuns = 10;
        
        long average = 0;

        for (int j = 0; j < numRuns; j++) {

            long start = System.currentTimeMillis();
            Plan p = planner.createQueryPlan(query, tx);
            Scan s = p.open();

            try {
                EmbeddedMetaData md = new EmbeddedMetaData(p.schema());
                int numcols = md.getColumnCount();
                int totalwidth = 0;
                // print header
                for (int i = 1; i <= numcols; i++) {
                    String fldname = md.getColumnName(i);
                    int width = md.getColumnDisplaySize(i);
                    totalwidth += width;
                    String fmt = "%" + width + "s";
                    System.out.format(fmt, fldname);
                }
                System.out.println();
                for (int i = 0; i < totalwidth; i++)
                    System.out.print("-");
                System.out.println();

                // print records
                while (s.next()) {
                    for (int i = 1; i <= numcols; i++) {
                        String fldname = md.getColumnName(i);
                        int fldtype = md.getColumnType(i);
                        String fmt = "%" + md.getColumnDisplaySize(i);
                        if (fldtype == Types.INTEGER) {
                            int ival = s.getInt(fldname);
                            System.out.format(fmt + "d", ival);
                        } else {
                            String sval = s.getString(fldname);
                            System.out.format(fmt + "s", sval);
                        }
                    }
                    System.out.println();
                }
                s.close();
                long end = System.currentTimeMillis();
                System.out.println("time lapsed: " + (end - start) + "ms");

                average += end - start;
            } catch (Exception e) {
                System.out.println("Exception: " + e.getMessage());
            }
        }
        average = average / numRuns;
        System.out.println("\n\naverage time lapsed: " + average + "ms");

    }
}
