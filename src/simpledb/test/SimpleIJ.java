package simpledb.test;

import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.jdbc.embedded.EmbeddedMetaData;
import simpledb.jdbc.network.NetworkDriver;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class SimpleIJ {
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);

		SimpleDB db = new SimpleDB("studentdblarge");
		Transaction tx = db.newTx();
		Planner planner = db.planner();
		try {
			System.out.print("\nSQL> ");
			while (sc.hasNextLine()) {
				// process one line of input
				String cmd = sc.nextLine().trim();
				if (cmd.startsWith("exit"))
					break;
				else if (cmd.startsWith("select"))
					doQuery(planner, tx, cmd);
				else
					doUpdate(planner, tx, cmd);
				System.out.print("\nSQL> ");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		sc.close();
		tx.commit();
	}

	private static void doQuery(Planner planner, Transaction tx, String cmd) {
		Plan p = planner.createQueryPlan(cmd, tx);
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
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
		}
	}

	private static void doUpdate(Planner planner, Transaction tx, String cmd) {
		try {
			int howmany = planner.executeUpdate(cmd, tx);
			System.out.println(howmany + " records processed");
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
		}
	}
}