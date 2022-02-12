package simpledb.test;
import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class FindMajors {
	public static void main(String[] args) {
		try {
			System.out.print("Enter a department name: ");
			Scanner sc = new Scanner(System.in);
			String major = sc.next();
			sc.close();
			System.out.println("Here are the " + major + " majors");
			System.out.println("Name\tGradYear");

			String qry = "select sname, gradyear "
					+ "from student, dept "
					+ "where did = majorid "
					+ "and dname = '" + major + "'";

			SimpleDB db = new SimpleDB("studentdb2");
			Transaction tx = db.newTx();
			Planner planner = db.planner();

			Plan p = planner.createQueryPlan(qry, tx);
			Scan s = p.open();

			while (s.next()) {
				String sname = s.getString("sname");
				int gradyear = s.getInt("gradyear");
				System.out.println(sname + "\t" + gradyear);
			}
			s.close();
			tx.commit();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
