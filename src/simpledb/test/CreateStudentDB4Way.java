package simpledb.test;

import java.sql.*;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateStudentDB4Way {
	public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("studentdb4way");
			Transaction tx = db.newTx();
			Planner planner = db.planner();

			String cmd = "create table GRADSTUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			planner.executeUpdate(cmd, tx);
			System.out.println("Table GRADSTUDENT created.");

			// cmd = "create index sid on GRADSTUDENT(sid) using btree";
			// planner.executeUpdate(cmd, tx);
			// System.out.println("Indexing majorid");

			cmd = "insert into GRADSTUDENT(SId, SName, MajorId, GradYear) values ";
			String[] gradstudvals = {
					"(7, 'john', 30, 2021)",
					"(3, 'max', 10, 2022)",
					"(6, 'kim', 20, 2020)",
					"(5, 'bob', 30, 2020)", };
			for (int i = 0; i < gradstudvals.length; i++)
				planner.executeUpdate(cmd + gradstudvals[i], tx);
			System.out.println("GRADSTUDENT records inserted.");

			cmd = "create table GRADDEPT(DId int, DName varchar(8))";
			planner.executeUpdate(cmd, tx);
			System.out.println("DEPT records inserted.");

			cmd = "insert into GRADDEPT(DId, DName) values ";
			String[] graddeptvals = { "(10, 'compsci')",
					"(20, 'math')",
					"(30, 'drama')" };
			for (int i = 0; i < graddeptvals.length; i++)
				planner.executeUpdate(cmd + graddeptvals[i], tx);
			System.out.println("GRADDEPT records inserted.");

			cmd = "create table GRADCOURSE(CId int, Title varchar(20), DeptId int)";
			planner.executeUpdate(cmd, tx);
			System.out.println("Table GRADCOURSE created.");

			cmd = "insert into GRADCOURSE(CId, Title, DeptId) values ";
			String[] gradcoursevals = { "(12, 'db systems', 10)",
					"(22, 'compilers', 10)",
					"(32, 'calculus', 20)",
					"(42, 'algebra', 20)",
					"(52, 'acting', 30)",
					"(62, 'elocution', 30)" };
			for (int i = 0; i < gradcoursevals.length; i++)
				planner.executeUpdate(cmd + gradcoursevals[i], tx);
			System.out.println("GRADCOURSE records inserted.");

			cmd = "create table GRADENROLL(EId int, StudentId int, courseID int, Grade varchar(2))";
			planner.executeUpdate(cmd, tx);
			System.out.println("Table GRADENROLL created.");

			cmd = "insert into GRADENROLL(EId, StudentId, courseID, Grade) values ";
			String[] gradenrollvals = {
					"(64, 6, 32, 'A' )",
					"(74, 3, 22, 'A')",
					"(84, 3, 32, 'C' )",
					"(94, 5, 62, 'B+')",
					"(104, 7, 62, 'B' )",
					"(114, 7, 52, 'A' )",
			};
			for (int i = 0; i < gradenrollvals.length; i++)
				planner.executeUpdate(cmd + gradenrollvals[i], tx);
			System.out.println("ENROLL records inserted.");

			tx.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
