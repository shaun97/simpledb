package simpledb.test;

import java.sql.*;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateLargeStudentDB {
	public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("studentdblarge");
			Transaction tx = db.newTx();
			Planner planner = db.planner();

			String cmd = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			planner.executeUpdate(cmd, tx);
			System.out.println("Table STUDENT created.");

			cmd = "create index sid on STUDENT(sid) using btree";
			planner.executeUpdate(cmd, tx);
			System.out.println("Indexing majorid");

			// cmd = "create index majorid on STUDENT(MajorId) using btree";
			// planner.executeUpdate(cmd, tx);
			// System.out.println("Indexing majorid");

			cmd = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			String[] studvals = {
					"(6, 'kim', 20, 2020)",
					"(7, 'art', 30, 2021)",
					"(8, 'pat', 20, 2019)",
					"(9, 'lee', 10, 2021)",
					"(1, 'joe', 10, 2021)",
					"(2, 'amy', 20, 2020)",
					"(3, 'max', 10, 2022)",
					"(4, 'sue', 20, 2022)",
					"(5, 'bob', 30, 2020)",
					"(10, 'john', 50, 2022)",
					"(12, 'patterson', 30, 2017)",
					"(11, 'tan', 40, 2023)",
					"(13, 'joey', 80, 2023)",
					"(14, 'amos', 60, 2025)",
					"(16, 'ryan', 30, 2026)",
					"(15, 'sueann', 20, 2024)",
					"(17, 'kimmy', 50, 2020)",
					"(20, 'oolong', 20, 2021)",
					"(18, 'bobby', 10, 2021)",
					"(19, 'jason', 20, 2021",
					"(23, 'jasmine', 70, 2023)",
					"(26, 'hugh', 80, 2027)",
					"(21, 'jackman', 70, 2025)",
					"(22, 'clarissa', 60, 2023)",
					"(24, 'rob', 50, 2024)",
					"(25, 'robert', 70, 2021)",
					"(27, 'tan', 40, 2018)",
					"(28, 'ah', 60, 2028)",
					"(29, 'kao', 80, 2028)",
					"(30, 'jonas', 70, 2026)",
					"(31, 'trinity', 30, 2024)",
					"(32, 'tracy', 20, 2023)",
			};
			for (int i = 0; i < studvals.length; i++)
				planner.executeUpdate(cmd + studvals[i], tx);
			System.out.println("STUDENT records inserted.");

			cmd = "create table DEPT(DId int, DName varchar(8))";
			planner.executeUpdate(cmd, tx);
			System.out.println("DEPT records inserted.");

			cmd = "insert into DEPT(DId, DName) values ";
			String[] deptvals = { "(10, 'compsci')",
					"(20, 'math')",
					"(30, 'drama')",
					"(40, 'arts')",
					"(50, 'soci')",
					"(60, 'econs')",
					"(70, 'law')",
					"(80, 'med')",
					"(90, 'driving')",
					"(100, 'meme')",
			};
			for (int i = 0; i < deptvals.length; i++)
				planner.executeUpdate(cmd + deptvals[i], tx);
			System.out.println("DEPT records inserted.");

			cmd = "create table COURSE(CId int, Title varchar(20), DeptId int)";
			planner.executeUpdate(cmd, tx);
			System.out.println("Table COURSE created.");

			cmd = "insert into COURSE(CId, Title, DeptId) values ";
			String[] coursevals = { "(12, 'db systems', 10)",
					"(22, 'compilers', 10)",
					"(32, 'calculus', 20)",
					"(42, 'algebra', 20)",
					"(52, 'acting', 30)",
					"(62, 'elocution', 30)",
					"(24, 'datastructures', 10)",
					"(45, 'linearalgebra', 20)",
					"(34, 'calculus2', 20)",
					"(55, 'speech', 30)",
					"(62, 'humananatomy', 80)" };
			for (int i = 0; i < coursevals.length; i++)
				planner.executeUpdate(cmd + coursevals[i], tx);
			System.out.println("COURSE records inserted.");

			cmd = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
			planner.executeUpdate(cmd, tx);
			System.out.println("Table SECTION created.");

			cmd = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
			String[] sectvals = { "(13, 12, 'turing', 2018)",
					"(23, 12, 'turing', 2019)",
					"(33, 32, 'newton', 2019)",
					"(43, 32, 'einstein', 2017)",
					"(53, 62, 'brando', 2018)" };
			for (int i = 0; i < sectvals.length; i++)
				planner.executeUpdate(cmd + sectvals[i], tx);
			System.out.println("SECTION records inserted.");

			cmd = "create index courseID on Section(courseId)";
			planner.executeUpdate(cmd, tx);
			System.out.println("Indexing courseid");

			cmd = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
			planner.executeUpdate(cmd, tx);
			System.out.println("Table ENROLL created.");

			cmd = "create index studentid on Enroll(StudentId)";
			planner.executeUpdate(cmd, tx);
			System.out.println("Indexing majorid");

			cmd = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
			String[] enrollvals = { "(14, 1, 13, 'A')",
					"(24, 1, 43, 'C' )",
					"(34, 2, 43, 'B+')",
					"(44, 4, 33, 'B' )",
					"(54, 4, 53, 'A' )",
					"(64, 6, 53, 'A' )",
					"(74, 3, 13, 'A')",
					"(84, 3, 43, 'C' )",
					"(94, 5, 43, 'B+')",
					"(104, 7, 33, 'B' )",
					"(114, 7, 53, 'A' )",
					"(124, 8, 53, 'A' )" };
			for (int i = 0; i < enrollvals.length; i++)
				planner.executeUpdate(cmd + enrollvals[i], tx);
			System.out.println("ENROLL records inserted.");

			tx.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
