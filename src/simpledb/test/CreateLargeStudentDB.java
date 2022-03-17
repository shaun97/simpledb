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
			System.out.println("Indexing sid");

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
					"(19, 'jason', 20, 2021)",
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
					"(30, 'publichealth')",
					"(40, 'arts')",
					"(50, 'design')",
					"(60, 'music')",
					"(70, 'law')",
					"(80, 'med')",
					"(90, 'engineering')",
					"(100, 'science')"
			};
			for (int i = 0; i < deptvals.length; i++)
				planner.executeUpdate(cmd + deptvals[i], tx);
			System.out.println("DEPT records inserted.");

			cmd = "create table COURSE(CId int, Title varchar(20), DeptId int)";
			planner.executeUpdate(cmd, tx);
			System.out.println("Table COURSE created.");

			cmd = "insert into COURSE(CId, Title, DeptId) values ";
			String[] coursevals = { "(100, 'IntroductiontoBusinessAnalytics', 10)",
					"(101, 'DataManagementandVisualisation', 10)",
					"(102, 'OptimizationMethodsinBusinessAnalytics', 10)",
					"(103, 'BusinessConceptsandMetricsforAnalytics', 10)",
					"(104, 'FeatureEngineeringforMachineLearning', 10)", "(105, 'Data-DrivenMarketing', 10)", };

			// String[] coursevals0 = { "(102, 'Probability', 100)", "(429,
			// 'GeographiesofDevelopment', 40)",
			// "(430, 'TransportandCommunications', 40)", "(431, 'CitiesinTransition', 40)",
			// "(432, 'SoutheastAsia', 40)", "(433, 'EnergyEnvironmentandSustainability',
			// 40)",
			// "(434, 'AquaticRiparianandCoastalSystems', 40)",
			// "(435, 'ChineseHistoryandLiterature', 40)",

			// };
			// String[] coursevals1 = { "(906, 'BiochemistryandBiomaterialsforBioengineers',
			// 90)",
			// "(907, 'BiophotonicsAndBioimaging', 90)",
			// "(908, 'PrinciplesofStructuralMechanicsandMaterials', 90)", "(909,
			// 'GeotechnicalEngineering', 90)",
			// "(910, 'HydrologyandFreeSurfaceFlows', 90)", "(911,
			// 'StructuralSteelDesignandSystem', 90)",
			// "(912, 'DesignofLandTransportInfrastructures', 90)", "(913,
			// 'GroundImprovement', 90)",
			// "(914, 'PileFoundations', 90)", "(915,
			// 'StructuralSupportSystemsforExcavation', 90)",
			// "(916, 'TransportationPlanning', 90)", "(1017, 'RemoteSensing', 100)", };
			// String[] coursevals2 = { "(1018, 'MathematicalMethodsinPhysicsIII', 100)",
			// "(1019, 'ADVANCEDSTATISTICALMECHANICS', 100)",
			// "(1020, 'SelectedTopicsinQuantumFieldTheory', 100)", "(1021,
			// 'AdvancedDynamics', 100)",
			// "(1022, 'PhysicsofNanostructures', 100)", "(1023, 'AdvancedBiophysics',
			// 100)",
			// "(1024, 'PharmaceuticalMarketing', 100)", "(1025,
			// 'IntroductiontoQuantitativeFinance', 100)",
			// "(1026, 'InvestmentInstrumentandRiskManagement', 100)", "(1027,
			// 'Probability', 100)", };
			// String[] coursevals3 = { "(436, 'FramingBollywood:UnpackingTheMagic', 40)",
			// "(437, 'CulturalBorrowing:JapanandChina', 40)",
			// "(438, 'UnderstandingConsumption', 40)", "(439, 'FilmArtandHumanConcerns',
			// 40)",
			// "(440, 'NamesasMarkersofSocio-culturalIdentity', 40)",
			// "(441, 'GhostsandSpiritsinSocietyandCulture', 40)", "(442, 'ArtinSociety',
			// 40)",
			// "(443, 'ArtinAsia:ThroughMediaStyleSpaceandTime', 40)",
			// "(444, 'TraditionalChineseKnowledgeofHealthandWell-being', 40)", "(445,
			// 'Luck', 40)",
			// "(446, 'WorldsofFootball', 40)", "(247, 'MathematicsI', 20)", "(248,
			// 'CalculusforComputing', 20)",
			// "(249, 'LinearAlgebraI', 20)", "(250, 'LinearAlgebraI', 20)", "(251,
			// 'Calculus', 20)",
			// "(252, 'LinearAlgebraII', 20)", "(253, 'MultivariableCalculus', 20)", };
			// String[] coursevals4 = { "(763,
			// 'StrategiesforAsianDisputes-AComparativeAnalysis', 70)",
			// "(764, 'Mediation/ConciliationofInter-&Investor-StateDisputes', 70)",
			// "(765, 'RestitutionofUnjustEnrichment', 70)", "(766,
			// 'PublicHealthLawandRegulation', 70)",
			// "(767, 'MergersandAcquisitions:APractitioner’sPerspective', 70)",
			// "(768, 'AdvancedPracticuminInternationalArbitration', 70)",
			// "(769, 'ComparativeEvidenceinInternationalArbitration', 70)", };
			// String[] coursevals5 = { "(770, 'InternationalRefugeeLaw', 70)",
			// "(771, 'InternationalRegulationoftheGlobalCommons', 70)",
			// "(772, 'MonetaryLawinComparativePerspective', 70)", "(873,
			// 'UnderstandingYourBrain', 80)",
			// "(574, 'RealEstateInvestmentAnalysis', 50)", "(575,
			// 'PropertyTaxandStatutoryValuation', 50)",
			// "(576, 'StrategicAssetManagement', 50)", "(577,
			// 'CorporateFinanceforRealEstate', 50)",
			// "(578, 'AdvancedUrbanPlanning', 50)", "(579, 'RealEstateDevelopment', 50)",
			// "(580, 'ProfessionalPracticeandEthics', 50)", "(581,
			// 'REITandBusinessTrustManagement', 50)",
			// "(582, 'RealEstateSecuritisation', 50)", "(383, 'PublicinAction', 30)",
			// "(384, 'PublicinAction', 30)", "(385, 'PublicinAction', 30)", };

			// String[] coursevals6 = { "(388, 'SocietyandtheSocialDeterminants', 30)",
			// "(389, 'IntroductiontoGlobalHealth', 30)",
			// "(690, 'SocialHistoryofthePiano', 60)", "(691,
			// 'StateoftheArt:ACurrentViewofMusicinSingapore', 60)",
			// "(692, 'MusicandMachines', 60)", "(693, 'MusicandMachines', 60)", "(694,
			// 'MusicandMachines', 60)",
			// "(695, 'DesktopMixingProduction', 60)", "(696,
			// 'InterdisciplinaryElectronicArtsSurvey', 60)",
			// "(697, 'LiveInteractivity', 60)", "(698, 'VirtualInstrumentSoundDesign',
			// 60)",
			// "(699, 'WorldMusicEnsemble', 60)", "(700,
			// 'Re-imaginingPianismthroughAnalysis', 60)" };

			// String[] coursevals7 = { "(254, 'MathematicalAnalysisI', 20)", "(255,
			// 'Probability', 20)",
			// "(256, 'Probability', 20)",
			// "(257, 'IntroductiontoGeometry', 20)", "(258, 'AlgebraII', 20)", "(259,
			// 'ComplexAnalysisI', 20)",
			// "(760, 'ConflictofLawsinInt’lCommercialArbitration', 70)",
			// "(761, 'AdvancedIssuesintheLaw&PracticeofInt’lArbitration', 70)",
			// "(762, 'BehaviouralEconomicsLaw&Regulation', 70)", };

			for (int i = 0; i < coursevals.length; i++)
				planner.executeUpdate(cmd + coursevals[i], tx);
			System.out.println("COURSE records1 inserted.");

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
