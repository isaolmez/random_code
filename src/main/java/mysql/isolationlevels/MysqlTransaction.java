package mysql.isolationlevels;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public class MysqlTransaction {
	private static final String schema = "test";
	private static final String table = "mytable";
	private static final String tableWithoutIndex = "mytable_without_index";
	private static String tableName = tableWithoutIndex;
	private static final int transactionIsolationLevel = Connection.TRANSACTION_REPEATABLE_READ;

	public static void main(String[] args) throws Exception {
		createInitialTables();
		// caseUpdateInLongUpdate();
		caseUpdateInLongUpdate();
	}
	
	private static void caseUpdateInLongInsert() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(2);
		executor.submit(longInsertTransaction);
		executor.submit(updateTransaction);
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
	}

	private static void caseInsertInLongInsert() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(2);
		executor.submit(longInsertTransaction);
		executor.submit(insertTransaction);
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
	}
	
	private static void caseInsertInLongUpdate() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(2);
		executor.submit(longUpdateTransaction);
		executor.submit(insertTransaction);
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
	}

	private static void caseUpdateInLongUpdate() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(2);
		executor.submit(longUpdateTransaction);
		executor.submit(updateTransaction);
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
	}

	private static Connection getConnection() throws SQLException {
		Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "sar45tre");
		connection.setTransactionIsolation(transactionIsolationLevel);
		connection.setAutoCommit(false);
		return connection;
	}
	
	private static Connection getConnection(String schema) throws SQLException {
		Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + schema , "root", "sar45tre");
		connection.setTransactionIsolation(transactionIsolationLevel);
		connection.setAutoCommit(false);
		return connection;
	}

	private static void createInitialTables() throws SQLException{
		Connection connection = getConnection();
		Statement statement = connection.createStatement();
		statement.execute("CREATE SCHEMA IF NOT EXISTS " + schema + " DEFAULT CHARACTER SET utf8");
		connection.setCatalog(schema);
		Statement statementTable = connection.createStatement();
		statementTable.execute("CREATE TABLE IF NOT EXISTS " + table + " ("
				  + " `id` int(11) NOT NULL AUTO_INCREMENT,"  
				  + " `name` varchar(254) DEFAULT NULL,"
				  + " `value` int(11) DEFAULT NULL,"
				  + " PRIMARY KEY (`id`),"
				  + " KEY `secondary` (`name`)"
				  + " ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8");
		statementTable.execute("CREATE TABLE IF NOT EXISTS " + tableWithoutIndex + " ("
				  + " `id` int(11) NOT NULL AUTO_INCREMENT,"
				  + " `name` varchar(254) DEFAULT NULL,"
				  + " `value` int(11) DEFAULT NULL,"
				  + " PRIMARY KEY (`id`)"
				  + " ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8");
		PreparedStatement stmt = connection.prepareStatement("REPLACE INTO " + table + "(id, name, value) VALUES(?, ?, ?)");
		stmt.setInt(1, 1);
		stmt.setString(2, "a");
		stmt.setInt(3, 1);
		stmt.execute();
		stmt.setInt(1, 2);
		stmt.setString(2, "b");
		stmt.setInt(3, 1);
		stmt.execute();
		PreparedStatement stmt2 = connection.prepareStatement("REPLACE INTO " + tableWithoutIndex + "(id, name, value) VALUES(?, ?, ?)");
		stmt2.setInt(1, 1);
		stmt2.setString(2, "a");
		stmt2.setInt(3, 1);
		stmt2.execute();
		stmt2.setInt(1, 2);
		stmt2.setString(2, "b");
		stmt2.setInt(3, 1);
		stmt2.execute();
		connection.commit();
		connection.close();
	}

	private static Thread insertTransaction = new Thread() {
		Random r = new Random();

		final @Override public void run() {
			try {
				final Connection connection = getConnection(schema);
				final Timer timer = new Timer();
				TimerTask task = new TimerTask() {
					int count = 0;

					@Override
					public void run() {
						count++;
						System.out.println("insertThread\t\t " + count + " seconds have passed.");
						if (count == 10) {
							try {
								System.out.println("insertThread\t\t commit start.");
								connection.commit();
								System.out.println("insertThread\t\t committed.");
								timer.cancel();
							} catch (SQLException e) {
								System.out.println(e.getMessage());
							}
						}
					}
				};
				timer.schedule(task, 1000, 1000);

				PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + tableName + "(name, value) VALUES(?, ?)");
				stmt.setString(1, "a" + r.nextInt(100));
				stmt.setInt(2, 1);
				Thread.sleep(1000);
				System.out.println("insertThread\t\t statement execute start");
				stmt.execute();
				System.out.println("insertThread\t\t statement execute end");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	};

	private static Thread longInsertTransaction = new Thread() {
		Random r = new Random();

		final @Override public void run() {
			try {
				final Connection connection = getConnection(schema);
				final Timer timer = new Timer();
				TimerTask task = new TimerTask() {
					int count = 0;

					@Override
					public void run() {
						count++;
						System.out.println("longInsertTx\t\t " + count + " seconds have passed.");
						if (count == 20) {
							try {
								System.out.println("longInsertTx\t\t commit start.");
								connection.commit();
								System.out.println("longInsertTx\t\t committed.");
								timer.cancel();
							} catch (SQLException e) {
								System.out.println(e.getMessage());
							}
						}
					}
				};
				timer.schedule(task, 1000, 1000);

				PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + tableName + "(name, value) VALUES(?, ?)");
				stmt.setString(1, "a" + r.nextInt(100));
				stmt.setInt(2, 1);
				System.out.println("longInsertTx\t\t statement execute start");
				stmt.execute();
				System.out.println("longInsertTx\t\t statement execute end");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	};
	
	private static Thread updateTransaction = new Thread() {
		@Override
		public void run() {
			try {
				final Connection connection = getConnection(schema);
				final Timer timer = new Timer();
				TimerTask task = new TimerTask() {
					int count = 0;

					@Override
					public void run() {
						count++;
						System.out.println("updateThread\t\t " + count + " seconds have passed.");
						if (count == 10) {
							try {
								System.out.println("updateThread\t\t commit start.");
								connection.commit();
								System.out.println("updateThread\t\t committed.");
								timer.cancel();
							} catch (SQLException e) {
								System.out.println(e.getMessage());
							}
						}
					}
				};
				timer.schedule(task, 1000, 1000);
				Statement stmt2 = connection.createStatement();
				Thread.sleep(1000);
				System.out.println("updateThread\t\t statement execute start");
				stmt2.execute("UPDATE " + tableName + " SET value=22 WHERE name='b'");
				System.out.println("updateThread\t\t statement execute end");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	};

	private static Thread longUpdateTransaction = new Thread() {
		@Override
		public void run() {
			try {
				final Connection connection = getConnection(schema);
				final Timer timer = new Timer();
				TimerTask task = new TimerTask() {
					int count = 0;

					@Override
					public void run() {
						count++;
						System.out.println("longUpdateTx\t\t " + count + " seconds have passed.");
						if (count == 20) {
							try {
								System.out.println("longUpdateTx\t\t commit start.");
								connection.commit();
								System.out.println("longUpdateTx\t\t committed.");
								timer.cancel();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}
					}
				};

				timer.schedule(task, 1000, 1000);
				Statement stmt = connection.createStatement();
				System.out.println("longUpdateTx\t\t statement execute start");
				stmt.execute("UPDATE " + tableName + " SET value=22 WHERE name='a'");
				System.out.println("longUpdateTx\t\t statement execute end");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	};
}
