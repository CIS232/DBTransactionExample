import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * The CoffeeDBManager class performs operations
 * the CoffeeDB database.
 */

public class CoffeeDBManager {
	// Constant for database URL.
	public final static String DB_URL = "jdbc:mysql://localhost:33306/coffee";

	public static Connection getDBConnection() throws SQLException{
		return DriverManager.getConnection(DB_URL, "root", "password");
	}


	/**
	 * The getCustomerNames method returns an ArrayList
	 * of Strings containing all the customer names.
	 */
	public static ArrayList<String> getCustomerNames() throws SQLException {
		// Create a connection to the database.
		Connection conn = getDBConnection();

		// Create a Statement object for the query.
		Statement stmt =
				conn.createStatement(
						ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_READ_ONLY);

		// Execute the query.
		ResultSet resultSet = stmt.executeQuery(
				"SELECT Name FROM Customer");

		// Get the number of rows
		resultSet.last();                 // Move last row
		int numRows = resultSet.getRow(); // Get row number
		resultSet.first();                // Move to first row

		// Create an ArrayList for the customer names.
		ArrayList<String> listData = new ArrayList<>();

		// Populate the ArrayList with customer names.
		for (int index = 0; index < numRows; index++) {
			// Store the customer name in the array.
			listData.add(resultSet.getString(1));

			// Go to the next row in the result set.
			resultSet.next();
		}

		// Close the connection and statement objects.
		conn.close();
		stmt.close();

		// Return the listData array.
		return listData;
	}

	/**
	 * The getCoffeeNames method returns an array
	 * of Strings containing all the coffee names.
	 */
	public static ArrayList<String> getCoffeeNames() throws SQLException {
		// Create a connection to the database.
		Connection conn = getDBConnection();

		// Create a Statement object for the query.
		Statement stmt =
				conn.createStatement(
						ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_READ_ONLY);

		// Execute the query.
		ResultSet resultSet = stmt.executeQuery(
				"SELECT Description FROM Coffee");

		// Get the number of rows
		resultSet.last();                 // Move to last row
		int numRows = resultSet.getRow(); // Get row number
		resultSet.first();                // Move to first row

		// Create an array for the coffee names.
		ArrayList<String> listData = new ArrayList<>();

		// Populate the array with coffee names.
		for (int index = 0; index < numRows; index++) {
			// Store the coffee name in the array.
			listData.add(resultSet.getString(1));

			// Go to the next row in the result set.
			resultSet.next();
		}

		// Close the connection and statement objects.
		conn.close();
		stmt.close();

		// Return the listData array.
		return listData;
	}

	/**
	 * The getProdNum method returns a specific
	 * coffee's product number.
	 */
	public static String getProdNum(String coffeeName) throws SQLException {
		String prodNum = ""; // Product number

		// Create a connection to the database.
		Connection conn = getDBConnection();

		// Create a Statement object for the query.
		Statement stmt = conn.createStatement();

		// Execute the query.
		ResultSet resultSet = stmt.executeQuery(
				"SELECT ProdNum " +
						"FROM Coffee " +
						"WHERE Description = '" +
						coffeeName + "'");

		// If the result set has a row, go to it
		// and retrieve the product number.
		if (resultSet.next())
			prodNum = resultSet.getString(1);

		// Close the Connection and Statement objects.
		conn.close();
		stmt.close();

		// Return the product number.
		return prodNum;
	}

	/**
	 * The getCoffeePrice method returns the price
	 * of a coffee.
	 */
	public static double getCoffeePrice(String prodNum) throws SQLException {
		double price = 0.0;  // Coffee price

		// Create a connection to the database.
		Connection conn = getDBConnection();

		// Create a Statement object for the query.
		Statement stmt = conn.createStatement();

		// Execute the query.
		ResultSet resultSet = stmt.executeQuery(
				"SELECT Price " +
						"FROM Coffee " +
						"WHERE ProdNum = '" +
						prodNum + "'");

		// If the result set has a row, go to it
		// and retrieve the price.
		if (resultSet.next())
			price = resultSet.getDouble(1);

		// Close the connection and statement objects.
		conn.close();
		stmt.close();

		// Return the price.
		return price;
	}

	/**
	 * The getCustomerNum method returns a specific
	 * customer's number.
	 */
	public static String getCustomerNum(String name) throws SQLException {
		String customerNumber = "";

		// Create a connection to the database.
		Connection conn = getDBConnection();

		// Create a Statement object for the query.
		Statement stmt = conn.createStatement();

		// Execute the query.
		ResultSet resultSet = stmt.executeQuery(
				"SELECT CustomerNumber " +
						"FROM Customer " +
						"WHERE Name = '" + name + "'");

		if (resultSet.next())
			customerNumber = resultSet.getString(1);

		// Close the connection and statement objects.
		conn.close();
		stmt.close();

		// Return the customer number.
		return customerNumber;
	}

	/**
	 * The submitOrder method submits an order to
	 * the UnpaidOrder table in the CoffeeDB database.
	 */
	public static void submitOrder(String custNum, String prodNum,
	                               double orderQuantity, double price,
	                               String orderDate) throws SQLException {
		// Calculate the cost of the order.
		double cost = orderQuantity * price;

		// Create a connection to the database.
		Connection conn = getDBConnection();
		conn.setAutoCommit(false);

		try {
			double inventoryQuantity = getInventoryQuantity(prodNum, conn);

			if (inventoryQuantity >= orderQuantity) {
				subtractCoffeeQuantity(prodNum, orderQuantity, conn);
				addNewUnpaidOrder(custNum, prodNum, orderQuantity, orderDate, cost, conn);
				conn.commit();
			} else {
				throw new RuntimeException("Not enough coffee for desired quantity.");
			}
		} catch (SQLException ex) {
			conn.rollback();
			throw ex;
		}

		// Close the connection and statement objects.
		conn.close();
	}

	private static void subtractCoffeeQuantity(String prodNum, double orderQuantity, Connection conn) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("UPDATE Coffee SET Quantity = Quantity - ? WHERE ProdNum = ?");
		stmt.setDouble(1, orderQuantity);
		stmt.setString(2, prodNum);
		stmt.executeUpdate();
	}

	private static double getInventoryQuantity(String prodNum, Connection conn) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("SELECT Quantity from Coffee WHERE ProdNum = ?");
		stmt.setString(1, prodNum);
		ResultSet result = stmt.executeQuery();
		double inventoryQuantity = 0;
		if (result.next()) {
			inventoryQuantity = result.getDouble(1);
		}
		return inventoryQuantity;
	}

	private static void addNewUnpaidOrder(String custNum, String prodNum, double quantity, String orderDate, double cost, Connection conn) throws SQLException {
		String sql = "INSERT INTO UnpaidOrder(CustomerNumber, ProdNum, OrderDate, Quantity, Cost) VALUES (?, ?, ?, ?, ?)";
		// Create a Statement object for the query.
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.setString(1, custNum);
		stmt.setString(2, prodNum);
		stmt.setString(3, orderDate);
		stmt.setDouble(4, quantity);
		stmt.setDouble(5, cost);

		// Execute the query.
		stmt.executeUpdate();
		stmt.close();
	}
}
