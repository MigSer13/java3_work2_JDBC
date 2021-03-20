import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Main {

    private static Connection connection;
    private static Statement statement;

    public static void main(String[] args)
    {
        connect();

        //create
        HashMap nameColumnAndType = new HashMap();
        nameColumnAndType.put("id", "INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL");
        nameColumnAndType.put("name", "TEXT NOT NULL");
        nameColumnAndType.put("quantity", "INTEGER DEFAULT 0");
        nameColumnAndType.put("corrupted", "BOOLEAN DEFAULT false");

        HashMap nameColumnAndType2 = new HashMap();
        nameColumnAndType2.put("id", "INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL");
        nameColumnAndType2.put("name", "TEXT NOT NULL");
        try {
            statement.execute(createTable("products", nameColumnAndType));
            System.out.println("Создана таблица " + "products");
            statement.execute(createTable("students", nameColumnAndType2));
            System.out.println("Создана таблица " + "students");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //insert
        try {
            insertProducts();
            System.out.println("Добавлены продуты");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //select
        try {
            ResultSet rs = selectFromTable();
            while (rs.next()) {
                System.out.println(rs.getString("name") + " - " + rs.getInt("quantity") + "шт");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //delete
        try {
            deletingTableRecord();
            System.out.println("Удалены записи, у которых количество меньше 5");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //drop
        try {
            dropTable();
            System.out.println("Удалена таблица students");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void dropTable() throws SQLException
    {
        statement.execute("DROP table students");
    }

    private static void deletingTableRecord() throws SQLException
    {
        statement.execute("DELETE FROM products WHERE quantity < 5");
    }

    private static ResultSet selectFromTable() throws SQLException
    {
        ResultSet rs = statement.executeQuery("SELECT * FROM products WHERE name = 'груша'");
        return rs;
    }

    private static void insertProducts() throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement prStat = connection.prepareStatement("INSERT INTO products (name, quantity) VALUES (?,?);");
        prStat.setString(1, "яблоко");
        prStat.setInt(2, 11);
        prStat.addBatch();
        prStat.setString(1, "груша");
        prStat.setInt(2, 3);
        prStat.addBatch();
        prStat.setString(1, "банан");
        prStat.setInt(3, 5);
        prStat.addBatch();
        prStat.setString(1, "груша");
        prStat.setInt(2, 8);
        prStat.addBatch();
        prStat.executeBatch();
        connection.setAutoCommit(true);
    }

    private static void connect()
    {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:dataDB.db");
            statement = connection.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static String createTable(String nameTable, HashMap<String, String> columnAndType)
    {
        StringBuffer tableCreationCommand = new StringBuffer();
        tableCreationCommand.append("CREATE TABLE IF NOT EXISTS " + nameTable + " (");
        for (Map.Entry entry : columnAndType.entrySet()) {
            tableCreationCommand.append(entry.getKey() + " " + entry.getValue() + ", ");
        }
        tableCreationCommand.setLength(tableCreationCommand.length() - 2);
        tableCreationCommand.append(");");
        return tableCreationCommand.toString();
    }

}
