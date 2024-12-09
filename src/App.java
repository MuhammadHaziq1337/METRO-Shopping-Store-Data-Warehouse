package src;
import java.util.Scanner;

public class App {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        System.out.print("Enter database URL (default: jdbc:mysql://localhost:3306/metro): ");
        String dbUrl = scanner.nextLine().trim();
        if (dbUrl.isEmpty()) {
            dbUrl = "jdbc:mysql://localhost:3306/metro";
        }
        
        System.out.print("Enter username: ");
        String username = scanner.nextLine();
        
        System.out.print("Enter password: ");
        String password = scanner.nextLine();
        
        try {
            MeshJoin meshJoin = new MeshJoin(dbUrl, username, password);
            meshJoin.execute();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }
}