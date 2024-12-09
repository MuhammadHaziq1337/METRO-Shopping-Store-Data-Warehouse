package src;
import src.DateUtils;
import java.sql.*;
import java.util.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.LogRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;

// Enhanced tuple classes
class TransactionTuple {
    int orderId;
    String orderDate;
    int productId;
    int customerId;
    int quantity;
    double unitPrice;

    public TransactionTuple(ResultSet rs) throws SQLException {
        try {
            this.orderId = rs.getInt("order_id");
            this.orderDate = DateUtils.standardizeDate(rs.getString("order_date"));
            this.productId = rs.getInt("product_id");
            this.customerId = rs.getInt("customer_id");
            this.quantity = rs.getInt("quantity");
        } catch (Exception e) {
            throw new SQLException("Error creating TransactionTuple: " + e.getMessage());
        }
    }

    public TransactionTuple(int orderId, String orderDate, int productId, int quantity, int customerId) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.quantity = quantity;
        this.customerId = customerId;
    }
}

class EnrichedTuple {
    TransactionTuple transaction;
    Map<String, Object> customerData;
    Map<String, Object> productData;
    int storeId;
    int dateId;
    double totalSale;

    public EnrichedTuple(TransactionTuple transaction) {
        this.transaction = transaction;
        this.customerData = new HashMap<>();
        this.productData = new HashMap<>();
    }
}

class MasterDataTuple {
    int id;
    Map<String, Object> attributes;

    public MasterDataTuple(int id, Map<String, Object> attributes) {
        this.id = id;
        this.attributes = attributes;
    }
}

public class MeshJoin {
    private static final Logger logger = Logger.getLogger(MeshJoin.class.getName());
    static {
        
        logger.setLevel(Level.INFO);
       
        logger.setUseParentHandlers(false);
        // Create custom console handler
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter() {
            private static final String format = "[%1$s] %2$s%n";
            @Override
            public synchronized String format(LogRecord lr) {
                return String.format(format, 
                    lr.getLevel(),
                    lr.getMessage()
                );
            }
        });
        logger.addHandler(handler);
    }
    private static final int PARTITION_SIZE = 1000;
    private static final int STREAM_BUFFER_SIZE = 100;
    private static final int BATCH_SIZE = 1000;
    
    private Queue<List<TransactionTuple>> streamQueue;
    private Map<Integer, List<TransactionTuple>> customerHashTable;
    private Map<Integer, List<TransactionTuple>> productHashTable;
    private List<Map<String, Object>> customerBuffer;
    private List<Map<String, Object>> productBuffer;
    private Connection connection;
    private Map<String, Integer> dateCache;
    private int processedCount;

    public MeshJoin(String dbUrl, String username, String password) throws SQLException {
        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            connection.setAutoCommit(false);
    
            streamQueue = new LinkedList<>();
            customerHashTable = new HashMap<>();
            productHashTable = new HashMap<>();
            customerBuffer = new ArrayList<>();
            productBuffer = new ArrayList<>();
            dateCache = new HashMap<>();
            processedCount = 0;
    
            logger.info("MeshJoin initialized successfully");
        } catch (SQLException e) {
            logger.severe("Failed to initialize MeshJoin: " + e.getMessage());
            throw e;
        }
    }
    private void loadCustomerData() throws SQLException {
        String line;
        int customerCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader("customers_data.csv"))) {
            br.readLine(); // Skip header
            String insertQuery = "INSERT IGNORE INTO DIM_CUSTOMER (customer_id, customer_name, gender) VALUES (?, ?, ?)";
            PreparedStatement stmt = connection.prepareStatement(insertQuery);
            
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                stmt.setInt(1, Integer.parseInt(values[0].trim()));
                stmt.setString(2, values[1].trim());
                stmt.setString(3, values[2].trim());
                stmt.executeUpdate();
                customerCount++;
            }
            logger.info("Loaded " + customerCount + " customer records");
        } catch (IOException e) {
            throw new SQLException("Error loading customer data: " + e.getMessage());
        }
    }
    
    private void loadProductData() throws SQLException {
        BufferedReader br = null;
        int storeCount = 0;
        int productCount = 0;
        try {
            br = new BufferedReader(new FileReader("products_data.csv"));
            br.readLine(); // Skip header
            
            String insertStoreQuery = "INSERT IGNORE INTO DIM_STORE (store_id, store_name, location) VALUES (?, ?, ?)";
            String insertProductQuery = "INSERT IGNORE INTO DIM_PRODUCT (product_id, product_name, product_price, supplier_id, supplier_name, store_id) VALUES (?, ?, ?, ?, ?, (SELECT store_id FROM DIM_STORE WHERE store_name = ?))";
            
            try (PreparedStatement storeStmt = connection.prepareStatement(insertStoreQuery);
                 PreparedStatement productStmt = connection.prepareStatement(insertProductQuery)) {
                
                Set<String> processedStores = new HashSet<>();
                List<String> lines = new ArrayList<>();
                String line;
                
                // First pass: collect stores
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                    String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    String storeName = values[6].trim().replace("\"", "");
                    
                    if (!processedStores.contains(storeName)) {
                        storeStmt.setInt(1, processedStores.size() + 1);
                        storeStmt.setString(2, storeName);
                        storeStmt.setString(3, "Location");
                        storeStmt.executeUpdate();
                        processedStores.add(storeName);
                        storeCount++;
                    }
                }
                for (String productLine : lines) {
                    String[] values = productLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    try {
                        productStmt.setInt(1, Integer.parseInt(values[0].trim()));
                        productStmt.setString(2, values[1].trim().replace("\"", ""));
                        productStmt.setDouble(3, Double.parseDouble(values[2].trim().replace("$", "")));
                        productStmt.setInt(4, Integer.parseInt(values[3].trim()));
                        productStmt.setString(5, values[4].trim().replace("\"", ""));
                        productStmt.setString(6, values[6].trim().replace("\"", ""));
                        
                        int result = productStmt.executeUpdate();
                        if (result > 0) {
                            productCount++;
                        }
                    } catch (Exception e) {
                        logger.warning("Error inserting product: Line: " + productLine);
                    }
                }
                logger.info("Loaded " + storeCount + " store records and " + productCount + " product records");
            }
        } catch (IOException e) {
            throw new SQLException("Error loading product data: " + e.getMessage());
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }
    
    private void loadTransactionData() throws SQLException {
        String line;
        int transactionCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader("transactions_data.csv"))) {
            br.readLine(); // Skip header
            String insertQuery = "INSERT IGNORE INTO TRANSACTIONS (order_id, order_date, product_id, quantity, customer_id) " +
                               "VALUES (?, ?, ?, ?, ?)";
            PreparedStatement stmt = connection.prepareStatement(insertQuery);
    
            while ((line = br.readLine()) != null) {
                try {
                    String[] parts = line.split(",", -1);
                    if (parts.length == 5) {
                        int orderId = Integer.parseInt(parts[0].trim());
                        String dateTime = DateUtils.standardizeDate(parts[1].trim());
                        int productId = Integer.parseInt(parts[2].trim());
                        int quantity = Integer.parseInt(parts[3].trim());
                        int customerId = Integer.parseInt(parts[4].trim());
    
                        stmt.setInt(1, orderId);
                        stmt.setDate(2, java.sql.Date.valueOf(dateTime));
                        stmt.setInt(3, productId);
                        stmt.setInt(4, quantity);
                        stmt.setInt(5, customerId);
    
                        if (stmt.executeUpdate() > 0) {
                            transactionCount++;
                        }
                    }
                } catch (Exception e) {
                    logger.warning("Error processing transaction: Line: " + line);
                }
            }
            logger.info("Loaded " + transactionCount + " transaction records");
        } catch (IOException e) {
            throw new SQLException("Error loading transaction data: " + e.getMessage());
        }
    }

    private void loadStreamChunk() throws SQLException {
        String query = "SELECT t.* FROM TRANSACTIONS t LEFT JOIN FACT_SALES f " +
                      "ON t.order_id = f.order_id WHERE f.order_id IS NULL " +
                      "LIMIT ? OFFSET ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, STREAM_BUFFER_SIZE);
            stmt.setInt(2, processedCount);
    
            ResultSet rs = stmt.executeQuery();
            List<TransactionTuple> chunk = new ArrayList<>();
            int chunkSize = 0;
    
            while (rs.next()) {
                try {
                    TransactionTuple tuple = new TransactionTuple(rs);
                    chunk.add(tuple);
                    chunkSize++;
    
                    
                    if (tuple.customerId > 0) {
                        customerHashTable
                            .computeIfAbsent(tuple.customerId, k -> new ArrayList<>())
                            .add(tuple);
                    }
                    
                    if (tuple.productId > 0) {
                        productHashTable
                            .computeIfAbsent(tuple.productId, k -> new ArrayList<>())
                            .add(tuple);
                    }
                } catch (SQLException e) {
                    logger.warning("Failed to process tuple: " + e.getMessage());
                }
            }
    
            if (!chunk.isEmpty()) {
                streamQueue.offer(chunk);
                processedCount += chunkSize;
                if (chunkSize == STREAM_BUFFER_SIZE) {
                    logger.fine("Loaded chunk of " + chunkSize + " transactions");
                } else {
                    logger.info("Loaded final chunk of " + chunkSize + " transactions");
                }
            }
        } catch (SQLException e) {
            logger.severe("Failed to load stream chunk: " + e.getMessage());
            throw e;
        }
    }

    private int getOrCreateDateId(String orderDate) throws SQLException {
        if (dateCache.containsKey(orderDate)) {
            return dateCache.get(orderDate);
        }
    
        try {
            String standardDate = DateUtils.standardizeDate(orderDate);
            
            
            String selectQuery = "SELECT date_id FROM DIM_DATE WHERE full_date = ?";
            try (PreparedStatement stmt = connection.prepareStatement(selectQuery)) {
                stmt.setDate(1, java.sql.Date.valueOf(standardDate));
                ResultSet rs = stmt.executeQuery();
    
                if (rs.next()) {
                    int dateId = rs.getInt("date_id");
                    dateCache.put(orderDate, dateId);
                    return dateId;
                } else {
                    // Insert a new record if the date is not found
                    return insertNewDateRecord(orderDate);
                }
            }
        } catch (Exception e) {
            logger.severe("Error processing date: " + orderDate + ", Error: " + e.getMessage());
            throw new SQLException("Invalid date format: " + orderDate, e);
        }
    }
    
   
    private void initialize() throws SQLException {
        try {
            System.out.println("\n=== Initializing Data Loading ===");
            
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM TRANSACTIONS");
            stmt.execute("DELETE FROM DIM_CUSTOMER");
            stmt.execute("DELETE FROM DIM_PRODUCT");
            stmt.execute("DELETE FROM DIM_STORE");
    
            loadCustomerData();
            loadProductData();
            loadTransactionData();
    
            // Clear data structures
            streamQueue.clear();
            customerHashTable.clear();
            productHashTable.clear();
            customerBuffer.clear();
            productBuffer.clear();
            
            loadStreamChunk();
            loadMasterDataPartitions(0);
            
            System.out.println("=== Initialization Complete ===\n");
            
        } catch (SQLException e) {
            logger.severe("Initialization failed: " + e.getMessage());
            throw new SQLException("Initialization failed", e);
        }
    }

private int insertNewDateRecord(String orderDate) throws SQLException {
    try {
        System.out.println("Inserting new date record for: " + orderDate);
        
        // First standardize the date format
        String standardDate = DateUtils.standardizeDate(orderDate);
        java.util.Date parsedDate = new SimpleDateFormat("yyyy-MM-dd").parse(standardDate);
        Calendar cal = Calendar.getInstance();
        cal.setTime(parsedDate);

        String insertQuery = "INSERT INTO DIM_DATE (full_date, year, quarter, month, " +
                           "month_name, day_of_month, day_of_week, day_name) " +
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement stmt = connection.prepareStatement(insertQuery, 
                                     Statement.RETURN_GENERATED_KEYS)) {
            stmt.setDate(1, java.sql.Date.valueOf(standardDate));
            stmt.setInt(2, cal.get(Calendar.YEAR));
            stmt.setInt(3, (cal.get(Calendar.MONTH) / 3) + 1);
            stmt.setInt(4, cal.get(Calendar.MONTH) + 1);
            stmt.setString(5, new SimpleDateFormat("MMMM").format(parsedDate));
            stmt.setInt(6, cal.get(Calendar.DAY_OF_MONTH));
            stmt.setInt(7, cal.get(Calendar.DAY_OF_WEEK));
            stmt.setString(8, new SimpleDateFormat("EEEE").format(parsedDate));
            
            stmt.executeUpdate();
            
            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                int dateId = rs.getInt(1);
                dateCache.put(orderDate, dateId);
                return dateId;
            }
            throw new SQLException("Failed to create new date record");
        }
    } catch (ParseException e) {
        logger.severe("Failed to parse date: " + orderDate + ", Error: " + e.getMessage());
        throw new SQLException("Invalid date format: " + orderDate);
    }
}
    // Enhanced master data loading with partition management
    private void loadMasterDataPartitions(int partition) throws SQLException {
        try {
            loadCustomerPartition(partition);
            loadProductPartition(partition);
            logger.info("Loaded partition " + partition + " of master data");
        } catch (SQLException e) {
            logger.severe("Failed to load master data partitions: " + e.getMessage());
            throw e;
        }
    }

    private void loadCustomerPartition(int partition) throws SQLException {
        customerBuffer.clear();
        String query = "SELECT * FROM DIM_CUSTOMER LIMIT ? OFFSET ?";
        loadPartition(query, customerBuffer, partition, "customer");
    }

    private void loadProductPartition(int partition) throws SQLException {
        productBuffer.clear();
        String query = "SELECT * FROM DIM_PRODUCT LIMIT ? OFFSET ?";
        loadPartition(query, productBuffer, partition, "product");
    }

    private void loadPartition(String query, List<Map<String, Object>> buffer, int partition, String partitionType) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, PARTITION_SIZE);
            stmt.setInt(2, partition * PARTITION_SIZE);
    
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            int recordCount = 0;
    
            // Debug info
            System.out.println("Executing query for " + partitionType + ": " + query);
            
            while (rs.next()) {
                Map<String, Object> tuple = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    if (value != null) {
                        tuple.put(columnName, value);
                    }
                }
                buffer.add(tuple);
                recordCount++;
            }
            
            logger.info("Loaded " + recordCount + " " + partitionType + " records in partition " + partition);
        } catch (SQLException e) {
            logger.severe("Failed to load " + partitionType + " partition " + partition + ": " + e.getMessage());
            throw e;
        }
    }

    // Enhanced join operation with validation and error handling
    private void performJoin() throws SQLException {
        Map<Integer, EnrichedTuple> enrichedTuples = new HashMap<>();
        int joinedCount = 0;

        try {
            // Enrich with customer data
            for (Map<String, Object> customerData : customerBuffer) {
                int customerId = ((Number) customerData.get("customer_id")).intValue();
                List<TransactionTuple> transactions = customerHashTable.get(customerId);

                if (transactions != null) {
                    for (TransactionTuple transaction : transactions) {
                        EnrichedTuple enriched = enrichedTuples.computeIfAbsent(
                            transaction.orderId,
                            k -> new EnrichedTuple(transaction)
                        );
                        enriched.customerData.putAll(customerData);
                        enriched.dateId = getOrCreateDateId(transaction.orderDate);
                        joinedCount++;
                    }
                }
            }

            // Enrich with product data
            for (Map<String, Object> productData : productBuffer) {
                int productId = ((Number) productData.get("product_id")).intValue();
                List<TransactionTuple> transactions = productHashTable.get(productId);

                if (transactions != null) {
                    for (TransactionTuple transaction : transactions) {
                        EnrichedTuple enriched = enrichedTuples.get(transaction.orderId);
                        if (enriched != null) {
                            enriched.productData.putAll(productData);
                            enriched.storeId = ((Number) productData.get("store_id")).intValue();
                            double productPrice = ((Number) productData.get("product_price")).doubleValue();
                            enriched.totalSale = productPrice * transaction.quantity;
                        }
                    }
                }
            }

            logger.info("Joined " + joinedCount + " transactions with master data");
            
            // Insert enriched data into DW
            if (!enrichedTuples.isEmpty()) {
                insertIntoDW(enrichedTuples.values());
            }

        } catch (Exception e) {
            logger.severe("Join operation failed: " + e.getMessage());
            connection.rollback();
            throw new SQLException("Join operation failed", e);
        }
    }// Enhanced DW insertion with batch processing
    private void insertIntoDW(Collection<EnrichedTuple> enrichedTuples) throws SQLException {
        String factInsert = "INSERT INTO FACT_SALES (order_id, order_date, date_id, " +
                           "customer_id, product_id, store_id, quantity, unit_price, total_sale) " +
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        int batchCount = 0;
        int totalInserted = 0;
    
        try (PreparedStatement stmt = connection.prepareStatement(factInsert)) {
            for (EnrichedTuple tuple : enrichedTuples) {
                try {
                    // Validate data before insertion
                    if (!isValidTuple(tuple)) {
                        logger.warning("Invalid tuple found, skipping: OrderID=" + tuple.transaction.orderId);
                        continue;
                    }
    
                    double unitPrice = ((Number) tuple.productData.get("product_price")).doubleValue();
                    double totalSale = unitPrice * tuple.transaction.quantity;
    
                    stmt.setString(1, String.valueOf(tuple.transaction.orderId));
                    stmt.setDate(2, java.sql.Date.valueOf(DateUtils.standardizeDate(tuple.transaction.orderDate)));
                    stmt.setInt(3, tuple.dateId);
                    stmt.setInt(4, tuple.transaction.customerId);
                    stmt.setInt(5, tuple.transaction.productId);
                    stmt.setInt(6, tuple.storeId);
                    stmt.setInt(7, tuple.transaction.quantity);
                    stmt.setDouble(8, unitPrice);
                    stmt.setDouble(9, totalSale);
                    
                    stmt.addBatch();
                    batchCount++;
                    
                    if (batchCount >= BATCH_SIZE) {
                        int[] results = stmt.executeBatch();
                        totalInserted += Arrays.stream(results).sum();
                        batchCount = 0;
                        connection.commit();
                        logger.info("Inserted batch of " + BATCH_SIZE + " records");
                    }
                } catch (Exception e) {
                    logger.warning("Failed to add tuple to batch: " + e.getMessage());
                }
            }
            
        
            if (batchCount > 0) {
                int[] results = stmt.executeBatch();
                totalInserted += Arrays.stream(results).sum();
                connection.commit();
                logger.info("Inserted final batch of " + batchCount + " records");
            }
            
            logger.info("Total records inserted: " + totalInserted);
            
        } catch (SQLException e) {
            connection.rollback();
            logger.severe("Batch insertion failed: " + e.getMessage());
            throw e;
        }
    }

   
    private boolean isValidTuple(EnrichedTuple tuple) {
        return tuple != null &&
               tuple.transaction != null &&
               tuple.dateId > 0 &&
               tuple.transaction.customerId > 0 &&
               tuple.transaction.productId > 0 &&
               tuple.storeId > 0 &&
               tuple.totalSale >= 0 &&
               !tuple.customerData.isEmpty() &&
               !tuple.productData.isEmpty();
    }

    public void execute() throws SQLException {
        try {
            System.out.println("\n=== Starting MESHJOIN ETL Process ===\n");
            initialize();
            int currentPartition = 0;
            int totalPartitions = getTotalPartitions();
            long startTime = System.currentTimeMillis();
    
            while (!streamQueue.isEmpty()) {
                loadMasterDataPartitions(currentPartition);
                performJoin();
                currentPartition = (currentPartition + 1) % totalPartitions;
    
                if (currentPartition == 0) {
                    cleanupOldestChunk();
                    loadStreamChunk();
                }
            }
    
            long endTime = System.currentTimeMillis();
            double totalTimeSeconds = (endTime - startTime) / 1000.0;
            
            System.out.println("\n=== MESHJOIN ETL Process Completed ===");
            System.out.println("Total records processed: " + processedCount);
            System.out.printf("Total execution time: %.2f seconds%n", totalTimeSeconds);
            System.out.printf("Average processing rate: %.2f records/second%n", processedCount/totalTimeSeconds);
            System.out.println("=======================================\n");
            
        } finally {
            close();
        }
    }

    private void cleanupOldestChunk() {
        List<TransactionTuple> oldestChunk = streamQueue.poll();
        if (oldestChunk != null) {
            int cleanedCount = 0;
            for (TransactionTuple tuple : oldestChunk) {
                // Clean up customer hash table
                List<TransactionTuple> customerTuples = customerHashTable.get(tuple.customerId);
                if (customerTuples != null) {
                    customerTuples.remove(tuple);
                    if (customerTuples.isEmpty()) {
                        customerHashTable.remove(tuple.customerId);
                    }
                    cleanedCount++;
                }
    
                // Clean up product hash table
                List<TransactionTuple> productTuples = productHashTable.get(tuple.productId);
                if (productTuples != null) {
                    productTuples.remove(tuple);
                    if (productTuples.isEmpty()) {
                        productHashTable.remove(tuple.productId);
                    }
                }
            }
            
            // Only log if we actually cleaned up records
            if (cleanedCount > 0) {
                logger.fine("Cleaned up chunk of " + oldestChunk.size() + " records");
            }
            
            // Memory management hint
            if (cleanedCount % 1000 == 0) {
                System.gc(); 
            }
        }
    }

    private void logProgress(long startTime) {
        long currentTime = System.currentTimeMillis();
        long elapsedSeconds = (currentTime - startTime) / 1000;
        if (elapsedSeconds > 0) {
            logger.fine(String.format("Processing rate: %.2f records/second", 
                (double) processedCount / elapsedSeconds));
        }
    }
    private int getTotalPartitions() throws SQLException {
        String query = "SELECT CEIL(COUNT(*) / CAST(? AS FLOAT)) AS partition_count FROM DIM_CUSTOMER";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, PARTITION_SIZE);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return Math.max(1, rs.getInt("partition_count"));
            }
        }
        return 1;
    }
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.commit();  
                connection.close();
                logger.info("Database connection closed successfully");
            }
        } catch (SQLException e) {
            logger.severe("Error during cleanup: " + e.getMessage());
        } finally {
            customerHashTable.clear();
            productHashTable.clear();
            customerBuffer.clear();
            productBuffer.clear();
            streamQueue.clear();
            dateCache.clear();
        }
    }
    public static void main(String[] args) {
        try {
            String dbUrl = "jdbc:mysql://localhost:3306/metro";
            String username = "your_username";
            String password = "your_password";

            MeshJoin meshJoin = new MeshJoin(dbUrl, username, password);
            meshJoin.execute();
            
        } catch (Exception e) {
            logger.severe("Application error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}