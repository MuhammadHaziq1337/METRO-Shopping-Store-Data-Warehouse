						METRO Shopping Store Data Warehouse - Project Setup Guide

Prerequisites:

	Windows Operating System
	Visual Studio Code (VSCode)
	MySQL Server (version 8.0 or higher)
	Java Development Kit (JDK 17 or higher)
	MySQL Connector/J (mysql-connector-j-9.1.0.jar)

Project Setup Steps
     1. Database Setup
	Open MySQL Workbench
	Copy and execute Warehouse.sql to create database schema:
        Execute the schema creation script
        source path/to/Warehouse.sql
2. Project Structure Setup
	following directory structure:
	warehouse/
	├── main/
  	  ├── src/
   	 │   ├── App.java
   	 │   ├── MeshJoin.java
   	 │   └── DateUtils.java
   	 ├── lib/
   	 │   └── mysql-connector-j-9.1.0.jar
   	 ├── customers_data.csv
   	 ├── products_data.csv
   	 └── transactions_data.csv

3. VSCode Setup

	Open VSCode
	Install Java Extension Pack if not already installed
	Open the project folder: File -> Open Folder -> Select 'warehouse' folder
	Set up Java classpath:
	Press Ctrl+Shift+P
	Type "Java: Configure Classpath"
	Add mysql-connector-j-9.1.0.jar to Referenced Libraries



4. Compile and Run Instructions

	Open terminal in VSCode (Ctrl+`)
	Navigate to project directory:
	 path/to/warehouse/main
	Compile the Java files:
	javac -cp ".;lib/mysql-connector-j-9.1.0.jar" src/*.java
Run the application:
	java -cp ".;lib/mysql-connector-j-9.1.0.jar;." src.App

5. Database Connection

	When prompted, enter:
	Database URL: jdbc:mysql://localhost:3306/metro
	Username: your_mysql_username
	Password: your_mysql_password

6. Verify Data Loading

	After running, verify data loading by executing these queries in MySQL:
	sqlCopy-- Check loaded data
	SELECT COUNT(*) FROM DIM_CUSTOMER;
	SELECT COUNT(*) FROM DIM_PRODUCT;
	SELECT COUNT(*) FROM DIM_STORE;
	SELECT COUNT(*) FROM FACT_SALES;

7. Execute OLAP Queries