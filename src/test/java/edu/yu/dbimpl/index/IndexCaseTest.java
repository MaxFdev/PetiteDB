package edu.yu.dbimpl.index;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.index.IndexMgrBase.IndexType;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.metadata.TableMgr;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.record.LayoutBase;
import edu.yu.dbimpl.record.RID;
import edu.yu.dbimpl.record.Schema;
import edu.yu.dbimpl.record.SchemaBase;
import edu.yu.dbimpl.record.TableScan;
import edu.yu.dbimpl.record.TableScanBase;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;

/**
 * Realistic use case tests for the Index module.
 * Tests cover scenarios from light to heavy workload, demonstrating
 * the index module's capabilities in various real-world scenarios.
 */
public class IndexCaseTest {

    private static final Logger logger = LogManager.getLogger(IndexCaseTest.class);

    // Test configuration constants
    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_SIZE = 100;
    private static final int MAX_WAIT_TIME = 500;
    private static final String TEST_BASE_DIR = "testing/IndexCaseTest";
    private static final String LOG_FILE = "index_case_logfile";

    /**
     * Helper method to create a fresh TxMgr for each test.
     */
    private TxMgrBase createTxMgr(String testName, boolean isStartup) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(isStartup));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        Path testDir = Path.of(TEST_BASE_DIR, testName);
        try {
            if (isStartup && Files.exists(testDir)) {
                deleteDirectory(testDir);
            }
        } catch (Exception e) {
            logger.warn("Could not clean up test directory: {}", testDir);
        }

        FileMgrBase fileMgr = new FileMgr(testDir.toFile(), BLOCK_SIZE);
        LogMgrBase logMgr = new LogMgr(fileMgr, LOG_FILE);
        BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, BUFFER_SIZE, MAX_WAIT_TIME);
        return new TxMgr(fileMgr, logMgr, bufferMgr, MAX_WAIT_TIME);
    }

    /**
     * Helper method to delete a directory recursively.
     */
    private void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception e) {
                            // ignore
                        }
                    });
        }
    }

    /**
     * Helper method to create a table and return its layout.
     */
    private LayoutBase createTestTable(TableMgrBase tableMgr, String tableName, SchemaBase schema, TxBase tx) {
        return tableMgr.createTable(tableName, schema, tx);
    }

    // ========================================================================
    // SECTION 1: LIGHT WORKLOAD TESTS
    // ========================================================================

    @Nested
    @DisplayName("Light Workload Tests")
    class LightWorkloadTests {

        @Test
        @DisplayName("Simple Employee Lookup")
        void simpleEmployeeLookup() {
            TxMgrBase txMgr = createTxMgr("simpleEmployeeLookup", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("empId");
            schema.addStringField("empName", 16);
            schema.addIntField("deptId");
            LayoutBase layout = createTestTable(tableMgr, "Employees", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "Employees", "empName", IndexType.STATIC_HASH);

            // Insert employees
            TableScanBase ts = new TableScan(tx, "Employees", layout);
            List<RID> rids = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                ts.insert();
                ts.setInt("empId", i);
                String name = "Employee_" + i;
                ts.setString("empName", name);
                ts.setInt("deptId", i % 5);
                rids.add(ts.getRid());
                names.add(name);
            }
            ts.close();

            // Insert into index
            IndexBase index = indexMgr.instantiate(tx, indexId);
            for (int i = 0; i < 20; i++) {
                index.insert(new Datum(names.get(i)), rids.get(i));
            }

            // Search by name
            for (int i = 0; i < 10; i++) {
                int searchIdx = i * 2; // Search for every other employee
                index.beforeFirst(new Datum(names.get(searchIdx)));
                assertTrue(index.next(), "Should find employee " + names.get(searchIdx));
                assertEquals(rids.get(searchIdx), index.getRID());
            }

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Product Catalog Search")
        void productCatalogSearch() {
            TxMgrBase txMgr = createTxMgr("productCatalogSearch", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("prodId");
            schema.addStringField("prodName", 16);
            schema.addDoubleField("price");
            schema.addBooleanField("inStock");
            LayoutBase layout = createTestTable(tableMgr, "Products", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "Products", "price", IndexType.STATIC_HASH);

            // Insert products
            TableScanBase ts = new TableScan(tx, "Products", layout);
            List<RID> rids = new ArrayList<>();
            List<Double> prices = new ArrayList<>();
            for (int i = 0; i < 15; i++) {
                ts.insert();
                ts.setInt("prodId", i);
                ts.setString("prodName", "Product_" + i);
                double price = 10.0 + i * 5.0;
                ts.setDouble("price", price);
                ts.setBoolean("inStock", i % 2 == 0);
                rids.add(ts.getRid());
                prices.add(price);
            }
            ts.close();

            // Insert into index
            IndexBase index = indexMgr.instantiate(tx, indexId);
            for (int i = 0; i < 15; i++) {
                index.insert(new Datum(prices.get(i)), rids.get(i));
            }

            // Search by specific prices
            for (int i = 0; i < 8; i++) {
                int searchIdx = i;
                index.beforeFirst(new Datum(prices.get(searchIdx)));
                assertTrue(index.next(), "Should find product with price " + prices.get(searchIdx));
                assertEquals(rids.get(searchIdx), index.getRID());
            }

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Student Grade Lookup")
        void studentGradeLookup() {
            TxMgrBase txMgr = createTxMgr("studentGradeLookup", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("studId");
            schema.addStringField("studName", 16);
            schema.addDoubleField("gpa");
            LayoutBase layout = createTestTable(tableMgr, "Students", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "Students", "gpa", IndexType.STATIC_HASH);

            // Insert students
            TableScanBase ts = new TableScan(tx, "Students", layout);
            List<RID> rids = new ArrayList<>();
            List<Double> gpas = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                ts.insert();
                ts.setInt("studId", i);
                ts.setString("studName", "Student_" + i);
                double gpa = 2.0 + (i * 0.3);
                ts.setDouble("gpa", gpa);
                rids.add(ts.getRid());
                gpas.add(gpa);
            }
            ts.close();

            // Insert into index
            IndexBase index = indexMgr.instantiate(tx, indexId);
            for (int i = 0; i < 12; i++) {
                index.insert(new Datum(gpas.get(i)), rids.get(i));
            }

            // Search by specific GPAs
            for (int i = 0; i < 5; i++) {
                int searchIdx = i * 2;
                index.beforeFirst(new Datum(gpas.get(searchIdx)));
                assertTrue(index.next(), "Should find student with GPA " + gpas.get(searchIdx));
                assertEquals(rids.get(searchIdx), index.getRID());
            }

            index.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 2: MEDIUM WORKLOAD TESTS
    // ========================================================================

    @Nested
    @DisplayName("Medium Workload Tests")
    class MediumWorkloadTests {

        @Test
        @DisplayName("Multi-Index Customer Database")
        void multiIndexCustomerDatabase() {
            TxMgrBase txMgr = createTxMgr("multiIndexCustomerDatabase", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("custId");
            schema.addStringField("custName", 16);
            schema.addStringField("email", 16);
            schema.addDoubleField("balance");
            LayoutBase layout = createTestTable(tableMgr, "Customers", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int nameIndexId = indexMgr.persistIndexDescriptor(tx, "Customers", "custName", IndexType.STATIC_HASH);
            int emailIndexId = indexMgr.persistIndexDescriptor(tx, "Customers", "email", IndexType.STATIC_HASH);
            int balanceIndexId = indexMgr.persistIndexDescriptor(tx, "Customers", "balance", IndexType.STATIC_HASH);

            // Insert customers
            TableScanBase ts = new TableScan(tx, "Customers", layout);
            List<RID> rids = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<String> emails = new ArrayList<>();
            List<Double> balances = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                ts.insert();
                ts.setInt("custId", i);
                String name = "Customer_" + i;
                String email = "cust" + i + "@email.com";
                double balance = 100.0 + i * 10.0;
                ts.setString("custName", name);
                ts.setString("email", email);
                ts.setDouble("balance", balance);
                rids.add(ts.getRid());
                names.add(name);
                emails.add(email);
                balances.add(balance);
            }
            ts.close();

            // Insert into all indexes
            IndexBase nameIndex = indexMgr.instantiate(tx, nameIndexId);
            IndexBase emailIndex = indexMgr.instantiate(tx, emailIndexId);
            IndexBase balanceIndex = indexMgr.instantiate(tx, balanceIndexId);
            for (int i = 0; i < 100; i++) {
                nameIndex.insert(new Datum(names.get(i)), rids.get(i));
                emailIndex.insert(new Datum(emails.get(i)), rids.get(i));
                balanceIndex.insert(new Datum(balances.get(i)), rids.get(i));
            }

            // Search by name
            for (int i = 0; i < 10; i++) {
                int searchIdx = i * 10;
                nameIndex.beforeFirst(new Datum(names.get(searchIdx)));
                assertTrue(nameIndex.next());
                assertEquals(rids.get(searchIdx), nameIndex.getRID());
            }

            // Search by email
            for (int i = 0; i < 10; i++) {
                int searchIdx = i * 10;
                emailIndex.beforeFirst(new Datum(emails.get(searchIdx)));
                assertTrue(emailIndex.next());
                assertEquals(rids.get(searchIdx), emailIndex.getRID());
            }

            // Search by balance
            for (int i = 0; i < 10; i++) {
                int searchIdx = i * 10;
                balanceIndex.beforeFirst(new Datum(balances.get(searchIdx)));
                assertTrue(balanceIndex.next());
                assertEquals(rids.get(searchIdx), balanceIndex.getRID());
            }

            // Update operations: change balance for some customers
            for (int i = 0; i < 10; i++) {
                int updateIdx = i * 5;
                double oldBalance = balances.get(updateIdx);
                double newBalance = oldBalance + 50.0;

                // Delete old index entry
                balanceIndex.delete(new Datum(oldBalance), rids.get(updateIdx));

                // Update table
                TableScanBase updateTs = new TableScan(tx, "Customers", layout);
                updateTs.moveToRid(rids.get(updateIdx));
                updateTs.setDouble("balance", newBalance);
                updateTs.close();

                // Insert new index entry
                balanceIndex.insert(new Datum(newBalance), rids.get(updateIdx));
                balances.set(updateIdx, newBalance);
            }

            // Verify updates
            for (int i = 0; i < 10; i++) {
                int verifyIdx = i * 5;
                balanceIndex.beforeFirst(new Datum(balances.get(verifyIdx)));
                assertTrue(balanceIndex.next());
                assertEquals(rids.get(verifyIdx), balanceIndex.getRID());
            }

            nameIndex.close();
            emailIndex.close();
            balanceIndex.close();
            tx.commit();
        }

        @Test
        @DisplayName("Order Management System")
        void orderManagementSystem() {
            TxMgrBase txMgr = createTxMgr("orderManagementSystem", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);

            // Create Orders table
            Schema ordersSchema = new Schema();
            ordersSchema.addIntField("orderId");
            ordersSchema.addIntField("custId");
            ordersSchema.addStringField("orderDate", 16);
            ordersSchema.addDoubleField("total");
            LayoutBase ordersLayout = createTestTable(tableMgr, "Orders", ordersSchema, tx);

            // Create OrderItems table
            Schema itemsSchema = new Schema();
            itemsSchema.addIntField("itemId");
            itemsSchema.addIntField("orderId");
            itemsSchema.addIntField("prodId");
            itemsSchema.addIntField("qty");
            LayoutBase itemsLayout = createTestTable(tableMgr, "OrderItems", itemsSchema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int ordersCustIdIndexId = indexMgr.persistIndexDescriptor(tx, "Orders", "custId", IndexType.STATIC_HASH);
            int ordersTotalIndexId = indexMgr.persistIndexDescriptor(tx, "Orders", "total", IndexType.STATIC_HASH);
            int itemsOrderIdIndexId = indexMgr.persistIndexDescriptor(tx, "OrderItems", "orderId",
                    IndexType.STATIC_HASH);

            // Insert orders
            TableScanBase ordersTs = new TableScan(tx, "Orders", ordersLayout);
            List<RID> orderRids = new ArrayList<>();
            List<Integer> custIds = new ArrayList<>();
            List<Double> totals = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                ordersTs.insert();
                ordersTs.setInt("orderId", i);
                int custId = i % 10; // 10 customers
                ordersTs.setInt("custId", custId);
                ordersTs.setString("orderDate", "2024-01-" + String.format("%02d", (i % 28) + 1));
                double total = 100.0 + i * 5.0;
                ordersTs.setDouble("total", total);
                orderRids.add(ordersTs.getRid());
                custIds.add(custId);
                totals.add(total);
            }
            ordersTs.close();

            // Insert order items
            TableScanBase itemsTs = new TableScan(tx, "OrderItems", itemsLayout);
            List<RID> itemRids = new ArrayList<>();
            List<Integer> itemOrderIds = new ArrayList<>();
            int itemId = 0;
            for (int i = 0; i < 50; i++) {
                int itemsPerOrder = 3;
                for (int j = 0; j < itemsPerOrder; j++) {
                    itemsTs.insert();
                    itemsTs.setInt("itemId", itemId++);
                    itemsTs.setInt("orderId", i);
                    itemsTs.setInt("prodId", i * 10 + j);
                    itemsTs.setInt("qty", j + 1);
                    itemRids.add(itemsTs.getRid());
                    itemOrderIds.add(i);
                }
            }
            itemsTs.close();

            // Insert into indexes
            IndexBase ordersCustIdIndex = indexMgr.instantiate(tx, ordersCustIdIndexId);
            IndexBase ordersTotalIndex = indexMgr.instantiate(tx, ordersTotalIndexId);
            IndexBase itemsOrderIdIndex = indexMgr.instantiate(tx, itemsOrderIdIndexId);

            for (int i = 0; i < 50; i++) {
                ordersCustIdIndex.insert(new Datum(custIds.get(i)), orderRids.get(i));
                ordersTotalIndex.insert(new Datum(totals.get(i)), orderRids.get(i));
            }

            for (int i = 0; i < 150; i++) {
                itemsOrderIdIndex.insert(new Datum(itemOrderIds.get(i)), itemRids.get(i));
            }

            // Find all orders for a customer
            for (int custId = 0; custId < 5; custId++) {
                int count = 0;
                ordersCustIdIndex.beforeFirst(new Datum(custId));
                while (ordersCustIdIndex.next()) {
                    count++;
                }
                assertTrue(count > 0, "Should find orders for customer " + custId);
            }

            // Find all items for an order
            for (int orderId = 0; orderId < 5; orderId++) {
                int count = 0;
                itemsOrderIdIndex.beforeFirst(new Datum(orderId));
                while (itemsOrderIdIndex.next()) {
                    count++;
                }
                assertEquals(3, count, "Should find 3 items for order " + orderId);
            }

            // Find orders by total
            for (int i = 0; i < 5; i++) {
                int searchIdx = i * 10;
                ordersTotalIndex.beforeFirst(new Datum(totals.get(searchIdx)));
                assertTrue(ordersTotalIndex.next());
                assertEquals(orderRids.get(searchIdx), ordersTotalIndex.getRID());
            }

            ordersCustIdIndex.close();
            ordersTotalIndex.close();
            itemsOrderIdIndex.close();
            tx.commit();
        }

        @Test
        @DisplayName("Inventory Management")
        void inventoryManagement() {
            TxMgrBase txMgr = createTxMgr("inventoryManagement", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("itemId");
            schema.addStringField("itemName", 16);
            schema.addStringField("category", 16);
            schema.addIntField("qty");
            schema.addDoubleField("price");
            LayoutBase layout = createTestTable(tableMgr, "Inventory", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int nameIndexId = indexMgr.persistIndexDescriptor(tx, "Inventory", "itemName", IndexType.STATIC_HASH);
            int categoryIndexId = indexMgr.persistIndexDescriptor(tx, "Inventory", "category", IndexType.STATIC_HASH);
            int priceIndexId = indexMgr.persistIndexDescriptor(tx, "Inventory", "price", IndexType.STATIC_HASH);

            // Insert inventory items
            TableScanBase ts = new TableScan(tx, "Inventory", layout);
            List<RID> rids = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<String> categories = new ArrayList<>();
            List<Double> prices = new ArrayList<>();
            String[] cats = { "Electronics", "Clothing", "Food", "Books" };
            for (int i = 0; i < 75; i++) {
                ts.insert();
                ts.setInt("itemId", i);
                String name = "Item_" + i;
                ts.setString("itemName", name);
                String category = cats[i % cats.length];
                ts.setString("category", category);
                ts.setInt("qty", 10 + i);
                double price = 5.0 + i * 2.0;
                ts.setDouble("price", price);
                rids.add(ts.getRid());
                names.add(name);
                categories.add(category);
                prices.add(price);
            }
            ts.close();

            // Insert into indexes
            IndexBase nameIndex = indexMgr.instantiate(tx, nameIndexId);
            IndexBase categoryIndex = indexMgr.instantiate(tx, categoryIndexId);
            IndexBase priceIndex = indexMgr.instantiate(tx, priceIndexId);
            for (int i = 0; i < 75; i++) {
                nameIndex.insert(new Datum(names.get(i)), rids.get(i));
                categoryIndex.insert(new Datum(categories.get(i)), rids.get(i));
                priceIndex.insert(new Datum(prices.get(i)), rids.get(i));
            }

            // Search by name
            for (int i = 0; i < 10; i++) {
                int searchIdx = i * 7;
                nameIndex.beforeFirst(new Datum(names.get(searchIdx)));
                assertTrue(nameIndex.next());
                assertEquals(rids.get(searchIdx), nameIndex.getRID());
            }

            // Search by category
            for (String cat : cats) {
                int count = 0;
                categoryIndex.beforeFirst(new Datum(cat));
                while (categoryIndex.next()) {
                    count++;
                }
                assertTrue(count > 0, "Should find items in category " + cat);
            }

            // Search by price
            for (int i = 0; i < 5; i++) {
                int searchIdx = i * 15;
                priceIndex.beforeFirst(new Datum(prices.get(searchIdx)));
                assertTrue(priceIndex.next());
                assertEquals(rids.get(searchIdx), priceIndex.getRID());
            }

            // Close indexes before creating update TableScans to avoid pin/unpin conflicts
            nameIndex.close();
            categoryIndex.close();
            priceIndex.close();

            // Update quantities (requires delete + insert for price index if price changes)
            // For simplicity, we'll update quantities without changing indexed fields
            for (int i = 0; i < 15; i++) {
                int updateIdx = i * 5;
                TableScanBase updateTs = new TableScan(tx, "Inventory", layout);
                updateTs.moveToRid(rids.get(updateIdx));
                int newQty = updateTs.getInt("qty") + 10;
                updateTs.setInt("qty", newQty);
                updateTs.close();
            }

            // Reopen indexes for verification
            nameIndex = indexMgr.instantiate(tx, nameIndexId);
            categoryIndex = indexMgr.instantiate(tx, categoryIndexId);
            priceIndex = indexMgr.instantiate(tx, priceIndexId);

            // Verify index consistency after updates
            for (int i = 0; i < 10; i++) {
                int verifyIdx = i * 7;
                nameIndex.beforeFirst(new Datum(names.get(verifyIdx)));
                assertTrue(nameIndex.next());
                assertEquals(rids.get(verifyIdx), nameIndex.getRID());
            }

            nameIndex.close();
            categoryIndex.close();
            priceIndex.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 3: HEAVY WORKLOAD TESTS
    // ========================================================================

    @Nested
    @DisplayName("Heavy Workload Tests")
    class HeavyWorkloadTests {

        @Test
        @DisplayName("Large Dataset Index Performance")
        void largeDatasetIndexPerformance() {
            TxMgrBase txMgr = createTxMgr("largeDatasetIndexPerformance", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("empId");
            schema.addStringField("empName", 16);
            schema.addIntField("deptId");
            schema.addDoubleField("salary");
            LayoutBase layout = createTestTable(tableMgr, "EmpLarge", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "EmpLarge", "empId", IndexType.STATIC_HASH);

            // Insert 1000 employees
            TableScanBase ts = new TableScan(tx, "EmpLarge", layout);
            List<RID> rids = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                ts.insert();
                ts.setInt("empId", i);
                ts.setString("empName", "Emp_" + i);
                ts.setInt("deptId", i % 20);
                ts.setDouble("salary", 50000.0 + i * 100.0);
                rids.add(ts.getRid());
            }
            ts.close();

            // Insert into index
            IndexBase index = indexMgr.instantiate(tx, indexId);
            for (int i = 0; i < 1000; i++) {
                index.insert(new Datum(i), rids.get(i));
            }

            // Search for specific employees
            for (int i = 0; i < 100; i++) {
                int searchId = i * 10;
                index.beforeFirst(new Datum(searchId));
                assertTrue(index.next(), "Should find employee with ID " + searchId);
                assertEquals(rids.get(searchId), index.getRID());
            }

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Complex Multi-Table Query Simulation")
        void complexMultiTableQuerySimulation() {
            TxMgrBase txMgr = createTxMgr("complexMultiTableQuerySimulation", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);

            // Create Departments table
            Schema deptSchema = new Schema();
            deptSchema.addIntField("deptId");
            deptSchema.addStringField("deptName", 16);
            deptSchema.addDoubleField("budget");
            LayoutBase deptLayout = createTestTable(tableMgr, "Departments", deptSchema, tx);

            // Create Employees table
            Schema empSchema = new Schema();
            empSchema.addIntField("empId");
            empSchema.addStringField("empName", 16);
            empSchema.addIntField("deptId");
            empSchema.addDoubleField("salary");
            LayoutBase empLayout = createTestTable(tableMgr, "Employees", empSchema, tx);

            // Create Projects table
            Schema projSchema = new Schema();
            projSchema.addIntField("projId");
            projSchema.addStringField("projName", 16);
            projSchema.addIntField("deptId");
            projSchema.addDoubleField("budget");
            LayoutBase projLayout = createTestTable(tableMgr, "Projects", projSchema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int empDeptIdIndexId = indexMgr.persistIndexDescriptor(tx, "Employees", "deptId", IndexType.STATIC_HASH);
            int empNameIndexId = indexMgr.persistIndexDescriptor(tx, "Employees", "empName", IndexType.STATIC_HASH);
            int projDeptIdIndexId = indexMgr.persistIndexDescriptor(tx, "Projects", "deptId", IndexType.STATIC_HASH);
            int deptNameIndexId = indexMgr.persistIndexDescriptor(tx, "Departments", "deptName", IndexType.STATIC_HASH);

            // Insert departments
            TableScanBase deptTs = new TableScan(tx, "Departments", deptLayout);
            List<RID> deptRids = new ArrayList<>();
            List<String> deptNames = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                deptTs.insert();
                deptTs.setInt("deptId", i);
                String name = "Dept_" + i;
                deptTs.setString("deptName", name);
                deptTs.setDouble("budget", 100000.0 + i * 10000.0);
                deptRids.add(deptTs.getRid());
                deptNames.add(name);
            }
            deptTs.close();

            // Insert employees
            TableScanBase empTs = new TableScan(tx, "Employees", empLayout);
            List<RID> empRids = new ArrayList<>();
            List<Integer> empDeptIds = new ArrayList<>();
            List<String> empNames = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                empTs.insert();
                empTs.setInt("empId", i);
                String name = "Emp_" + i;
                empTs.setString("empName", name);
                int deptId = i % 20;
                empTs.setInt("deptId", deptId);
                empTs.setDouble("salary", 50000.0 + i * 500.0);
                empRids.add(empTs.getRid());
                empDeptIds.add(deptId);
                empNames.add(name);
            }
            empTs.close();

            // Insert projects
            TableScanBase projTs = new TableScan(tx, "Projects", projLayout);
            List<RID> projRids = new ArrayList<>();
            List<Integer> projDeptIds = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                projTs.insert();
                projTs.setInt("projId", i);
                projTs.setString("projName", "Proj_" + i);
                int deptId = i % 20;
                projTs.setInt("deptId", deptId);
                projTs.setDouble("budget", 50000.0 + i * 2000.0);
                projRids.add(projTs.getRid());
                projDeptIds.add(deptId);
            }
            projTs.close();

            // Insert into indexes
            IndexBase empDeptIdIndex = indexMgr.instantiate(tx, empDeptIdIndexId);
            IndexBase empNameIndex = indexMgr.instantiate(tx, empNameIndexId);
            IndexBase projDeptIdIndex = indexMgr.instantiate(tx, projDeptIdIndexId);
            IndexBase deptNameIndex = indexMgr.instantiate(tx, deptNameIndexId);

            for (int i = 0; i < 200; i++) {
                empDeptIdIndex.insert(new Datum(empDeptIds.get(i)), empRids.get(i));
                empNameIndex.insert(new Datum(empNames.get(i)), empRids.get(i));
            }

            for (int i = 0; i < 50; i++) {
                projDeptIdIndex.insert(new Datum(projDeptIds.get(i)), projRids.get(i));
            }

            for (int i = 0; i < 20; i++) {
                deptNameIndex.insert(new Datum(deptNames.get(i)), deptRids.get(i));
            }

            // Find all employees in a department
            for (int deptId = 0; deptId < 5; deptId++) {
                int count = 0;
                empDeptIdIndex.beforeFirst(new Datum(deptId));
                while (empDeptIdIndex.next()) {
                    count++;
                }
                assertTrue(count > 0, "Should find employees in department " + deptId);
            }

            // Find all projects for a department
            for (int deptId = 0; deptId < 5; deptId++) {
                int count = 0;
                projDeptIdIndex.beforeFirst(new Datum(deptId));
                while (projDeptIdIndex.next()) {
                    count++;
                }
                assertTrue(count > 0, "Should find projects for department " + deptId);
            }

            // Find employee by name
            for (int i = 0; i < 10; i++) {
                int searchIdx = i * 20;
                empNameIndex.beforeFirst(new Datum(empNames.get(searchIdx)));
                assertTrue(empNameIndex.next());
                assertEquals(empRids.get(searchIdx), empNameIndex.getRID());
            }

            // Find department by name
            for (int i = 0; i < 5; i++) {
                deptNameIndex.beforeFirst(new Datum(deptNames.get(i)));
                assertTrue(deptNameIndex.next());
                assertEquals(deptRids.get(i), deptNameIndex.getRID());
            }

            empDeptIdIndex.close();
            empNameIndex.close();
            projDeptIdIndex.close();
            deptNameIndex.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index Maintenance Under Heavy Updates")
        void indexMaintenanceUnderHeavyUpdates() {
            TxMgrBase txMgr = createTxMgr("indexMaintenanceUnderHeavyUpdates", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("acctId");
            schema.addStringField("acctName", 16);
            schema.addDoubleField("balance");
            schema.addBooleanField("active");
            LayoutBase layout = createTestTable(tableMgr, "Accounts", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int nameIndexId = indexMgr.persistIndexDescriptor(tx, "Accounts", "acctName", IndexType.STATIC_HASH);
            int balanceIndexId = indexMgr.persistIndexDescriptor(tx, "Accounts", "balance", IndexType.STATIC_HASH);

            // Insert 200 accounts
            TableScanBase ts = new TableScan(tx, "Accounts", layout);
            List<RID> rids = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<Double> balances = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                ts.insert();
                ts.setInt("acctId", i);
                String name = "Account_" + i;
                ts.setString("acctName", name);
                double balance = 1000.0 + i * 50.0;
                ts.setDouble("balance", balance);
                ts.setBoolean("active", true);
                rids.add(ts.getRid());
                names.add(name);
                balances.add(balance);
            }
            ts.close();

            // Insert into indexes
            IndexBase nameIndex = indexMgr.instantiate(tx, nameIndexId);
            IndexBase balanceIndex = indexMgr.instantiate(tx, balanceIndexId);
            for (int i = 0; i < 200; i++) {
                nameIndex.insert(new Datum(names.get(i)), rids.get(i));
                balanceIndex.insert(new Datum(balances.get(i)), rids.get(i));
            }

            // Perform 50 balance updates
            // Use unique balance values to avoid collisions with existing accounts
            for (int i = 0; i < 50; i++) {
                int updateIdx = i * 4;
                double oldBalance = balances.get(updateIdx);
                // Set new balance to a value that won't conflict with any existing balance
                // Original balances range from 1000 to 10950 (1000 + 199*50)
                // Use values starting from 20000 to ensure uniqueness
                double newBalance = 20000.0 + i;

                // Delete old index entry
                balanceIndex.delete(new Datum(oldBalance), rids.get(updateIdx));

                // Update table
                TableScanBase updateTs = new TableScan(tx, "Accounts", layout);
                updateTs.moveToRid(rids.get(updateIdx));
                updateTs.setDouble("balance", newBalance);
                updateTs.close();

                // Insert new index entry
                balanceIndex.insert(new Datum(newBalance), rids.get(updateIdx));
                balances.set(updateIdx, newBalance);
            }

            // Perform 30 account name changes
            for (int i = 0; i < 30; i++) {
                int updateIdx = i * 6 + 1;
                String oldName = names.get(updateIdx);
                String newName = "NewAcct_" + updateIdx;

                // Delete old index entry
                nameIndex.delete(new Datum(oldName), rids.get(updateIdx));

                // Update table
                TableScanBase updateTs = new TableScan(tx, "Accounts", layout);
                updateTs.moveToRid(rids.get(updateIdx));
                updateTs.setString("acctName", newName);
                updateTs.close();

                // Insert new index entry
                nameIndex.insert(new Datum(newName), rids.get(updateIdx));
                names.set(updateIdx, newName);
            }

            // Close indexes before verification to ensure clean state
            nameIndex.close();
            balanceIndex.close();

            // Reopen indexes for verification
            nameIndex = indexMgr.instantiate(tx, nameIndexId);
            balanceIndex = indexMgr.instantiate(tx, balanceIndexId);

            // Verify all searches still work correctly
            for (int i = 0; i < 20; i++) {
                int searchIdx = i * 10;
                nameIndex.beforeFirst(new Datum(names.get(searchIdx)));
                assertTrue(nameIndex.next());
                assertEquals(rids.get(searchIdx), nameIndex.getRID());
            }

            for (int i = 0; i < 20; i++) {
                int searchIdx = i * 10;
                balanceIndex.beforeFirst(new Datum(balances.get(searchIdx)));
                assertTrue(balanceIndex.next());
                assertEquals(rids.get(searchIdx), balanceIndex.getRID());
            }

            nameIndex.close();
            balanceIndex.close();
            tx.commit();
        }

        @Test
        @DisplayName("Hash Collision Stress Test")
        void hashCollisionStressTest() {
            TxMgrBase txMgr = createTxMgr("hashCollisionStressTest", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("testId");
            schema.addStringField("testVal", 16);
            LayoutBase layout = createTestTable(tableMgr, "HashTest", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "HashTest", "testVal", IndexType.STATIC_HASH);

            // Insert 200+ records with carefully chosen string values
            // Using patterns that may hash to same bucket
            TableScanBase ts = new TableScan(tx, "HashTest", layout);
            List<RID> rids = new ArrayList<>();
            List<String> values = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                ts.insert();
                ts.setInt("testId", i);
                // Create strings with similar patterns to increase collision probability
                String value = "TestVal_" + (i % 50) + "_" + i;
                ts.setString("testVal", value);
                rids.add(ts.getRid());
                values.add(value);
            }
            ts.close();

            // Insert into index
            IndexBase index = indexMgr.instantiate(tx, indexId);
            for (int i = 0; i < 200; i++) {
                index.insert(new Datum(values.get(i)), rids.get(i));
            }

            // Search for each inserted value
            for (int i = 0; i < 200; i++) {
                index.beforeFirst(new Datum(values.get(i)));
                assertTrue(index.next(), "Should find value " + values.get(i));
                assertEquals(rids.get(i), index.getRID());
            }

            // Test delete operations with collisions
            for (int i = 0; i < 50; i++) {
                int deleteIdx = i * 4;
                index.delete(new Datum(values.get(deleteIdx)), rids.get(deleteIdx));
            }

            // Close index after delete operations to ensure clean state
            index.close();

            // Reopen index for verification
            index = indexMgr.instantiate(tx, indexId);

            // Verify deleted values are gone
            for (int i = 0; i < 50; i++) {
                int verifyIdx = i * 4;
                index.beforeFirst(new Datum(values.get(verifyIdx)));
                assertFalse(index.next(), "Deleted value should not be found: " + values.get(verifyIdx));
            }

            // Verify non-deleted values still exist
            for (int i = 0; i < 50; i++) {
                int verifyIdx = i * 4 + 1;
                index.beforeFirst(new Datum(values.get(verifyIdx)));
                assertTrue(index.next(), "Non-deleted value should be found: " + values.get(verifyIdx));
            }

            index.close();
            tx.commit();
        }
    }
}
