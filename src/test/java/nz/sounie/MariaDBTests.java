package nz.sounie;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

// NB: Docker needs to be running in order for this test to be runnable.
@Testcontainers
class MariaDBTests {
    private static final String DB_USER = "db-user";
    private static final String PASSWORD = "aBcDeFg54321";

    // Annotation applied to hook into TestContainers lifecycle management.
    @Container
    private static final MariaDBContainer<?> mariadb = new MariaDBContainer<>("mariadb")
            .withDatabaseName("sampleDB")
            .withUsername(DB_USER)
            .withPassword(PASSWORD);

    @BeforeAll
    static void setUp() throws SQLException
    {
        String jdbcUrl = mariadb.getJdbcUrl();
        System.out.println("JDBC URL: " + jdbcUrl);

        // Set up table
        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD)) {
            createTable(connection);
        }
    }

    @Test
    void testTransactionIsolationLevel() throws Exception {
        String jdbcUrl = mariadb.getJdbcUrl();

        int transactionReadUncommited = Connection.TRANSACTION_READ_UNCOMMITTED;
        int transactionReadCommited = Connection.TRANSACTION_READ_COMMITTED;

        assertThat(mariadb.isRunning()).isTrue();
        assertThat(mariadb.getHost()).isNotEmpty();
        assertThat(mariadb.getFirstMappedPort()).isGreaterThan(0);

        UUID id = UUID.randomUUID();

        final int NUMBER_OF_UPSERTERS = 5;
        List<Upserter> upserters = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_UPSERTERS; i++) {
            Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD);
            connection.setAutoCommit(false);

            connection.setTransactionIsolation(transactionReadCommited);
            Upserter upserter = new Upserter(connection, id, "First event", i + 1);
            upserters.add(upserter);
        }

        // Shuffle the ordering of elements in the upserters list
        Collections.shuffle(upserters);

        // Set up a concurrent executor to perform the upsert calls concurrently
        ExecutorService executorService =  Executors.newFixedThreadPool(NUMBER_OF_UPSERTERS);

        for (Upserter upserter : upserters) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    // Sleeping to allow ExecutorService to accumulate upserters before they run
                    try {
                        Thread.sleep(200L);
                    } catch (InterruptedException e) {
                        System.out.println("Sleep interrupted");
                    }
                    upserter.performUpsert();
                    if (!upserter.isSuccess()) {
                        System.err.println("Upsert failed");
                    }

                    upserter.closeConnection();
                }
            });
        }

        // Wait for all upserters to finish.)
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        executorService.shutdown();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD)) {
            readRow(connection, id, "First event", 5);
        }
    }

    @Test
    void testUpdateChangingNoRecords() throws Exception {
        String jdbcUrl = mariadb.getJdbcUrl();

        // Using properties to allow specifying flag(s) for connection
        Properties properties = new Properties();
        properties.put("user", DB_USER);
        properties.put("password", PASSWORD);

        // Specifying that
        properties.put("useAffectedRows", true);

        Connection connection = DriverManager.getConnection(jdbcUrl, properties);

        UUID id = UUID.randomUUID();

        Upserter upserter1 = new Upserter(connection, id, "Another event", 5);
        upserter1.performUpsert();

//        readRow(connection, id, "Another event", 5);

        // Expect this to go down the update path, but not change any data
        Upserter upserter2 = new Upserter(connection, id, "Another event", 4);
        upserter2.performUpsert();

        readRow(connection, id, "Another event", 5);
    }

    @Test
    void outputMetadata() throws SQLException {
        String jdbcUrl = mariadb.getJdbcUrl();

        Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD);
    /* In a real environment, metadata may be useful for ensuring that the provisioned resource
    that is connected to will have the expected capabilities.
    */
        DatabaseMetaData metadata = connection.getMetaData();

        int maxConnections = metadata.getMaxConnections();
        System.out.println("Max connections: " + maxConnections);

        String systemFunctions = metadata.getSystemFunctions();
        System.out.println("System functions: " + systemFunctions);

        String timeDateFunctions = metadata.getTimeDateFunctions();
        System.out.println("Time date functions: " + timeDateFunctions);


    }

    private void readRow(Connection connection, UUID expectedId, String expectedName, int expectedVersion) throws SQLException {
        try (PreparedStatement readStatement = connection.prepareStatement("SELECT id, name, version from event" +
                " where id = ?")) {
            // assert that one result and has name of "First event"
            readStatement.setObject(1, expectedId);
            readStatement.execute();

            try (ResultSet resultSet = readStatement.getResultSet()) {
                boolean firstResult = resultSet.next();
                assertThat(firstResult).isTrue();
                UUID id = resultSet.getObject("id", UUID.class);
                int version = resultSet.getInt("version");
                assertThat(id).isEqualTo(expectedId);
                assertThat(version).isEqualTo(expectedVersion);
                assertThat(resultSet.getString("name")).isEqualTo(expectedName);

                // Verifying that no unexpected additional results are returned.
                boolean subsequentResult = resultSet.next();
                assertThat(subsequentResult).isFalse();
            }
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE event (id UUID PRIMARY KEY, " +
                    "name varchar(255) NOT NULL," +
                    "version bigint NOT NULL DEFAULT 0)");
        }
    }
}
