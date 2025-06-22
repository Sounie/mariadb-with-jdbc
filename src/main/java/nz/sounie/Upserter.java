package nz.sounie;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Self-contained worker object for upserting a specified version to a specified id.
 * <p>
 *     Intended to be used for trying out testing race conditions and the influence of transactions
 *     with different isolation levels.
 * </p>
 */
public class Upserter {
    final Connection connection;
    final PreparedStatement statement;
    final int version;

    boolean success = false;

    /**
     *
     * @param connection with isolation level set up, based on options available from Connection
     * @param id primary key
     * @param version
     */
    Upserter(Connection connection, UUID id, String name, int version) {
        try {
            this.connection = connection;
            this.version = version;
            // MariaDB doesn't appear to allow specifying a where clause for "on duplicate key update"
            // So we have an update with an ugly check for version on each individual property

            // May need to declare a procedure

            this.statement =  connection.prepareStatement(
                    """
INSERT INTO event (id, name, version) VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE
    name = (IF(version < VALUE(version), VALUE(name), name)),
    version = (IF(version < VALUE(version), VALUE(version), version))
"""
            );

        } catch (SQLException e) {
            throw new IllegalStateException("Failed to prepare statement", e);
        }

        try {
            statement.setObject(1, id);
            statement.setString(2, name);
            statement.setInt(3, version);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to set up state for prepared statement.", e);
        }
    }

    void performUpsert()
    {
        boolean rowChanged = false;
        try {
            int rowsChanged = statement.executeUpdate();

            // Checking to see values returned, as update for on duplicate is documented as returning 2
            // Reference: https://mariadb.com/docs/server/reference/sql-statements/data-manipulation/inserting-loading-data/insert-on-duplicate-key-update
            if (rowsChanged == 1) {
                rowChanged = true;
            } else if (rowsChanged == 2) {
                rowChanged = true;
            }

//            if (version % 5 == 0) {
//                // Attempting to simulate sometimes dropping connection mid-transaction
//                System.out.println("Closing connection mid-transaction for version " + version);
//                try {
//                    // Simulating a delay before connection gets dropped, allowing for possibility of pther
//                    // statements to see non-committed version.
//                    Thread.sleep(2000L);
//                    System.out.println("After delay, closing connection for version " + version);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                connection.close();
//            } else {
                connection.commit();

                this.success = true;
//            }
        } catch (SQLException e) {
            try {
                System.err.println("Failed to commit prepared statement, " + e.getMessage() + " Rolling back.");
                connection.rollback();
            }  catch (SQLException ex) {
                System.err.println("Failure during rollback, " + ex.getMessage());
            }
            // Don't need to do anything here, as success state will remain false.
        } finally {
            try {
                if (rowChanged) {
                    if (success) {
                        System.out.println("Version " + version + " inserted / updated");
                    }
                    else {
                        System.out.println("Version " + version + " inserted / updated, but connection closed without commit.");
                    }
                } else {
                    System.out.println("No row changed, presume version was already > " + version);
                }
                statement.close();
            } catch (SQLException e) {
                // We don't regard this as the upsert failing.
                System.err.println("Exception while closing statement: " + e.getMessage());
            }
        }
    }

    public void closeConnection() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            System.err.println("Exception when  closing connection: " + e.getMessage());
        }
    }

    public boolean isSuccess() {
        return success;
    }
}
