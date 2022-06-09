package de.quoss.narayana.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.transaction.Synchronization;

/**
 * Synchronization to close JMS connection at the end of the transaction.
 *
 * @author <a href="mailto:gytis@redhat.com">Gytis Trikleris</a>
 */
public class ConnectionClosingSynchronization implements Synchronization {

    private final Connection connection;

    /**
     * @param connection connection to be closed.
     */
    public ConnectionClosingSynchronization(Connection connection) {
        this.connection = connection;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionClosingSynchronization.class);

    @Override
    public void beforeCompletion() {
        // Nothing to do
    }

    /**
     * Close the connection no matter what the status of the transaction is.
     *
     * @param status the status of the completed transaction
     */
    @Override
    public void afterCompletion(int status) {
        LOGGER.trace("Closing connection {}", connection);

        try {
            connection.close();
        } catch (JMSException e) {
            LOGGER.warn(String.format("Failed to close connection %s.", connection), e);
        }
    }

}
