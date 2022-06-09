package de.quoss.narayana.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.transaction.Synchronization;

/**
 * Synchronization to close JMS session at the end of the transaction.
 */
public class SessionClosingSynchronization implements Synchronization {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionClosingSynchronization.class);

    private final Session session;

    /**
     * @param session session to be closed.
     */
    public SessionClosingSynchronization(Session session) {
        this.session = session;
    }

    @Override
    public void beforeCompletion() {
        // Nothing to do
    }

    /**
     * Close the session no matter what the status of the transaction is.
     *
     * @param status the status of the completed transaction
     */
    @Override
    public void afterCompletion(int status) {
        LOGGER.trace("Closing session {}", session);

        try {
            session.close();
        } catch (JMSException e) {
            LOGGER.warn(String.format("Failed to close jms session %s.", session), e);
        }
    }

}
