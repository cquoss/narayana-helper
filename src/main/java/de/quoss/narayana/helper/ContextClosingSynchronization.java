package de.quoss.narayana.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSContext;
import javax.transaction.Synchronization;

/**
 * Synchronization to close JMS session at the end of the transaction.
 */
public class ContextClosingSynchronization implements Synchronization {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContextClosingSynchronization.class);

    private final JMSContext context;

    /**
     * @param context context to be closed.
     */
    public ContextClosingSynchronization(final JMSContext context) {
        if (context == null) {
            throw new NarayanaHelperException("JMS context must not be null.");
        }
        this.context = context;
    }

    @Override
    public void beforeCompletion() {
        // Nothing to do
    }

    /**
     * Close the context no matter what the status of the transaction is.
     *
     * @param status the status of the completed transaction
     */
    @Override
    public void afterCompletion(final int status) {
        LOGGER.trace("Closing context {}", context);
        context.close();
    }

}
