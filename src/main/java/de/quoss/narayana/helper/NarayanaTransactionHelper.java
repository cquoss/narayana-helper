package de.quoss.narayana.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

public class NarayanaTransactionHelper implements TransactionHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarayanaTransactionHelper.class);

    private static final String TRC_FMT_END = "{} end";

    private final TransactionManager transactionManager;

    public NarayanaTransactionHelper(final TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public boolean isTransactionAvailable() throws JMSException {
        final String methodName = "isTransactionAvailable()";
        LOGGER.trace("{} start", methodName);
        Transaction txn = getTransaction();
        LOGGER.debug("{} [txn={}]", methodName, txn);
        int status = getStatus();
        boolean result = status != Status.STATUS_NO_TRANSACTION;
        LOGGER.trace("{} end [result={}]", methodName, result);
        return result;
    }

    @Override
    public void registerSynchronization(final Synchronization synchronization) throws JMSException {
        final String methodName = "registerSynchronization(Synchronization)";
        LOGGER.trace("{} start [synchronization={}]", methodName, synchronization);
        try {
            getTransaction().registerSynchronization(synchronization);
        } catch (IllegalStateException | RollbackException | SystemException e) {
            throw getJmsException(e.getMessage(), e);
        }
        LOGGER.trace(TRC_FMT_END, methodName);
    }

    @Override
    public void registerXAResource(final XAResource xaResource) throws JMSException {
        final String methodName = "registerXAResource(XAResource)";
        LOGGER.trace("{} start [xaResource={}]", methodName, xaResource);
        try {
            if (!getTransaction().enlistResource(xaResource)) {
                throw getJmsException("Error enlisting resource.", null);
            }
        } catch (RollbackException | IllegalStateException | SystemException e) {
            throw getJmsException(e.getMessage(), e);
        }
        LOGGER.trace(TRC_FMT_END, methodName);
    }

    @Override
    public void deregisterXAResource(final XAResource xaResource) throws JMSException {
        final String methodName = "deregisterXAResource(XAResource)";
        LOGGER.trace("{} start [xaResource={}]", methodName, xaResource);
        try {
            if (!getTransaction().delistResource(xaResource, XAResource.TMSUCCESS)) {
                throw getJmsException("Error delisting resource.", null);
            }
        } catch (IllegalStateException | SystemException e) {
            throw getJmsException(e.getMessage(), e);
        }
        LOGGER.trace(TRC_FMT_END, methodName);
    }

    // ---- Private Helper Methods ---------------------------------------

    private Transaction getTransaction() throws JMSException {
        try {
            return transactionManager.getTransaction();
        } catch (SystemException e) {
            throw getJmsException(e.getMessage(), e);
        }
    }

    private int getStatus() throws JMSException {
        try {
            return transactionManager.getTransaction().getStatus();
        } catch (SystemException e) {
            throw getJmsException(e.getMessage(), e);
        }
    }

    private JMSException getJmsException(final String message, final Exception cause) {
        JMSException jmsException = new JMSException(message);
        jmsException.setLinkedException(cause);
        return jmsException;
    }

}
