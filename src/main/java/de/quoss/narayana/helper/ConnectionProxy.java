package de.quoss.narayana.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;

public class ConnectionProxy implements XAQueueConnection, XATopicConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProxy.class);

    private final Connection connection;

    private final TransactionHelper transactionHelper;

    public ConnectionProxy(final Connection connection, final TransactionHelper transactionHelper) {
        final String methodName = "ConnectionProxy(Connection, TransactionHelper)";
        LOGGER.trace("{} start [connection={},transactionHelper={}]", methodName, connection, transactionHelper);
        if (connection == null) {
            throw new NarayanaHelperException("Connection must not be null.");
        }
        this.connection = connection;
        if (transactionHelper == null) {
            throw new NarayanaHelperException("Transaction helper must not be null.");
        }
        this.transactionHelper = transactionHelper;
        LOGGER.trace("{} end", methodName);
    }

    // ---- Connection JMS Spec 1.1 API Methods -----------------------------------------

    @Override
    public void close() throws JMSException {
        if (transactionHelper.isTransactionAvailable()) {
            transactionHelper.registerSynchronization(new ConnectionClosingSynchronization(connection));
        } else {
            connection.close();
        }
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(final Destination destination, final String messageSelector, final ServerSessionPool sessionPool, final int maxMessages) throws JMSException {
        return connection.createConnectionConsumer(destination, messageSelector, sessionPool, maxMessages);
    }

    @Override
    public String getClientID() throws JMSException {
        return connection.getClientID();
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        return connection.getExceptionListener();
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return connection.getMetaData();
    }

    @Override
    public void setClientID(final String clientID) throws JMSException {
        final String methodName = "setClientID(String)";
        LOGGER.trace("{} start [clientID={}]", methodName, clientID);
        connection.setClientID(clientID);
        LOGGER.trace("{} end", methodName);
    }

    @Override
    public void setExceptionListener(final ExceptionListener listener) throws JMSException {
        connection.setExceptionListener(listener);
    }

    @Override
    public void start() throws JMSException {
        connection.start();
    }

    @Override
    public void stop() throws JMSException {
        connection.stop();
    }

    // ---- Connection JMS Spec 2.0 API Methods -----------------------------------------

    @Override
    public Session createSession() throws JMSException {
        if (transactionHelper.isTransactionAvailable()) {
            return createAndRegisterXASession();
        }
        return connection.createSession();
    }

    @Override
    public Session createSession(final int sessionMode) throws JMSException {
        if (transactionHelper.isTransactionAvailable()) {
            return createAndRegisterXASession();
        }
        return connection.createSession(sessionMode);
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(final Topic topic, final String subscriptionName, final String messageSelector, final ServerSessionPool serverSessionPool, final int maxMessages) throws JMSException {
        return connection.createSharedConnectionConsumer(topic, subscriptionName, messageSelector, serverSessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(final Topic topic, final String subscriptionName, final String messageSelector, final ServerSessionPool sessionPool, final int maxMessages) throws JMSException {
        return connection.createSharedDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    // ---- XAConnection API Methods -----------------------------------------

    @Override
    public Session createSession(final boolean transacted, final int acknowledgeMode) throws JMSException {
        if (transactionHelper.isTransactionAvailable()) {
            return createAndRegisterXASession();
        }
        return connection.createSession(transacted, acknowledgeMode);
    }

    @Override
    public XASession createXASession() throws JMSException {
        if (transactionHelper.isTransactionAvailable()) {
            return createAndRegisterXASession();
        }
        return ((XAConnection) connection).createXASession();
    }

    // ---- QueueConnection API Methods -----------------------------------------

    @Override
    public ConnectionConsumer createConnectionConsumer(final Queue queue, final String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return null;
    }

    @Override
    public QueueSession createQueueSession(final boolean transacted, final int acknowledgeMode) throws JMSException {
        return null;
    }

    // ---- XAQueueConnection API Methods -----------------------------------------

    @Override
    public XAQueueSession createXAQueueSession() throws JMSException {
        return null;
    }

    // ---- TopicConnection API Methods -----------------------------------------

    @Override
    public ConnectionConsumer createConnectionConsumer(final Topic topic, final String messageSelector, final ServerSessionPool serverSessionPool, final int maxMessages) throws JMSException {
        return null;
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(final Topic topic, final String subscriptionName, final String messageSelector, final ServerSessionPool sessionPool, final int maxMessages) throws JMSException {
        return connection.createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    // ---- XATopicConnection JMS Spec 1.1 API Methods -----------------------------------------

    @Override
    public XATopicSession createXATopicSession() throws JMSException {
        return null;
    }

    @Override
    public TopicSession createTopicSession(boolean b, int i) throws JMSException {
        return null;
    }

    // ---- Private Helper Methods -----------------------------------------

    private XASession createAndRegisterXASession() throws JMSException {

        XASession session = ((XAConnection) connection).createXASession();
        XASession result = new SessionProxy(session, transactionHelper);

        try {
            transactionHelper.registerXAResource(session.getXAResource());
        } catch (JMSException e) {
            session.close();
            throw e;
        }

        return result;
    }

}
