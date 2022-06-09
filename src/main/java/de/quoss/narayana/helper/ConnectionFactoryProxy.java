package de.quoss.narayana.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAJMSContext;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;

/**
 * <p>
 *   Helper class to support en- and delisting of jms xa resources in narayana user transactions.
 *   Supports jakarta jms spec version 2.0.3.
 * </p>
 */
public class ConnectionFactoryProxy implements XAQueueConnectionFactory, XATopicConnectionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionFactoryProxy.class);

    private static final String MSG_CF_NO_QCF = "Connection factory is not of type QueueConnectionFactory.";

    private static final String MSG_CF_NO_TCF = "Connection factory is not of type TopicConnectionFactory.";

    private static final String MSG_CF_NO_XACF = "Connection factory is not of type XAConnectionFactory.";

    private static final String MSG_CF_NO_XAQCF = "Connection factory is not of type XAQueueConnectionFactory.";

    private static final String MSG_CF_NO_XATCF = "Connection factory is not of type XATopicConnectionFactory.";

    private static final String MSG_CF_NULL = "Connection factory is null.";

    private static final String TRC_START_FMT = "{} start";

    private static final String TRC_START_FMT_USER_PWD = "{} start [userName={},password=...]";

    private static final String TRC_END_FMT = "{} end";

    private static final String TRC_END_FMT_RESULT = "{} end [result={}]";

    private final ConnectionFactory connectionFactory;

    private final TransactionHelper transactionHelper;

    public ConnectionFactoryProxy(final ConnectionFactory connectionFactory, final TransactionHelper transactionHelper) {
        final String methodName = "ConnectionFactoryProxy(ConnectionFactory, TransactionHelper)";
        if (connectionFactory == null) {
            throw new NarayanaHelperException("Connection factory must not be null.");
        }
        LOGGER.trace("{} start [connectionFactory={},connectionFactory.class.name={},transactionHelper={}]", methodName, connectionFactory, connectionFactory.getClass().getName(), transactionHelper);
        this.connectionFactory = connectionFactory;
        if (transactionHelper == null) {
            throw new NarayanaHelperException("Transaction helper must not be null.");
        }
        this.transactionHelper = transactionHelper;
        LOGGER.trace(TRC_END_FMT, methodName);
    }

    // ---- ConnectionFactory JMS Spec 1.1 API Methods -------------------------------

    @Override
    public Connection createConnection() throws JMSException {
        final String methodName = "createConnection()";
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(TRC_START_FMT, methodName);
            // let's see who creates the connection
            Exception e = new Exception("trace");
            LOGGER.trace("Trace exception:", e);
        }
        Connection result;
        if (connectionFactory == null) {
            throw new NarayanaHelperException(MSG_CF_NULL);
        } else {
            // if we are capable of it we hand out a proxied xa transaction
            if (connectionFactory instanceof XAConnectionFactory) {
                result = new ConnectionProxy(((XAConnectionFactory) connectionFactory).createXAConnection(), transactionHelper);
            } else {
                result = connectionFactory.createConnection();
            }
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public Connection createConnection(final String userName, final String password) throws JMSException {
        final String methodName = "createConnection(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        Connection result;
        if (connectionFactory == null) {
            throw new NarayanaHelperException(MSG_CF_NULL);
        } else {
            // if we are capable of it we hand out a proxied xa transaction
            if (connectionFactory instanceof XAConnectionFactory) {
                result = new ConnectionProxy(((XAConnectionFactory) connectionFactory).createXAConnection(userName, password), transactionHelper);
            } else {
                result = connectionFactory.createConnection(userName, password);
            }
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    // ---- ConnectionFactory JMS Spec 2.0 API Methods -------------------------------

    @Override
    public JMSContext createContext() {
        final String methodName = "createContext()";
        LOGGER.trace(TRC_START_FMT, methodName);
        JMSContext result;
        if (connectionFactory == null) {
            throw new NarayanaHelperException(MSG_CF_NULL);
        } else {
            try {
                if (transactionHelper.isTransactionAvailable()) {
                    result = createAndRegisterXAContext();
                } else {
                    result = new ContextProxy(connectionFactory.createContext(), transactionHelper);
                }
            } catch (JMSException e) {
                throw new NarayanaHelperException(e);
            }
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public JMSContext createContext(final String userName, final String password) {
        final String methodName = "createContext(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        JMSContext result;
        if (connectionFactory == null) {
            throw new NarayanaHelperException(MSG_CF_NULL);
        } else {
            try {
                if (transactionHelper.isTransactionAvailable()) {
                    result = createAndRegisterXAContext(userName, password);
                } else {
                    result = new ContextProxy(connectionFactory.createContext(userName, password), transactionHelper);
                }
            } catch (JMSException e) {
                throw new NarayanaHelperException(e);
            }
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public JMSContext createContext(final String userName, final String password, final int sessionMode) {
        final String methodName = "createContext(String, String, int)";
        LOGGER.trace("{} start [userName={},password=...,sessionMode={}]", methodName, userName, sessionMode);
        JMSContext result;
        if (connectionFactory == null) {
            throw new NarayanaHelperException(MSG_CF_NULL);
        } else {
            try {
                if (transactionHelper.isTransactionAvailable()) {
                    result = createAndRegisterXAContext(userName, password);
                } else {
                    result = new ContextProxy(connectionFactory.createContext(userName, password, sessionMode), transactionHelper);
                }
            } catch (JMSException e) {
                throw new NarayanaHelperException(e);
            }
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public JMSContext createContext(final int sessionMode) {
        final String methodName = "createContext(int)";
        LOGGER.trace("{} start [sessionMode={}]", methodName, sessionMode);
        JMSContext result;
        if (connectionFactory == null) {
            throw new NarayanaHelperException(MSG_CF_NULL);
        } else {
            try {
                if (transactionHelper.isTransactionAvailable()) {
                    result = createAndRegisterXAContext();
                } else {
                    result = new ContextProxy(connectionFactory.createContext(sessionMode), transactionHelper);
                }
            } catch (JMSException e) {
                throw new NarayanaHelperException(e);
            }
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    // ---- XAConnectionFactory JMS Spec 1.1 API Methods ----------------------------------

    @Override
    public XAConnection createXAConnection() throws JMSException {
        final String methodName = "createXAConnection()";
        LOGGER.trace(TRC_START_FMT, methodName);
        XAConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XAConnectionFactory) {
            result = new ConnectionProxy(((XAConnectionFactory) connectionFactory).createXAConnection(), transactionHelper);
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_XACF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public XAConnection createXAConnection(final String userName, final String password) throws JMSException {
        final String methodName = "createXAConnection(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        XAConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XAConnectionFactory) {
            result = new ConnectionProxy(((XAConnectionFactory) connectionFactory).createXAConnection(userName, password), transactionHelper);
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_XACF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    // ---- XAConnectionFactory JMS Spec 2.0 API Methods ----------------------------------

    @Override
    public XAJMSContext createXAContext() {
        final String methodName = "createXAContext()";
        LOGGER.trace(TRC_START_FMT, methodName);
        XAJMSContext result;
        if (connectionFactory instanceof XAConnectionFactory) {
            try {
                if (transactionHelper.isTransactionAvailable()) {
                    result = createAndRegisterXAContext();
                } else {
                    result = new ContextProxy(((XAConnectionFactory) connectionFactory).createXAContext(), transactionHelper);
                }
            } catch (JMSException e) {
                throw new NarayanaHelperException(e);
            }
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_XACF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public XAJMSContext createXAContext(final String userName, final String password) {
        final String methodName = "createXAContext(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        XAJMSContext result;
        if (connectionFactory instanceof XAConnectionFactory) {
            try {
                if (transactionHelper.isTransactionAvailable()) {
                    result = createAndRegisterXAContext(userName, password);
                } else {
                    result = new ContextProxy(((XAConnectionFactory) connectionFactory).createXAContext(userName, password), transactionHelper);
                }
            } catch (JMSException e) {
                throw new NarayanaHelperException(e);
            }
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_XACF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    // ---- QueueConnectionFactory API Methods ----------------------------------

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        final String methodName = "createQueueConnection()";
        LOGGER.trace(TRC_START_FMT, methodName);
        QueueConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XAQueueConnectionFactory) {
            result = new ConnectionProxy(((XAQueueConnectionFactory) connectionFactory).createXAQueueConnection(), transactionHelper);
        } else if (connectionFactory instanceof QueueConnectionFactory) {
            result = ((QueueConnectionFactory) connectionFactory).createQueueConnection();
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_QCF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public QueueConnection createQueueConnection(final String userName, final String password) throws JMSException {
        final String methodName = "createQueueConnection(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        QueueConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XAQueueConnectionFactory) {
            result = new ConnectionProxy(((XAQueueConnectionFactory) connectionFactory).createXAQueueConnection(userName, password), transactionHelper);
        } else if (connectionFactory instanceof QueueConnectionFactory) {
            result = ((QueueConnectionFactory) connectionFactory).createQueueConnection(userName, password);
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_QCF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    // ---- XAQueueConnectionFactory API Methods ----------------------------------

    @Override
    public XAQueueConnection createXAQueueConnection() throws JMSException {
        final String methodName = "createXAQueueConnection()";
        LOGGER.trace(TRC_START_FMT, methodName);
        XAQueueConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XAQueueConnectionFactory) {
            result = new ConnectionProxy(((XAQueueConnectionFactory) connectionFactory).createXAQueueConnection(), transactionHelper);
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_XAQCF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public XAQueueConnection createXAQueueConnection(final String userName, final String password) throws JMSException {
        final String methodName = "createXAQueueConnection(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        XAQueueConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XAQueueConnectionFactory) {
            result = new ConnectionProxy(((XAQueueConnectionFactory) connectionFactory).createXAQueueConnection(userName, password), transactionHelper);
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_XAQCF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    // ---- TopicConnectionFactory API Methods ----------------------------------

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        final String methodName = "createTopicConnection()";
        LOGGER.trace(TRC_START_FMT, methodName);
        TopicConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XATopicConnectionFactory) {
            result = new ConnectionProxy(((XATopicConnectionFactory) connectionFactory).createXATopicConnection(), transactionHelper);
        } else if (connectionFactory instanceof TopicConnectionFactory) {
            result = ((TopicConnectionFactory) connectionFactory).createTopicConnection();
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_TCF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public TopicConnection createTopicConnection(final String userName, final String password) throws JMSException {
        final String methodName = "createTopicConnection(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        TopicConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XATopicConnectionFactory) {
            result = new ConnectionProxy(((XATopicConnectionFactory) connectionFactory).createXATopicConnection(userName, password), transactionHelper);
        } else if (connectionFactory instanceof TopicConnectionFactory) {
            result = ((TopicConnectionFactory) connectionFactory).createTopicConnection(userName, password);
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_TCF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    // ---- XATopicConnectionFactory API Methods ----------------------------------

    @Override
    public XATopicConnection createXATopicConnection() throws JMSException {
        final String methodName = "createXATopicConnection()";
        LOGGER.trace(TRC_START_FMT, methodName);
        XATopicConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XATopicConnectionFactory) {
            result = new ConnectionProxy(((XATopicConnectionFactory) connectionFactory).createXATopicConnection(), transactionHelper);
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_XATCF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    @Override
    public XATopicConnection createXATopicConnection(String userName, String password) throws JMSException {
        final String methodName = "createXATopicConnection(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        XATopicConnection result;
        // if we are capable of it we hand out a proxied xa transaction
        if (connectionFactory instanceof XATopicConnectionFactory) {
            result = new ConnectionProxy(((XATopicConnectionFactory) connectionFactory).createXATopicConnection(userName, password), transactionHelper);
        } else {
            throw new NarayanaHelperException(MSG_CF_NO_XATCF);
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    // ---- Private Methods ----------------------------------

    private XAJMSContext createAndRegisterXAContext() throws JMSException {
        final String methodName = "createAndRegisterXAContext()";
        LOGGER.trace(TRC_START_FMT, methodName);
        XAJMSContext context = ((XAConnectionFactory) connectionFactory).createXAContext();
        XAJMSContext result = createAndRegisterXAContext(context);
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    private XAJMSContext createAndRegisterXAContext(final String userName, final String password) throws JMSException {
        final String methodName = "createAndRegisterXAContext(String, String)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, userName);
        XAJMSContext context = ((XAConnectionFactory) connectionFactory).createXAContext(userName, password);
        XAJMSContext result = createAndRegisterXAContext(context);
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

    private XAJMSContext createAndRegisterXAContext(final XAJMSContext context) throws JMSException {
        final String methodName = "createAndRegisterXAContext(XAJMSContext)";
        LOGGER.trace(TRC_START_FMT_USER_PWD, methodName, context);
        XAJMSContext result = new ContextProxy(context, transactionHelper);
        try {
            transactionHelper.registerXAResource(context.getXAResource());
        } catch (JMSException e) {
            context.close();
            throw e;
        }
        LOGGER.trace(TRC_END_FMT_RESULT, methodName, result);
        return result;
    }

}
