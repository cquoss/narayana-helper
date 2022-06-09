package de.quoss.narayana.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAJMSContext;
import javax.transaction.xa.XAResource;
import java.io.Serializable;

public class ContextProxy implements XAJMSContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContextProxy.class);

    private final JMSContext context;

    private final TransactionHelper transactionHelper;

    public ContextProxy(final JMSContext context, final TransactionHelper transactionHelper) {
        if (context == null) {
            throw new NarayanaHelperException("JMS context must not be null.");
        }
        this.context = context;
        if (transactionHelper == null) {
            throw new NarayanaHelperException("Transaction helper must not be null.");
        }
        this.transactionHelper = transactionHelper;
    }

    @Override
    public JMSContext getContext() {
        return context;
    }

    @Override
    public XAResource getXAResource() {
        if (context instanceof XAJMSContext) {
            return ((XAJMSContext) context).getXAResource();
        } else {
            throw new NarayanaHelperException("Context is not of type XAJMSContext.");
        }
    }

    @Override
    public JMSContext createContext(final int sessionMode) {
        return context.createContext(sessionMode);
    }

    @Override
    public JMSProducer createProducer() {
        return context.createProducer();
    }

    @Override
    public String getClientID() {
        return context.getClientID();
    }

    @Override
    public void setClientID(final String clientID) {
        context.setClientID(clientID);
    }

    @Override
    public ConnectionMetaData getMetaData() {
        return context.getMetaData();
    }

    @Override
    public ExceptionListener getExceptionListener() {
        return context.getExceptionListener();
    }

    @Override
    public void setExceptionListener(final ExceptionListener listener) {
        context.setExceptionListener(listener);
    }

    @Override
    public void start() {
        context.start();
    }

    @Override
    public void stop() {
        context.stop();
    }

    @Override
    public void setAutoStart(final boolean autoStart) {
        context.setAutoStart(autoStart);
    }

    @Override
    public boolean getAutoStart() {
        return context.getAutoStart();
    }

    @Override
    public void close() {
        final String methodName = "close()";
        LOGGER.trace("{} start", methodName);
        try {
            if (transactionHelper.isTransactionAvailable()) {
                transactionHelper.deregisterXAResource(((XAJMSContext) context).getXAResource());
                transactionHelper.registerSynchronization(new ContextClosingSynchronization(context));
            } else {
                context.close();
            }
        } catch (JMSException e) {
            throw new NarayanaHelperException("Error closing context.", e);
        }
        LOGGER.trace("{} end", methodName);
    }

    @Override
    public BytesMessage createBytesMessage() {
        return context.createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        return context.createMapMessage();
    }

    @Override
    public Message createMessage() {
        return context.createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return context.createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(final Serializable object) {
        return context.createObjectMessage();
    }

    @Override
    public StreamMessage createStreamMessage() {
        return context.createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() {
        return context.createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(final String text) {
        return context.createTextMessage(text);
    }

    @Override
    public boolean getTransacted() {
        return context.getTransacted();
    }

    @Override
    public int getSessionMode() {
        return context.getSessionMode();
    }

    @Override
    public void commit() {
        context.commit();
    }

    @Override
    public void rollback() {
        context.rollback();
    }

    @Override
    public void recover() {
        context.recover();
    }

    @Override
    public JMSConsumer createConsumer(final Destination destination) {
        return context.createConsumer(destination);
    }

    @Override
    public JMSConsumer createConsumer(final Destination destination, final String messageSelector) {
        return context.createConsumer(destination, messageSelector);
    }

    @Override
    public JMSConsumer createConsumer(final Destination destination, final String messageSelector, final boolean noLocal) {
        return context.createConsumer(destination, messageSelector, noLocal);
    }

    @Override
    public Queue createQueue(final String queueName) {
        return context.createQueue(queueName);
    }

    @Override
    public Topic createTopic(final String topicName) {
        return context.createTopic(topicName);
    }

    @Override
    public JMSConsumer createDurableConsumer(final Topic topic, final String name) {
        return context.createDurableConsumer(topic, name);
    }

    @Override
    public JMSConsumer createDurableConsumer(final Topic topic, final String name, final String messageSelector, final boolean noLocal) {
        return context.createDurableConsumer(topic, name, messageSelector, noLocal);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(final Topic topic, final String name) {
        return context.createSharedDurableConsumer(topic, name);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(final Topic topic, final String name, final String messageSelector) {
        return context.createSharedDurableConsumer(topic, name, messageSelector);
    }

    @Override
    public JMSConsumer createSharedConsumer(final Topic topic, final String sharedSubscriptionName) {
        return context.createSharedConsumer(topic, sharedSubscriptionName);
    }

    @Override
    public JMSConsumer createSharedConsumer(final Topic topic, final String sharedSubscriptionName, final String messageSelector) {
        return context.createSharedConsumer(topic, sharedSubscriptionName, messageSelector);
    }

    @Override
    public QueueBrowser createBrowser(final Queue queue) {
        return context.createBrowser(queue);
    }

    @Override
    public QueueBrowser createBrowser(final Queue queue, final String messageSelector) {
        return context.createBrowser(queue, messageSelector);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        return context.createTemporaryQueue();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        return context.createTemporaryTopic();
    }

    @Override
    public void unsubscribe(final String name) {
        context.unsubscribe(name);
    }

    @Override
    public void acknowledge() {
        context.acknowledge();
    }

}
