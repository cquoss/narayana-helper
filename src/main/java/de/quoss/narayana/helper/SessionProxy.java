package de.quoss.narayana.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;
import javax.transaction.xa.XAResource;
import java.io.Serializable;

public class SessionProxy implements XAQueueSession, XATopicSession {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionProxy.class);

    private final Session session;

    private final TransactionHelper transactionHelper;

    public SessionProxy(final Session session, final TransactionHelper transactionHelper) {
        final String methodName = "SessionProxy(Session, TransactionHelper)";
        LOGGER.trace("{} start [session={},transactionHelper={}]", methodName, session, transactionHelper);
        if (session == null) {
            throw new NarayanaHelperException("Session must not be null.");
        }
        this.session = session;
        this.transactionHelper = transactionHelper;
        LOGGER.trace("{} end", methodName);
    }

    @Override
    public Session getSession() throws JMSException {
        return session;
    }

    @Override
    public XAResource getXAResource() {
        return ((XASession) session).getXAResource();
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return session.createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        return session.createMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        return session.createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return session.createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(final Serializable object) throws JMSException {
        return session.createObjectMessage(object);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        return session.createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        return session.createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(final String text) throws JMSException {
        return session.createTextMessage(text);
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return session.getTransacted();
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return session.getAcknowledgeMode();
    }

    @Override
    public void commit() throws JMSException {
        session.commit();
    }

    @Override
    public void rollback() throws JMSException {
        session.rollback();
    }

    @Override
    public void close() throws JMSException {
        final String methodName = "close()";
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} start", methodName);
            // let's see who closes the session
            Exception e = new Exception("trace");
            LOGGER.trace("Trace exception:", e);
        }
        LOGGER.debug("{} [session.class.name={}]", methodName, session.getClass().getName());
        if (transactionHelper.isTransactionAvailable()) {
            transactionHelper.deregisterXAResource(((XASession) session).getXAResource());
            transactionHelper.registerSynchronization(new SessionClosingSynchronization(session));
        } else {
            session.close();
        }
        LOGGER.trace("{} end", methodName);
    }

    @Override
    public void recover() throws JMSException {
        session.recover();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return session.getMessageListener();
    }

    @Override
    public void setMessageListener(final MessageListener listener) throws JMSException {
        session.setMessageListener(listener);
    }

    @Override
    public void run() {
        session.run();
    }

    @Override
    public MessageProducer createProducer(final Destination destination) throws JMSException {
        return session.createProducer(destination);
    }

    @Override
    public MessageConsumer createConsumer(final Destination destination) throws JMSException {
        return session.createConsumer(destination);
    }

    @Override
    public MessageConsumer createConsumer(final Destination destination, final String messageSelector) throws JMSException {
        return session.createConsumer(destination, messageSelector);
    }

    @Override
    public MessageConsumer createConsumer(final Destination destination, final String messageSelector, final boolean noLocal) throws JMSException {
        return session.createConsumer(destination, messageSelector, noLocal);
    }

    @Override
    public MessageConsumer createSharedConsumer(final Topic topic, final String s) throws JMSException {
        return session.createSharedConsumer(topic, s);
    }

    @Override
    public MessageConsumer createSharedConsumer(final Topic topic, final String s, final String s1) throws JMSException {
        return session.createSharedConsumer(topic, s, s1);
    }

    @Override
    public Queue createQueue(final String queueName) throws JMSException {
        return session.createQueue(queueName);
    }

    @Override
    public Topic createTopic(final String topicName) throws JMSException {
        return session.createTopic(topicName);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(final Topic topic, final String name) throws JMSException {
        return session.createDurableSubscriber(topic, name);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(final Topic topic, final String name, final String messageSelector, final boolean noLocal) throws JMSException {
        final String methodName = "createDurableSubscriber(Topic, String, String, boolean)";
        LOGGER.trace("{} start [topic={},name={},messageSelector={},noLocal={}]", methodName, topic, name, messageSelector, noLocal);
        TopicSubscriber result = session.createDurableSubscriber(topic, name, messageSelector, noLocal);
        LOGGER.trace("{} end [result={}]", methodName, result);
        return result;
    }

    @Override
    public MessageConsumer createDurableConsumer(final Topic topic, final String s) throws JMSException {
        return session.createDurableConsumer(topic, s);
    }

    @Override
    public MessageConsumer createDurableConsumer(final Topic topic, final String s, final String s1, final boolean b) throws JMSException {
        return session.createDurableConsumer(topic, s, s1, b);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(final Topic topic, final String s) throws JMSException {
        return session.createSharedDurableConsumer(topic, s);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(final Topic topic, final String s, final String s1) throws JMSException {
        return session.createSharedDurableConsumer(topic, s, s1);
    }

    @Override
    public QueueBrowser createBrowser(final Queue queue) throws JMSException {
        return session.createBrowser(queue);
    }

    @Override
    public QueueBrowser createBrowser(final Queue queue, final String messageSelector) throws JMSException {
        return session.createBrowser(queue, messageSelector);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return session.createTemporaryQueue();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return session.createTemporaryTopic();
    }

    @Override
    public void unsubscribe(final String name) throws JMSException {
        session.unsubscribe(name);
    }

    @Override
    public QueueSession getQueueSession() throws JMSException {
        if (session instanceof XAQueueSession) {
            return ((XAQueueSession) session).getQueueSession();
        } else {
            throw new NarayanaHelperException("Session is not of type XAQueueSession.");
        }
    }

    @Override
    public TopicSession getTopicSession() throws JMSException {
        if (session instanceof XATopicSession) {
            return ((XATopicSession) session).getTopicSession();
        } else {
            throw new NarayanaHelperException("Session is not of type XATopicSession.");
        }
    }
}
