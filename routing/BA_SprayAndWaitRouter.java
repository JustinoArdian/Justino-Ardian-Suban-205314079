package routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import core.Connection;
import core.DTNHost;
import core.Message;
import core.Settings;
import core.SimClock;
import core.Tuple;
import java.util.Collections;
import java.util.Comparator;

/**
 * Implementation of Spray and Wait router with average delivery probability.
 */
public class BA_SprayAndWaitRouter extends ActiveRouter {

    // Constants for the Spray and Wait protocol
    public static final String NROF_COPIES = "nrofCopies";
    public static final String BINARY_MODE = "binaryMode";
    public static final String SPRAYANDWAIT_NS = "SprayAndWaitRouter";
    public static final String MSG_COUNT_PROPERTY = SPRAYANDWAIT_NS + ".copies";

    protected int initialNrofCopies;
    protected boolean isBinary;

    // Constants and settings for the PROPHET protocol
    public static final double P_INIT = 0.75;
    public static final double DEFAULT_BETA = 0.25;
    public static final double GAMMA = 0.98;

    public static final String SECONDS_IN_UNIT_S = "secondsInTimeUnit";
    public static final String BETA_S = "beta";

    private int secondsInTimeUnit;
    private double beta;
    private Map<DTNHost, Double> preds;
    private double lastAgeUpdate;

    // Connection state
    private boolean isUp;

    // Additional state for average delivery probability calculation
    private Map<DTNHost, Double> firstEncounterTime;

    public BA_SprayAndWaitRouter(Settings s) {
        super(s);
        Settings snwSettings = new Settings(SPRAYANDWAIT_NS);

        initialNrofCopies = snwSettings.getInt(NROF_COPIES);
        isBinary = snwSettings.getBoolean(BINARY_MODE);

        if (snwSettings.contains(BETA_S)) {
            beta = snwSettings.getDouble(BETA_S);
        } else {
            beta = DEFAULT_BETA;
        }

        initPreds();
        this.isUp = true;
        this.firstEncounterTime = new HashMap<>();
    }

    protected BA_SprayAndWaitRouter(BA_SprayAndWaitRouter r) {
        super(r);
        this.initialNrofCopies = r.initialNrofCopies;
        this.isBinary = r.isBinary;
        this.secondsInTimeUnit = r.secondsInTimeUnit;
        this.beta = r.beta;
        initPreds();
        this.firstEncounterTime = new HashMap<>();
//        this.firstEncounterTime = new HashMap<>(r.firstEncounterTime);

    }

    @Override
    public int receiveMessage(Message m, DTNHost from) {
        return super.receiveMessage(m, from);
    }

    @Override
    public Message messageTransferred(String id, DTNHost from) {
        Message msg = super.messageTransferred(id, from);
        Integer nrofCopies = (Integer) msg.getProperty(MSG_COUNT_PROPERTY);
        DTNHost destination = msg.getTo();
        DTNHost otherHost = from;
        BA_SprayAndWaitRouter othRouter = (BA_SprayAndWaitRouter) from.getRouter();

        // Get the average delivery probabilities for both nodes
//        double pAvgB = getPredFor(otherHost);
        double pAvgB = othRouter.getPredFor(destination);
        double pAvgA = getPredFor(destination);

        // Check if node B's average delivery probability to the destination is higher than node A's
//        if (pAvgB > pAvgA) {
            if (isBinary) {
                // In binary mode, node A and node B each get half of the message copies
                nrofCopies = (int) Math.ceil(nrofCopies / 2.0);
            }
            msg.updateProperty(MSG_COUNT_PROPERTY, nrofCopies);
//        }

        return msg;
    }

    @Override
    public boolean createNewMessage(Message msg) {
        makeRoomForNewMessage(msg.getSize());
        msg.setTtl(this.msgTtl);
        msg.addProperty(MSG_COUNT_PROPERTY, initialNrofCopies);
        addToMessages(msg, true);
        return true;
    }

    @Override
    public void update() {
        super.update();
        if (!canStartTransfer() || isTransferring()) {
            return;
        }

        if (exchangeDeliverableMessages() != null) {
            return;
        }

        List<Message> copiesLeft = sortByQueueMode(getMessagesWithCopiesLeft());

        if (copiesLeft.size() > 1) {
            this.tryMessagesToConnections(copiesLeft, getConnections());
        } else {
            tryOtherMessages();
        }
    }

    protected List<Message> getMessagesWithCopiesLeft() {
        List<Message> list = new ArrayList<Message>();

        for (Message m : getMessageCollection()) {
            Integer nrofCopies = (Integer) m.getProperty(MSG_COUNT_PROPERTY);
            assert nrofCopies != null : "SnW message " + m + " didn't have "
                    + "nrof copies property!";
            if (nrofCopies > 1) {
                list.add(m);
            }
        }

        return list;
    }

    @Override
    protected void transferDone(Connection con) {
        Integer nrofCopies;
        String msgId = con.getMessage().getId();
        Message msg = getMessage(msgId);

        if (msg == null) {
            return;
        }

        nrofCopies = (Integer) msg.getProperty(MSG_COUNT_PROPERTY);
        if (isBinary) {
            nrofCopies /= 2;
        } else {
            nrofCopies--;
        }
        msg.updateProperty(MSG_COUNT_PROPERTY, nrofCopies);
    }

    @Override
    public BA_SprayAndWaitRouter replicate() {
        return new BA_SprayAndWaitRouter(this);
    }

    private void initPreds() {
        this.preds = new HashMap<>();
    }

    @Override
    public void changedConnection(Connection con) {
        if (con.isUp()) {
            DTNHost otherHost = con.getOtherNode(getHost());
            updateDeliveryPredFor(otherHost);
            updateTransitivePreds(otherHost);
        }
    }

    private void updateDeliveryPredFor(DTNHost host) {
        double currentTime = SimClock.getTime();
        double oldValue = P_INIT;
        double newValue = oldValue + (1 - oldValue) * P_INIT;

        // Calculate t1 and t2
        double t1, t2;
        if (firstEncounterTime.containsKey(host)) {
            t1 = firstEncounterTime.get(host);
            t2 = currentTime - t1;
        } else {
            t1 = currentTime;
            t2 = 0;
            firstEncounterTime.put(host, t1);
        }

        // Calculate the average delivery probability as per the paper
        double avgValue = (oldValue * t1 + newValue * t2) / (t1 + t2);
        preds.put(host, avgValue);
    }

    public double getPredFor(DTNHost host) {
        ageDeliveryPreds();
        return preds.getOrDefault(host, 0.0);
    }

    private void updateTransitivePreds(DTNHost host) {
        MessageRouter otherRouter = host.getRouter();
        if (!(otherRouter instanceof BA_SprayAndWaitRouter)) {
            return;
        }

        double pForHost = getPredFor(host);
        Map<DTNHost, Double> othersPreds = ((BA_SprayAndWaitRouter) otherRouter).getDeliveryPreds();

        for (Map.Entry<DTNHost, Double> e : othersPreds.entrySet()) {
            if (e.getKey() == getHost()) {
                continue;
            }

            double pOld = getPredFor(e.getKey());
            double pNew = pOld + (1 - pOld) * pForHost * e.getValue() * beta;
            preds.put(e.getKey(), pNew);
        }
    }

    private void ageDeliveryPreds() {
        double timeDiff = (SimClock.getTime() - this.lastAgeUpdate) / secondsInTimeUnit;

        if (timeDiff == 0) {
            return;
        }

        double mult = Math.pow(GAMMA, timeDiff);
        for (Map.Entry<DTNHost, Double> e : preds.entrySet()) {
            e.setValue(e.getValue() * mult);
        }

        this.lastAgeUpdate = SimClock.getTime();
    }

    private Map<DTNHost, Double> getDeliveryPreds() {
        ageDeliveryPreds();
        return this.preds;
    }

    /**
     * Tries to send all other messages to all connected hosts ordered by their
     * delivery probability
     *
     * @return The return value of {@link #tryMessagesForConnected(List)}
     */
    private Tuple<Message, Connection> tryOtherMessages() {
        List<Tuple<Message, Connection>> messages
                = new ArrayList<Tuple<Message, Connection>>();

        Collection<Message> msgCollection = getMessageCollection();

        /* for all connected hosts collect all messages that have a higher
		   probability of delivery by the other host */
        for (Connection con : getConnections()) {
            DTNHost other = con.getOtherNode(getHost());
            BA_SprayAndWaitRouter othRouter = (BA_SprayAndWaitRouter) other.getRouter();

            if (othRouter.isTransferring()) {
                continue; // skip hosts that are transferring
            }

            for (Message m : msgCollection) {
                if (othRouter.hasMessage(m.getId())) {
                    continue; // skip messages that the other one has
                }
//                tryAllMessagesToAllConnections();
                if (othRouter.getPredFor(m.getTo()) > getPredFor(m.getTo())) {
                    // the other node has higher probability of delivery
                    messages.add(new Tuple<Message, Connection>(m, con));
                }
            }
        }

        if (messages.size() == 0) {
            return null;
        }

        // sort the message-connection tuples
        Collections.sort(messages, new TupleComparator());

        return tryMessagesForConnected(messages);	// try to send messages
    }

    /**
     * Comparator for Message-Connection-Tuples that orders the tuples by their
     * delivery probability by the host on the other side of the connection
     * (GRTRMax)
     */
    private class TupleComparator implements Comparator<Tuple<Message, Connection>> {

        public int compare(Tuple<Message, Connection> tuple1,
                Tuple<Message, Connection> tuple2) {
            // delivery probability of tuple1's message with tuple1's connection
            double p1 = ((BA_SprayAndWaitRouter) tuple1.getValue().
                    getOtherNode(getHost()).getRouter()).getPredFor(
                    tuple1.getKey().getTo());
            // -"- tuple2...
            double p2 = ((BA_SprayAndWaitRouter) tuple2.getValue().
                    getOtherNode(getHost()).getRouter()).getPredFor(
                    tuple2.getKey().getTo());

            // bigger probability should come first
            if (p2 - p1 == 0) {
                /* equal probabilities -> let queue mode decide */
                return compareByQueueMode(tuple1.getKey(), tuple2.getKey());
            } else if (p2 - p1 < 0) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}
