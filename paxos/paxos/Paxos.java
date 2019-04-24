package paxos;
import javax.jws.Oneway;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here

    int sequence;
    int highestDoneSeq = -1;
    /*
    int n_p; //highest prepare num seen
    int n_a; //highest accept num seen
    Object v_a; //highest accept value seen
    Object myVal; //value started
    Object decidedVal;
    State state;
    */

    public class seqInstance{
        int sequence;
        int n_p; //highest prepare num seen
        int n_a; //highest accept num seen
        int n;   //highest proposal num seen
        Object v_a; //highest accept value seen
        Object myVal; //value started
        Object decidedVal;
        State state;
        ConcurrentHashMap<Object,Integer> valToN = new ConcurrentHashMap<>();

        public seqInstance(int sequence){
            this.sequence = sequence;
            n_p = 0;
            n_a = 0;
            n= 0;
            v_a = null;
            state = State.Pending;
        }
    }

    public seqInstance getInstance(int seq){ //lock and unlock??
        mutex.lock();
        if(!seqInstances.containsKey(seq)){
            seqInstance newInst = new seqInstance(seq);
            seqInstances.put(seq, newInst);
        }
        mutex.unlock();
        return seqInstances.get(seq);
    }

    ConcurrentHashMap<Integer, seqInstance> seqInstances = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<seqInstance> seqInstancesQueue = new ConcurrentLinkedQueue<>();
    ArrayList<Integer> doneList = new ArrayList<>();    //ConcurrentHashMap<Integer, Object> sequenceValMap = new ConcurrentHashMap<>(); //key = sequence, value = decided val for that sequence

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        for(int i = 0; i < peers.length; i++){
            this.doneList.add(i, -1);
        }
        //n_p = 0;
        //n_a = 0;
        //v_a = null;
        //state = State.Pending;

        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        // Your code here
        //
        //if(seq < Min()) return;
        mutex.lock();
        seqInstance inst = getInstance(seq);
        this.sequence = seq;
        //this.myVal = value;
        inst.myVal = value;

        seqInstancesQueue.add(inst);
        System.out.println("Put " + inst.sequence + " into Queue");
        mutex.unlock();

        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run(){
        //Your code here
        //Phase 1
        //(a) A proposer selects a proposal number n and sends a prepare
        //request with number n to a majority of acceptors.
        //(b) If an acceptor receives a prepare request with number n greater
        //than that of any prepare request to which it has already responded,
        //then it responds to the request with a promise not to accept any more
        //proposals numbered less than n and with the highest-numbered proposal (if any) that it has accepted.
        //Phase 2
        //(a) If the proposer receives a response to its prepare requests
        //(numbered n) from a majority of acceptors, then it sends an accept
        //request to each of those acceptors for a proposal numbered n with a
        //value v, where v is the value of the highest-numbered proposal among
        //the responses, or is any value if the responses reported no proposals.
        //(b) If an acceptor receives an accept request for a proposal numbered
        //n, it accepts the proposal unless it has already responded to a prepare
        //request having a number greater than n.

        //seqInstance inst = getInstance(this.sequence);
        seqInstance inst = seqInstancesQueue.remove();
        System.out.println("Sequence " + inst.sequence + " removed");
        int seq = inst.sequence;
        //if(seq < Min()) return;
        System.out.println(seq + " has state "  + inst.state);
        while(inst.state != State.Decided){
            //phase 1
            System.out.println("Sequence " + inst.sequence + " paxos started");
            mutex.lock();
            System.out.println("help");
            int n = findHighestN(inst);
            System.out.println("n = " + n + " seq = " + inst.sequence);
            Request prepareReq = new Request(n, inst.myVal, inst.sequence);
            mutex.unlock();
            Response prepareResp = sendPrepareToPeers(prepareReq);

            //phase 2
            if(prepareResp.prepareMajority == true){
                Request acceptReq = new Request(prepareResp.n, prepareResp.v, inst.sequence);
                Response acceptResp = sendAcceptToPeers(acceptReq);

                if(acceptResp.acceptMajority == true){

                        Request decideReq = new Request(acceptResp.n, acceptResp.v, inst.sequence);
                        Response decideResp = sendDecideToPeers(decideReq);


                }
            }
        }
    }

    public synchronized int findHighestN(seqInstance inst){
        //return (Math.max(inst.n_a, inst.n_p)*peers.length + 1)/peers.length + me;
        return inst.n_p*peers.length + me + 1;
    }


    public Response sendPrepareToPeers(Request prepareReq){
        System.out.println(me + " sending prepare with sequence " + prepareReq.sequence);

        int acceptCount = 0;
        int highestNA = prepareReq.proposalNum;
        Object tempVal = prepareReq.value;
        for(int i = 0; i < peers.length; i++){
            Response prepareResp;
            if(me != i){
                prepareResp = Call("Prepare",prepareReq,i);
            }
            else{
                prepareResp = Prepare(prepareReq);
            }

            if(prepareResp != null && prepareResp.prepareOK == true){
                acceptCount++;
                if(prepareResp.n_a > highestNA){
                    highestNA = prepareResp.n_a;
                    tempVal = prepareResp.v_a;
                }
            }
        }

        Response resp = new Response();
        if(acceptCount >= peers.length / 2 + 1){
            resp.prepareMajority = true;
            resp.n = prepareReq.proposalNum;
            resp.v = tempVal;
        }
        return resp;
    }

    // RMI handler
    public Response Prepare(Request req){
        // your code here
        mutex.lock();
        seqInstance inst = getInstance(req.sequence);
        Response prepareResp = new Response();


        if(req.proposalNum > inst.n_p){
            inst.n_p = req.proposalNum;
            prepareResp.prepareOK = true;
        }

        prepareResp.n = req.proposalNum;
        prepareResp.n_a = inst.n_a;
        prepareResp.v_a = inst.v_a;

        mutex.unlock();

        return prepareResp;
    }


    public Response sendAcceptToPeers(Request acceptReq){
        System.out.println(me + " sending accept with sequence " + acceptReq.sequence);
        int acceptCount = 0;
        for(int i = 0; i < peers.length;i++){
            Response acceptResp;
            if(me != i){
                acceptResp = Call("Accept",acceptReq,i);
            }
            else{
                acceptResp = Accept(acceptReq);
            }

            if(acceptResp != null && acceptResp.acceptOK == true){
                acceptCount++;
            }

        }

        Response resp = new Response();
        if(acceptCount >= peers.length / 2 + 1){
            resp.acceptMajority = true;
            resp.n = acceptReq.proposalNum;
            resp.v = acceptReq.value;
        }

        return resp;

    }

    public Response Accept(Request req){
        // your code here
        //if(getInstance(req.sequence).state == State.Decided) return new Response();
        mutex.lock();
        seqInstance inst = getInstance(req.sequence);
        Response prepareResp = new Response();


        if(req.proposalNum >= inst.n_p){
            inst.n_p = req.proposalNum;
            inst.n_a = req.proposalNum;
            inst.v_a = req.value;
            prepareResp.acceptOK = true;
        }

        prepareResp.n = req.proposalNum;
        mutex.unlock();
        return prepareResp;
    }

    public Response sendDecideToPeers(Request decideReq){
        int minSeq = Integer.MAX_VALUE;
        if(getInstance(decideReq.sequence).state == State.Decided) return null;
        System.out.println(me  + " sending decide on value: " +decideReq.value);
        for(int i = 0; i < peers.length;i++){
            Response decideResp;
            if(me != i){
                decideResp = Call("Decide",decideReq,i);
                System.out.println(decideResp);
            }
            else{
                decideResp = Decide(decideReq);
                //System.out.println(decideResp);
            }
            mutex.lock();
            //System.out.println(doneList);
            //System.out.println(decideResp);
            if(decideResp != null) {
                if (this.doneList.get(decideResp.peerNum) < decideResp.maxSeq) {
                    this.doneList.set(decideResp.peerNum, decideResp.maxSeq);
                }
            }
            mutex.unlock();
            //if(decideResp.maxSeq < minSeq){
            //    minSeq = decideResp.maxSeq;
            //}
        }

        //Done(minSeq);

        Response resp = new Response();
        resp.decidedOK = true;
        resp.v = decideReq.value;
        return resp;

    }

    public Response Decide(Request req){
        // your code here
        if(getInstance(req.sequence).state == State.Decided) return null;

        mutex.lock();
        if(req.sequence > doneList.get(me)) {
            //doneList.set(me, req.sequence);
        }
        seqInstance inst = getInstance(req.sequence);

        inst.n_p = req.proposalNum;
        inst.n_a = req.proposalNum;
        inst.decidedVal = req.value;
        inst.state = State.Decided;

        System.out.println(me + " decided on value " + req.value + " just to check, inst.decidedVal " + inst.decidedVal);

        //this.sequenceValMap.put(req.sequence, req.value);

        Response resp = new Response();

        resp.maxSeq = this.highestDoneSeq;
        resp.peerNum = this.me;
        mutex.unlock();
        return resp;

    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        doneList.set(me,seq);
        mutex.lock();
        highestDoneSeq = this.doneList.get(me);
        for(int doneSeq : this.seqInstances.keySet()){
            if(doneSeq < seq){
                this.seqInstances.remove(doneSeq);
            }
        }
        mutex.unlock();
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        int maxSeq = Integer.MIN_VALUE;
        for(int seq : this.seqInstances.keySet()){
            if(seq > maxSeq){
                maxSeq = seq;
            }
        }

        if(maxSeq == Integer.MIN_VALUE){
            maxSeq = -1;
        }
        return maxSeq;
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        int minSeq = Integer.MAX_VALUE;
        for(int seq : doneList){
            if(seq < minSeq){
                minSeq = seq;
            }
        }
        if(minSeq == Integer.MAX_VALUE){
            minSeq = -2;
        }
        return minSeq + 1;

    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        mutex.lock();
        seqInstance inst = getInstance(seq);

        retStatus stat = new retStatus(inst.state, inst.decidedVal);
         if(seq < Min() - 1) {
            stat.state = State.Forgotten;
        }
        mutex.unlock();
        return stat;
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}
