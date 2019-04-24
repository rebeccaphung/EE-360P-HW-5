package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    // your data here
    int n;
    int n_p;
    int n_a;
    Object v_a;
    Object v;
    boolean prepareMajority;
    boolean acceptMajority;
    boolean prepareOK;
    boolean acceptOK;
    boolean decidedOK;
    int maxSeq;
    int peerNum;


    // Your constructor and methods here
    public Response(){
        prepareMajority = false;
        prepareOK = false;
        acceptMajority = false;
        acceptOK = false;
        decidedOK = false;
    }
}
