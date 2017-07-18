package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sushmitasinha on 4/10/17.
 */

public class MessagePojo implements Serializable {
    public String key;
    public String value;
    public int predecessor;
    public int successor;
    public String msgType;
    public ConcurrentHashMap<String, String> msgMap = null;

    //constructor
    public  MessagePojo( String key, String value, int predecessor, int successor,
                         ConcurrentHashMap<String, String> msgMap,String msgType){
        this.key = key;
        this.predecessor = predecessor;
        this.successor = successor;
        this.value = value;
        this.msgType = msgType;
        this.msgMap = msgMap;
    }

    public  MessagePojo(){
        super();
    }

}
