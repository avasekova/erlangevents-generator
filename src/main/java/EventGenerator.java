import com.ericsson.otp.erlang.*;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;

//TODO cleanup, doc
public class EventGenerator {

    private static ObjectMapper mapper = new ObjectMapper();

    //TODO casovanie udalosti (podla occurrenceTime), zatial to pustam vsetko rovno za sebou
    public static void main(String[] args) {
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/jel.log"))) {
            String line = br.readLine();

            JsonNode root;
            JsonNode event;
            if (line != null) {
                //prepare the connection
                OtpNode node = new OtpNode("java");
                OtpMbox mbox = node.createMbox("myMailbox");
                String defaultServerNodeName = "myserver@" + node.host();
                System.out.format("Server Node to contact [%s]> ", defaultServerNodeName);
                OtpErlangTuple serverPidTuple = new OtpErlangTuple(new OtpErlangObject[]{new OtpErlangAtom("server"), new OtpErlangAtom(defaultServerNodeName)});
                if (!node.ping(defaultServerNodeName, 1000)) {
                    System.out.println("Erlang node is not available: " + defaultServerNodeName);
                    return;
                }

                root = mapper.readTree(line);
                event = root.get("Event");

                //prepare the representation expected by Erlang and send
                OtpErlangList erlangStruct = convertObjectNode(event);
                sendMessage(mbox, "server", defaultServerNodeName, serverPidTuple, erlangStruct);

                while ((line = br.readLine()) != null) {
                    root = mapper.readTree(line);
                    event = root.get("Event");

                    //prepare the representation expected by Erlang and send
                    erlangStruct = convertObjectNode(event);
                    sendMessage(mbox, "server", defaultServerNodeName, serverPidTuple, erlangStruct);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void sendMessage(OtpMbox mbox, final String server, String defaultServerNodeName, OtpErlangTuple serverPidTuple, OtpErlangList erlangStruct) {
        mbox.send(server, defaultServerNodeName, new OtpErlangTuple(new OtpErlangObject[] {mbox.self(), erlangStruct}));
        OtpErlangObject serverReply = null;
        try {
            serverReply = mbox.receive(1000);
        } catch (OtpErlangExit | OtpErlangDecodeException e) {
            e.printStackTrace();
        }

        if (serverReply == null) {
            System.out.println("WARN: Timeout when receiving reply");
        } else {
            System.out.format("%s replied : %s%n", serverPidTuple, serverReply);
        }

    }

    private static OtpErlangList convertObjectNode(JsonNode json) {
        List<OtpErlangTuple> tuples = new ArrayList<>();
        Iterator<String> iterator = json.fieldNames();
        String fieldName;
        JsonNode subNode;
        OtpErlangTuple tuple;
        OtpErlangObject[] tupleItems = new OtpErlangObject[2];
        Calendar calendar = new GregorianCalendar();
        while (iterator.hasNext()) {
            fieldName = iterator.next();
            tupleItems[0] = new OtpErlangString(fieldName);
            subNode = json.get(fieldName);
            if (subNode.isValueNode()) {
                if (fieldName.equals("occurrenceTime")) {
                    tupleItems[1] = new OtpErlangLong(calendar.getTimeInMillis()); //miesto occurrenceTime je novy cas v ms
                } else {
                    if (subNode.isNumber()) {
                        if ((subNode.numberType() == JsonParser.NumberType.DOUBLE) || (subNode.numberType() == JsonParser.NumberType.FLOAT)) {
                            tupleItems[1] = new OtpErlangDouble(subNode.asDouble());
                        } else {
                            tupleItems[1] = new OtpErlangLong(subNode.asLong());
                        }
                    } else {
                        if (subNode.asText().equals("true") || subNode.asText().equals("false")) {
                            tupleItems[1] = new OtpErlangAtom(subNode.asText());
                        } else {
                            tupleItems[1] = new OtpErlangString(subNode.asText());
                        }
                    }
                }
            } else {
                if (subNode.isArray()) {
                    //TODO zatial vyplut cely ten zoznam do uvodzoviek ako String, ale potom nejak vymysliet
                    tupleItems[1] = new OtpErlangString(subNode.toString());
                } else {
                    if (subNode.isObject()) {
                        tupleItems[1] = convertObjectNode(subNode);
                    }
                }
            }
            tuple = new OtpErlangTuple(tupleItems);
            tuples.add(tuple);
        }

        OtpErlangTuple[] allTuples = new OtpErlangTuple[1];

        return new OtpErlangList(tuples.toArray(allTuples));
    }
}