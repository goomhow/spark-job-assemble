package com.ctc.stream;


import org.apache.kafka.common.serialization.Deserializer;
import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;


import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class QQDeserializer implements Deserializer<MessageQQ> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public MessageQQ deserialize(String s, byte[] bytes) {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        InputStreamReader reader = new InputStreamReader(in);
        Gson gson = new Gson();
        MessageQQ msg = gson.fromJson(reader,MessageQQ.class);
        return msg;
    }

    @Override
    public void close() {

    }

    public static void main(String[] args) {
        QQDeserializer jsonDeserializer = new QQDeserializer();
        String data = "{'message': '收到', 'message_id': 620, 'message_type': 'private', 'self_id': 3363643694, 'time': 1555547803, 'user_id': 1194338090}";
        MessageQQ msg= jsonDeserializer.deserialize("",data.getBytes());
        System.out.println(msg.getMessage());
    }
}
