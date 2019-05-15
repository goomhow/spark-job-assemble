package com.ctc.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

import java.io.IOException;

public class Client {
    private HTable table;
    private static Configuration conf = HBaseConfiguration.create();
    static {
        conf.set("hbase.zookeeper.quorum", "node11:2181,node12:2181,node13:2181");
        conf.set("hbase.client.scanner.timeout.period",String.valueOf(5*60*1000));
        conf.set("zookeeper.znode.parent","/hbase-unsecure");
    }

    public Client(String tableName) throws IOException {
        table = new HTable(conf,tableName);
    }

    public void scan(){
        Scan scan = new Scan();
        scan.addColumn("".getBytes(),"".getBytes());
        scan.setFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("\\d{11}")));
    }


}
