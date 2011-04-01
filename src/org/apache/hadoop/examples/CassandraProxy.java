package org.apache.hadoop.examples;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassandraProxy {

    public static List<String> MONTH_LIST = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug",
                                                          "Sep", "Oct", "Nov", "Dec");
    static Cassandra.Client    cassandraClient;
    static TTransport          socket;

    public CassandraProxy(String keySpace) {
        try {
            init(keySpace);
        } catch (InvalidRequestException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void init(String keySpace) throws InvalidRequestException, TException {
        String server = "10.249.201.13";
        // String server = "localhost";
        int port = 9160;

        /* 首先指定cassandra server的地址 */
        socket = new TSocket(server, port);
        System.out.println(" connected to " + server + ":" + port + ".");

        TFramedTransport transport = new TFramedTransport(socket);

        /* 指定通信协议为二进制流协议 */
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport);
        cassandraClient = new Cassandra.Client(binaryProtocol);

        /* 建立通信连接 */
        socket.open();
        cassandraClient.set_keyspace(keySpace);
        System.out.println("cassandra server ok");
    }

    public void addLog(String tableName, String key, String sendTime, String value) throws ParseException {
        /* 数据所在的行标 */
        String row = key;

        /* 创建一个column path */
        ColumnParent parent = new ColumnParent(tableName);

        Date sendDate = buildSendDate(sendTime);

        // super column
        SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd");
        String superColumnName = formater.format(sendDate);
        parent.setSuper_column(superColumnName.getBytes());
        // column
        Column col = new Column();
        formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String columnName = formater.format(sendDate);
        col.setName(columnName.getBytes());
        col.setValue(value.getBytes());

        /*
         * 执行插入操作，指定keysapce, row, col, 和数据内容， 后面两个参数一个是 timestamp， 另外一个是consistency_level timestamp是用来做数据一致性保证的，
         * 而consistency_level是用来控制数据分布的策略，前者的理论依据是bigtable, 后者的理论依据是dynamo
         */
        try {
            cassandraClient.insert(Translater.toByteBuffer(row), parent, col, ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnavailableException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TimedOutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static Date buildSendDate(String sendTime) {

        StringBuffer sb = new StringBuffer();
        String[] args = StringUtils.split(sendTime);
        sb.append(args[4]).append("-").append(buildMonth(args[1])).append("-").append(args[2]).append(" ").append(args[3]);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        try {
            return formatter.parse(sb.toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String buildMonth(String src) {
        return String.valueOf(MONTH_LIST.indexOf(src) + 1);

    }

    public static void main(String[] args) {
        String src = "Sun Mar 13 23:25:33.085 2011";
        System.out.println(buildSendDate(src));
    }
}
