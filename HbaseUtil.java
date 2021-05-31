package com.hnumi.hbase.util;

import com.hnumi.common.util.CommonUtil;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;


public class HbaseUtil {
    private static Connection conn;
    private static Properties props;
    private static Logger log = LoggerFactory.getLogger(HbaseUtil.class);

    static {
        props = new Properties();
        try {
            props.load(new InputStreamReader(Objects.requireNonNull(HbaseUtil.class.getClassLoader().getResourceAsStream("hbase.properties")), "UTF-8"));
        } catch (IOException e) {
            log.error("配置文件读取异常", e);
        }
        Configuration conf = HBaseConfiguration.create();
        String value = props.getProperty("hbase.zookeeper.quorum");
        if (StringUtils.isNotBlank(value)) {
            conf.set("hbase.zookeeper.quorum", value.trim());
        }
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error("获取HBase连接异常", e);
        }
    }

    public static Connection getConnection() {
        return conn;
    }

    public static Connection getConnection(String quorum) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error("获取HBase连接异常", e);
        }
        return null;
    }

    public static void setConnection(Connection connection) {
        conn = connection;
    }

    public static Admin getAdmin() {
        if (check()) {
            try {
                return conn.getAdmin();
            } catch (IOException e) {
                log.error("获取Admin对象异常", e);
            }
        }
        return null;
    }

    public static Table getTable(String tableName) {
        if (check()) {
            try {
                return conn.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                log.error("获取Table对象异常", e);
            }
        }
        return null;
    }

    public static boolean createTable(String tableName,String family,int minVersion,int maxVersion){
        List<String> families = new LinkedList();
        families.add(family);
        return createTable(tableName, families, minVersion, maxVersion);
    }
    public static boolean createTable(String tableName,String family,int version){
        return createTable(tableName, family, version,version);
    }
    public static boolean createTable(String tableName,String family){
        return createTable(tableName, family, 1,1);
    }
    public static boolean createTable(String tableName, int minVersion, int maxVersion, String... family){
        List<String> families = Arrays.asList(family);
        return createTable(tableName, families, minVersion,maxVersion);
    }
    public static boolean createTable(String tableName, int version, String... family){
        return createTable(tableName,version,version,family);
    }
    public static boolean createTable(String tableName,String... family){
        return createTable(tableName,1,1,family);
    }

    public static boolean createTable(String tableName, List<String> families, int minVersion, int maxVersion) {
        if(families== null || families.isEmpty()){
            log.error("列族数据为空，创建表失败");
            return false;
        }
        Admin admin = getAdmin();
        boolean status = false;
        try {
            checkVersion(minVersion, maxVersion);
            if (admin != null) {
                List<ColumnFamilyDescriptor> familyDescriptors = CommonUtil.convert(families, family -> ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(family))
                        .setMinVersions(minVersion)
                        .setMinVersions(maxVersion)
                        .build());
                TableDescriptor dec = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamilies(familyDescriptors).build();

                admin.createTable(dec);
                status = true;

            }
        } catch (IOException e) {
            log.error("创建表操作异常", e);
        } catch (Exception e) {
            log.error("版本数参数异常", e);
        } finally {
            try {
                if(admin != null){
                    admin.close();
                }
            } catch (IOException e) {
                log.error("关闭Admin对象异常", e);
            }
        }
        return status;

    }

    public static boolean createTable(String tableName, List<String> families, int version) {
        return createTable(tableName, families, 1, version);
    }
    public static boolean createTable(String tableName, List<String> families) {
        return createTable(tableName, families, 1);
    }

    public static boolean putByRowKey(String tableName, String rowKey, String family, Map<String, Object> map) {
        if(map == null || map.isEmpty()){
            log.warn("数据为空，未添加任何数据");
            return false;
        }
        Table table = getTable(tableName);
        boolean result = false;
        if(table != null){
            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                //map的key是列名，value是列值
                map.forEach((key,value)->{
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value==null?"null":value.toString()));
                });
                table.put(put);
                result = true;
            } catch (IOException e) {
                log.error("插入数据异常",e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("关闭表对象异常",e);
                }
            }
        }
        return result;
    }

    public static boolean putByRowKey(String tableName, String rowKey, String family,String column,Object value){
        Table table = getTable(tableName);
        boolean result = false;
        if(table != null){
            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value==null?"null":value.toString()));
                table.put(put);
                result = true;
            } catch (IOException e) {
                log.error("插入数据异常",e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("关闭表对象异常",e);
                }
            }
        }
        return result;
    }

    public static <T> boolean putByRowKey(String tableName, String rowKey, String family, T bean){
        return putByRowKey(tableName,rowKey,family,CommonUtil.convert(bean));
    }
    public static <T> boolean put(String tableName, String family, T bean){
        String rowKey;
        try {
            rowKey = CommonUtil.getFieldValue(bean, "id");
            if(rowKey == null){
                log.error("对象的id字段数据为空，添加数据失败");
                return false;
            }
        } catch (NoSuchFieldException e) {
            log.error("对象必须有id字段，但是接收到的对象不具备id字段",e);
            return false;
        } catch (IllegalAccessException e) {
            log.error("字段 id 不具备访问权限",e);
            return false;
        }
        return putByRowKey(tableName,rowKey,family,CommonUtil.convert(bean));
    }

    public static <T> boolean puts(String tableName,String family,List<T> beans){
        if(beans == null || beans.isEmpty()){
            log.warn("数据为空，未添加任何数据");
            return false;
        }
        Table table = getTable(tableName);
        boolean result = false;
        if(table != null){
            try {
                List<Put> puts = new ArrayList<>();
                for (T bean : beans) {
                    puts.add(convertToPut(bean, family));
                }
                table.put(puts);
                result = true;
            } catch (IOException e) {
                log.error("插入数据异常",e);
            } catch (NoSuchFieldException e) {
                log.error("对象必须有id字段，但是接收到的对象不具备id字段",e);
            } catch (IllegalAccessException e) {
                log.error("字段 id 不具备访问权限",e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("关闭表对象异常",e);
                }
            }
        }
        return result;
    }
    /**
     *删除数据
     */
    public static boolean deleByRowKey(String tableName,String rowkey){
        Table table = getTable(tableName);
        Delete delete =new Delete(Bytes.toBytes(rowkey));
        try {
            table.delete(delete);
            log.warn("删除成功");
            return true;
        } catch (IOException e) {
            log.warn("删除失败");
            return false;
        }
    }
    public static boolean deleByRowKey(String tableName,String rowkey,String Family,String column){
        Table table = getTable(tableName);
        Delete delete =new Delete(Bytes.toBytes(rowkey));
        delete.addColumn(Bytes.toBytes(Family),Bytes.toBytes(column));
        try {
            table.delete(delete);
            log.warn("删除成功");
            return true;
        } catch (IOException e) {
            log.warn("删除失败");
            return false;
        }
    }
    public static boolean deleByRowKey(String tableName,String rowkey,String Family){
        Table table = getTable(tableName);
        Delete delete =new Delete(Bytes.toBytes(rowkey));
        delete.addFamily(Bytes.toBytes(Family));
        try {
            table.delete(delete);
            log.warn("删除成功");
            return true;
        } catch (IOException e) {
            log.warn("删除失败");
            return false;
        }
    }
    public static <T> T getBeanByRowKey(String tableName,String rowKey,String family,Class<T> beanType){
        Table table = getTable(tableName);
        T t = null;
        if(table != null ){
            try {
                Result result = table.get(new Get(Bytes.toBytes(rowKey)));
                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));
                Constructor<T> constructor = beanType.getConstructor();
                t = constructor.newInstance();
                for(Map.Entry<byte[], byte[]> entry : familyMap.entrySet()){
                    try{
                        Field field = beanType.getDeclaredField(Bytes.toString(entry.getKey()));
                        field.setAccessible(true);
                        field.set(t, ConvertUtils.convert(Bytes.toString(entry.getValue()), field.getType()));
                    }catch (NoSuchFieldException | IllegalAccessException e) {
                        log.warn("{} 不是 {} 的成员",Bytes.toString(entry.getKey()),beanType);
                    }
                }
            } catch (IOException e) {
                log.error("根据 RowKey 获取数据失败",e);
            } catch (IllegalAccessException e) {
                log.error("实例化Bean对象异常，BeanType没有公共的无参构造器",e);
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                log.error("BeanType必须具备无参构造器, 而当前对象不具备无参构造器",e);
            } catch (InstantiationException e) {
                log.error("实例化Bean对象异常",e);
            }
        }
        return t;
    }
    /**
     *关闭连接
     */
    public static void close() {
        try {
            if (null != conn)
            {conn.close();}
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    /**
     * 判断表是否存在
     * 默认读取配置文件中的表名并进行判断
     */
    public static boolean isExistTable(Configuration conf,String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return true;
        } else {
            return false;
        }
    }
    /**
      * 列出所有表的名称
      */
    public static void listTables(){
        Admin admin = getAdmin();
        try {
            TableName[] ts = admin.listTableNames();
            for (TableName t: ts) {
                System.out.println(t);
            }
        } catch (IOException e) {
            log.error("获取表名异常");
        }
    }


    private static boolean check() {
        if (conn == null) {
            log.error("创建HBase连接失败，请重新配置HBase的Zookeeper地址");
            return false;
        }
        return true;
    }

    private static void checkVersion(int minVersion, int maxVersion) throws IllegalArgumentException {
        if (minVersion > maxVersion) {
            throw new IllegalArgumentException(maxVersion + " 必须大于等于 " + minVersion);
        }
        if (minVersion < 1) {
            throw new IllegalArgumentException(minVersion + " 必须大于等于 1");
        }

    }

    private static Map<String, String> convert(Map<byte[], byte[]> map) {
        Map<String, String> result = new HashMap<>();
        if (map != null) {
            map.forEach((byte[] k, byte[] v) -> result.put(Bytes.toString(k), Bytes.toString(v)));
        }
        return result;
    }

    private static <T> Put convertToPut(T bean,String family) throws NoSuchFieldException, IllegalAccessException {
        Put put = new Put(Bytes.toBytes(CommonUtil.getFieldValue(bean,"id")));
        //map的key是列名，value是列值
        CommonUtil.convert(bean).forEach((key,value)-> put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value==null?"null":value.toString())));
        return put;
    }
}
