package org.mlamp.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.sql.*;
import java.util.*;

public class HiveUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HiveUtil.class);

    private static String auth;
    private static String principal;
    private static String base_url;
    private static String username;
    private static String password;
    private static String keytab;
    private static String krb5;
    private static String hiveUrl;

    public static void init(String paramsMap) {
        try {
            Properties prop = new Properties();
            prop.load(new FileReader(paramsMap));
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            LOG.info("Hive Driver classLoader: " + Driver.class.getClassLoader());
            base_url = prop.getOrDefault("hive.url", "").toString();
            username = prop.getOrDefault("kerberos.user.principal", "").toString();
            password = prop.getOrDefault("kerberos.user.password", "").toString();
            keytab = prop.getOrDefault("kerberos.user.keytab", "").toString();
            krb5 = prop.getOrDefault("kerberos.user.conf", "").toString();

            System.setProperty("java.security.krb5.conf", krb5);
            LOG.info("\nhive.url: " + base_url +
                    "; \nkerberos.user.principal: " + username +
                    "; \nkerberos.user.password: " + password +
                    "; \nkerberos.user.keytab: " + keytab);
            StringBuilder strBuilder = new StringBuilder(base_url);

            strBuilder
                    .append(";auth=KERBEROS")
//                        .append(auth)
                    .append(";principal=hive/hadoop.hadoop.com@HADOOP.COM")
//                        .append(principal)
                    .append(";user.principal=")
                    .append(username)
                    .append(";user.keytab=")
                    .append(keytab)
                    .append(";");
//            if ("KERBEROS".equalsIgnoreCase(auth)) {
//            }
            hiveUrl = strBuilder.toString();
            checkProps();
        } catch (Exception e) {
            LOG.error("初始化hiveUtil失败", e);
            throw new RuntimeException(e);
        }
    }

    private static void checkProps() {
        Preconditions.checkArgument(StringUtils.isNotEmpty(hiveUrl), "error, hive.url is null");
        Preconditions.checkArgument(StringUtils.isNotEmpty(username), "error, kerberos.user.principal is null");
        Preconditions.checkArgument(StringUtils.isNotEmpty(keytab), "error, kerberos.user.keytab is null");
        Preconditions.checkArgument(StringUtils.isNotEmpty(krb5), "error, kerberos.user.conf is null");
    }

    public static Connection getConnection() throws SQLException {
        try {
            return DriverManager.getConnection(hiveUrl, "", "");
//            if ("KERBEROS".equalsIgnoreCase(auth)) {
//            } else {
//                return DriverManager.getConnection(hiveUrl, username, password);
//            }
        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
            throw t;
        }
    }

    public static boolean existDb(String hiveDB) {
        //第一个参数为库名，第二个为用户，第三个为表名，第四个为表类型
        try (Connection conn = getConnection(); ResultSet dbs = conn.getMetaData().getTables(hiveDB, null, null, null)) {
            return dbs.next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean existTable(String hiveDB,String hiveTable) {
        //第一个参数为库名，第二个为用户，第三个为表名，第四个为表类型
        try (Connection conn = getConnection(); ResultSet tables = conn.getMetaData().getTables(hiveDB, null, hiveTable, null)) {
            return tables.next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> descTable(String hiveDB, String hiveTable) {
        Map<String, String> name2TypeMap = new HashMap<>();
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("use " + hiveDB);
            try (final ResultSet resultSet = statement.executeQuery("describe " + hiveTable)) {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                LOG.info("hive-columnCount:"+metaData.getColumnCount());
                for (int i = 1; i < metaData.getColumnCount(); i++) {
                    LOG.info("hive-columnName:"+metaData.getColumnName(i));
                }
                while (resultSet.next()) {
                    name2TypeMap.put(resultSet.getString(1), resultSet.getString(2));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return name2TypeMap;
    }

    /**
     * 获取表字段列表
     * @param hiveTable
     * @return
     */
    public static List<String> getTableFields(String hiveTable) {
        List<String> fields = new ArrayList<>();
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery("describe " + hiveTable)) {
                while (resultSet.next()) {
                    fields.add(resultSet.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return fields;
    }
}

