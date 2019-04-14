package com.company;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/**
 * JDBCでOracleDBにアクセスしてテーブルカラムのメタ情報を取得するサンプルプログラム
 */
public class Main {

    public static void main(String[] args) {
        Main m = new Main();
        m.execute();
    }

    private void execute() {
        Connection conn = null;

        try {
            Properties prop = getJdbcProperties();

            // ドライバロード
            Class.forName(prop.getProperty("jdbc.driver"));

            // DBコネクション取得
            String dbuser = prop.getProperty("jdbc.user");
            String dbpassword = prop.getProperty("jdbc.password");
            conn = DriverManager.getConnection(prop.getProperty("jdbc.manager"), dbuser, dbpassword);
            conn.setAutoCommit(false);

            // テーブルのメタ情報を取得する
            DatabaseMetaData dbmeta = conn.getMetaData();
            showTableMetaInfoToConsole(dbmeta, "META_GET_TEST");

            // トランザクションを終了する
            conn.commit();

        } catch (IOException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * JDBC接続プロパティを取得する.
     *
     * @return Properties 取得したJDBCプロパティ
     * @throws IOException プロパティ取得に失敗した
     */
    private Properties getJdbcProperties() throws IOException {
        Properties prop = new Properties();

        try (InputStream in = Main.class.getResourceAsStream("/jdbc.properties")) {
            prop.load(in);

            System.out.println(prop);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

        return prop;
    }

    /**
     * dbuserにあるtablenameのメタ情報をコンソールに出力する
     *
     * @param dbmeta    メタ情報
     * @param tablename 　テーブル名
     */
    private void showTableMetaInfoToConsole(DatabaseMetaData dbmeta, String tablename) throws SQLException {
        try (ResultSet rs = dbmeta.getColumns(null, null, tablename, null)) {
            if (rs != null) {
                System.out.println("COLUMN_NAME\tDATA_TYPE\tTYPE_NAME\tCOLUMN_SIZE");
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    String type = rs.getString("TYPE_NAME");
                    String columnSize = rs.getString("COLUMN_SIZE");

                    System.out.println(name + "\t" + dataType + "\t" + type + "\t" + columnSize);
                }
            }
        }
    }
}
