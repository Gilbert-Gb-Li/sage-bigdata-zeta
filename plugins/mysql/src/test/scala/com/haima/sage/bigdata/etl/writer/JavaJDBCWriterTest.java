package com.haima.sage.bigdata.etl.writer;

import org.junit.Test;

import java.sql.*;
import java.util.Random;

/**
 * Created by zhhuiyan on 2017/4/5.
 * Email: weiping_he@sage.com
 */
public class JavaJDBCWriterTest {
    private Random random = new Random();

    /*
    * @Test
    * FIXME add mysql test
    * */
    public void mysql() throws Exception {
        String JDBC_URL = "jdbc:mysql://localhost:3306/test";
        String JDBC_USER = "root";
        String JDBC_PASS = "root";
        Connection conn = DriverManager.getConnection(JDBC_URL + "?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=utf8", JDBC_USER, JDBC_PASS);
        conn.setAutoCommit(false);
//        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test.om_my(AudAudSts,AudEntUsr,AudEntDat,AudBrnNbr,AudBbkNbr,AudEntNbr,AudPicID,AudTskNbr,AudEvtNbr,AudPrdNbr,AudCcyNbr,AudTrxAmt,AudLgrDat,AudEacNbr,AudOwnCIt,AudCItTyp,AudPsbCod,AudBilNbr,AudTskEnd,AudMntUsr,AudMntDat,AudMntTim,AudTskCod,AudWkeCod,AudRtnCod,AudRecVer,AudPicFlg,AudSpc079,CreatorId,CreateTime,LastModifyId,LastModifyTime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test.om_auaudpicp(AudAudSts,AudEntUsr,AudEntDat,AudBrnNbr,AudBbkNbr,AudEntNbr,AudPicID,AudTskNbr,AudEvtNbr,AudPrdNbr,AudCcyNbr,AudTrxAmt,AudLgrDat,AudEacNbr,AudOwnCIt,AudCItTyp,AudPsbCod,AudBilNbr,AudTskEnd,AudMntUsr,AudMntDat,AudMntTim,AudTskCod,AudWkeCod,AudRtnCod,AudRecVer,AudPicFlg,AudSpc079,CreatorId,CreateTime,LastModifyId,LastModifyTime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

        Integer BATCH_SIZE = 1000;
        Integer COMMIT_SIZE = 10000;
//        Integer COUNT = 10000000;
        Integer COUNT = 1000000;
        for (int i = 1; i <= COUNT; i += BATCH_SIZE) {
            pstmt.clearBatch();
            for (int j = 0; j < BATCH_SIZE; j++) {
                long timeStamp = getTimeStamp();
                pstmt.setString(1, "S");
                pstmt.setString(2, "000000");
                pstmt.setDate(3, new Date(timeStamp));
                pstmt.setString(4, "000000");
                pstmt.setString(5, "000");
                pstmt.setString(6, "1510000");
                pstmt.setString(7, "99901703313300000003A0");
                pstmt.setString(8, "" + (1000000000000L + i));
                pstmt.setString(9, "00000000000");
                pstmt.setString(10, "0000000000");
                pstmt.setString(11, "00");
                pstmt.setDouble(12, 1.00);
                pstmt.setDate(13, new Date(timeStamp));
                pstmt.setString(14, "000000");
                pstmt.setString(15, "0000000000");
                pstmt.setString(16, "客户类型");
                pstmt.setString(17, "0000");
                pstmt.setString(18, "");
                pstmt.setString(19, "000");
                pstmt.setString(20, "000000");
                pstmt.setDate(21, new Date(timeStamp));
                pstmt.setTime(22, new Time(timeStamp));
                pstmt.setString(23, "00000000");
                pstmt.setString(24, "00000000");
                pstmt.setString(25, "0000000");
                pstmt.setInt(26, 0);
                pstmt.setString(27, "Y");
                pstmt.setString(28, "");
                pstmt.setString(29, "SYS001");
                pstmt.setTimestamp(30, new Timestamp(timeStamp));
                pstmt.setString(31, "SYS001");
                pstmt.setTimestamp(32, new Timestamp(timeStamp));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if ((i + BATCH_SIZE - 1) % COMMIT_SIZE == 0) {
                System.out.println(i + "=====");
                conn.commit();
            }
        }
        conn.commit();
        conn.close();
    }

    private long getTimeStamp() {
        long difference = random.nextInt(60 * 60 * 24 * 30 * 12) * 1000;
        return System.currentTimeMillis() - difference;
    }

}
