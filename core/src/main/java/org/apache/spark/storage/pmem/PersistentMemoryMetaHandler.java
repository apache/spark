package org.apache.spark.storage.pmem;
 
import java.nio.channels.FileLock;
import java.sql.Connection;
import org.sqlite.SQLiteConfig;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.*;

public class PersistentMemoryMetaHandler {

  private static String url = "jdbc:sqlite:/tmp/spark_shuffle_meta.db";
  private static String fileLockPath = "/tmp/spark_shuffle_file.lock";

  private static final File file = new File(fileLockPath);

  PersistentMemoryMetaHandler(String root_dir) {
    createTable(root_dir);
  }

  public void createTable(String root_dir) {
    String sql = "CREATE TABLE IF NOT EXISTS metastore (\n"
                + "	shuffleId text PRIMARY KEY,\n"
                + "	device text NOT NULL,\n"
                + " UNIQUE(shuffleId, device)\n"
                + ");\n";

    url = "jdbc:sqlite:" + root_dir + "/spark_shuffle_meta.db";
    synchronized (file) {
      FileOutputStream fos = null;
      try {
        fos = new FileOutputStream(file);
        FileLock fileLock = fos.getChannel().lock();
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute(sql);

        sql = "CREATE TABLE IF NOT EXISTS devices (\n"
                + "	device text UNIQUE,\n"
                + "	mount_count int\n"
                + ");";
        stmt.execute(sql);
        conn.close();
        fos.close();
      } catch (SQLException e) {
        System.out.println("createTable failed:" + e.getMessage());
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (fos != null) {
          try {
            fos.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
    System.out.println("Metastore DB connected: " + url);
  }

  public void insertRecord(String shuffleId, String device) {
    String sql = "INSERT OR IGNORE INTO metastore(shuffleId,device) VALUES('" + shuffleId + "','" + device + "')";
    synchronized (file) {
      FileOutputStream fos = null;
      try {
        fos = new FileOutputStream(file);
        FileLock fileLock = fos.getChannel().lock();
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout("30000");
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
        conn.close();
        fos.close();
      } catch (SQLException e) {
        System.err.println("insertRecord failed:" + e.getMessage());
        System.exit(-1);
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (fos != null) {
          try {
            fos.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  public String getShuffleDevice(String shuffleId){
    String sql = "SELECT device FROM metastore where shuffleId = ?";
    String res = "";
    synchronized (file) {
      FileOutputStream fos = null;
      try {
        fos = new FileOutputStream(file);
        FileLock fileLock = fos.getChannel().lock();
        Connection conn = DriverManager.getConnection(url);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, shuffleId);
        ResultSet rs = pstmt.executeQuery();
        if (rs != null) {
          res = rs.getString("device");
        }
        conn.close();
        fos.close();
      } catch (SQLException e) {
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (fos != null) {
          try {
            fos.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
    return res;
  }

  public String getUnusedDevice(ArrayList<String> full_device_list){
    String sql = "SELECT device, mount_count FROM devices";
    ArrayList<String> device_list = new ArrayList<String>();
    HashMap<String, Integer> device_count = new HashMap<String, Integer>();
    String device = "";
    int count;
    synchronized (file) {
      FileOutputStream fos = null;
      try {
        fos = new FileOutputStream(file);
        FileLock fileLock = fos.getChannel().lock();
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout("30000");
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
          device_list.add(rs.getString("device"));
          device_count.put(rs.getString("device"), rs.getInt("mount_count"));
        }

        full_device_list.removeAll(device_list);
        if (full_device_list.size() == 0) {
          // reuse old device, picked the device has smallest mount_count
          device = getDeviceWithMinCount(device_count);
          if (device == "") {
            throw new SQLException();
          }
          count = (Integer) device_count.get(device) + 1;
          sql = "UPDATE devices SET mount_count = " + count + " WHERE device = '" + device + "'\n";
        } else {
          device = full_device_list.get(0);
          count = 1;
          sql = "INSERT OR IGNORE INTO devices(device, mount_count) VALUES('" + device + "', " + count + ")\n";
        }

        System.out.println(sql);

        stmt.executeUpdate(sql);
        conn.close();
        fos.close();
      } catch (SQLException e) {
        System.err.println("getUnusedDevice insert device " + device + "failed: " + e.getMessage());
        System.exit(-1);
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    System.out.println("Metastore DB: get unused device, should be " + device + ".");
    return device;
  }

  public void remove() {
    new File(url).delete();
  }

  private String getDeviceWithMinCount(HashMap<String, Integer> device_count_map) {
    String device = "";
    int count = -1;
    for (Map.Entry<String, Integer> entry : device_count_map.entrySet()) {
      if (count == -1 || entry.getValue() < count) {
        device = entry.getKey();
        count = entry.getValue();
      }
    }
    return device;
  }

  public static void main(String[] args) {
    PersistentMemoryMetaHandler pmMetaHandler = new PersistentMemoryMetaHandler("/tmp/");
    String dev;
    /*System.out.println("insert record");
    pmMetaHandler.insertRecord("shuffle_0_1_0", "/dev/dax0.0");
    pmMetaHandler.insertRecord("shuffle_0_2_0", "/dev/dax0.0");
    pmMetaHandler.insertRecord("shuffle_0_3_0", "/dev/dax1.0");
    pmMetaHandler.insertRecord("shuffle_0_4_0", "/dev/dax1.0");
    */
    /*System.out.println("get shuffle device");
    String dev = pmMetaHandler.getShuffleDevice("shuffle_0_85_0");
    System.out.println("shuffle_0_85_0 uses device: " + dev);
    */
    ArrayList<String> device_list = new ArrayList<String>();
    device_list.add("/dev/dax0.0");
    device_list.add("/dev/dax1.0");
    device_list.add("/dev/dax2.0");
    device_list.add("/dev/dax3.0");

    dev = pmMetaHandler.getUnusedDevice(device_list);
    dev = pmMetaHandler.getUnusedDevice(device_list);
    dev = pmMetaHandler.getUnusedDevice(device_list);
    dev = pmMetaHandler.getUnusedDevice(device_list);

    dev = pmMetaHandler.getUnusedDevice(device_list);
    dev = pmMetaHandler.getUnusedDevice(device_list);
    dev = pmMetaHandler.getUnusedDevice(device_list);
    dev = pmMetaHandler.getUnusedDevice(device_list);
  }

}
