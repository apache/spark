package others;

public class BinaryTransfer {

  public static char[] chars = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

  // 11, 2 => (5|1,2|1,0|1), 111
  // 12, 2 => (6|0,3|0,1|1,0|1), 1100
  // 13, 2 => (6|1,3|0,1|1,0|1),1111
  public static String binaryChange(int value, int binary) {
    String res = new String();
    while (value > 0) {
      res = chars[value % binary] + res;
      value = value / binary;
    }
    return res;
  }

  public static void main(String[] args) {
    System.out.println("binaryChange(11, 2) = " +
        binaryChange(11, 2));
    System.out.println("binaryChange(66, 8) = " +
        binaryChange(66, 8));
  }
}



