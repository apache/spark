package others;

import java.util.Arrays;

// 942. DI String Match
public class DiStringMatch {

  // IDID => 0,4,1,3,2
  // III => 0,1,2,3
  public static int[] diStringMatch(String S) {
    int high = S.length();
    int low = 0;
    int[] res = new int[S.length() + 1];
    char[] chars = S.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      char ch = chars[i];
      if (ch == 'I') {
        res[i] = low++;
        if (i == chars.length - 1) {
          res[i + 1] = low;
        }
      } else {
        res[i] = high--;
        if (i == chars.length - 1) {
          res[i + 1] = high;
        }
      }
    }
    return res;
  }

  public static void main(String[] args) {
    System.out.println(" = " + Arrays.toString(diStringMatch("IDID")));
    System.out.println(" = " + Arrays.toString(diStringMatch("III")));
    System.out.println(" = " + Arrays.toString(diStringMatch("DDI")));
  }
}
