public class LongestPalindrome {

  // babade
  // edabab
  // 中心枚举方法, 注意回文数字的特征
  public static String longestPalindrome(String s) {

    if (s == null || s.length() == 0) {
      return s;
    }

    int maxLen = -1;
    String res = null;
    for (int i = 0; i < s.length(); i++) {
      String tmp1 = getlongest(s, i, i);
      if (tmp1.length() > maxLen) {
        maxLen = tmp1.length();
        res = tmp1;
      }
      String tmp2 = getlongest(s, i, i + 1);
      if (tmp2.length() > maxLen) {
        maxLen = tmp2.length();
        res = tmp2;
      }
    }
    return res;
  }


  public static String getlongest(String s, int left, int right) {
    while (left >= 0 && right <= s.length() - 1 && s.charAt(right) == s.charAt(left)) {
      left--;
      right++;
    }
    return s.substring(left + 1, right);
  }

  public static void main(String[] args) {
    System.out.println(" = " + longestPalindrome("babad"));
    System.out.println(" = " + longestPalindrome("cbbd"));
  }
}
