package others;

// 3. Longest Substring Without Repeating Characters
public class LengthOfLongestSubstring {

  // 不含有重复字符的最长子串
  public static int lengthOfLongestSubstring(String s) {
    if (null == s || s.length() == 0) {
      return 0;
    }
    int start = 0;
    int lastMaxLen = 1;
    int currentLen = 1;
    for (int end = 1; end < s.length(); end++) {
      String cut = s.substring(end, end + 1);
      String prefix = s.substring(start, end);
      if (prefix.contains(cut)) {
        start = s.indexOf(cut, start)+1;
      }
      currentLen = end - start + 1;
      if (currentLen > lastMaxLen) {
        lastMaxLen = currentLen;
      }
    }
    return lastMaxLen;
  }


  public static void main(String[] args) {
    System.out.println(" = " + lengthOfLongestSubstring(" "));
    System.out.println(" = " + lengthOfLongestSubstring("au"));
    System.out.println(" = " + lengthOfLongestSubstring("dvdf"));
    System.out.println(" = " + lengthOfLongestSubstring("bdavdfgn"));
    System.out.println(" = " + lengthOfLongestSubstring("abcabcbb"));
    System.out.println(" = " + lengthOfLongestSubstring("bbbbb"));
    System.out.println(" = " + lengthOfLongestSubstring("pwwkew"));
  }
}
