package dp;

import java.util.*;

// 898. Bitwise ORs of Subarrays
public class SubarrayBitwiseORs {
  // 1. dp[i][j], 表示从下标i开始到j的or运算结果
  // dp[i][j] = dp[i][j-1] | arr[j]
  // 上述dp二维数组可以滚动数组优化
//  public static int subarrayBitwiseORs(int[] arr) {
//    HashSet<Integer> res = new HashSet<Integer>();
//    int last = 0;
//    for (int i = 0; i < arr.length; i++) {
//      //  i=0 , j=0,1...n-1
//      //  i=1 , j=1,2...n-1
//      //  i=2 , j=2,3...n-1
//      //  i=3 , j=3,4...n-1
//      for (int j = i; j < arr.length; j++) {
//        if (i == j) {
//          last = arr[i];
//        } else {
//          last = last | arr[j];
//        }
//        res.add(last);
//      }
//    }
//    return res.size();
//  }


  // dp[i], 表示结尾为下标元素的Subarrays的or运算结果的集合
  public static int subarrayBitwiseORs(int[] arr) {
    HashSet<Integer> res = new HashSet<Integer>();
    Set<Integer> last = new HashSet<Integer>();
    for (int i = 0; i < arr.length; i++) {
      Set<Integer> cut = new HashSet<Integer>();
      cut.add(arr[i]);
      res.add(arr[i]);
      for (Integer ele : last) {
        cut.add(ele | arr[i]);
        res.add(ele | arr[i]);
      }
      last = cut;
    }
    return res.size();
  }

  public static void main(String[] args) {
    System.out.println(" = " + subarrayBitwiseORs(new int[]{0}));
    System.out.println(" = " + subarrayBitwiseORs(new int[]{1, 1, 2}));
    System.out.println(" = " + subarrayBitwiseORs(new int[]{1, 2, 4}));
  }
}
