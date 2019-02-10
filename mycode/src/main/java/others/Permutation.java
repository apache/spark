package others;

// 46. Permutations
public class Permutation {
  // 全排列，从不重复的数组中挑选m个数
  public static void permutation(int[] arr, int[] out, int begin, int m, int len) {
    if (m == 0) {
      for (int i = 0; i < out.length; i++) {
        System.out.print(out[i]);
      }
      System.out.println();
      return;
    }
    if (begin + m > arr.length) {
      return;
    }
    permutation(arr, out, begin + 1, m, len); // 不选
    out[len - m] = arr[begin];
    permutation(arr, out, begin + 1, m - 1, len); // 选
  }

  public static void main(String[] args) {
    int arr[] = new int[]{1, 2, 3, 4, 5};
    int m = 3;
    int[] out = new int[3];
    permutation(arr, out, 0, m, m);
  }
}
