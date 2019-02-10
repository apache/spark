package sort;

import java.util.Arrays;

public class InsertSort {

  // 小 => 大，稳定算法
  // 6,11,10 => 6,11,10 => 6,10,11
  public static void insertSort(int[] arr) {
    for (int i = 1; i < arr.length; i++) {
      int ele = arr[i];
      int j = i - 1;

      for (; j >= 0 && arr[j] > ele; j--) {// 直到prefix数组中有个元素小于ele
        arr[j + 1] = arr[j];
      }

      arr[j + 1] = ele;
    }
  }

  public static void main(String[] args) {
    int[] arr = {8, 2, 1, 1, 4, 6, 7, 3, 5, 9, 6, 11, 19, 13, 55, 67, 32, 22};
    insertSort(arr);
    System.out.println(Arrays.toString(arr));
  }
}
