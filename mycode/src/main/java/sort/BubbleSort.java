package sort;

import java.util.Arrays;

public class BubbleSort {
  // 小 => 大， 稳定算法
  public static void bubbleSort(int[] arr) {
    for (int i = arr.length - 1; i >= 0; i--) {
      boolean flag = false; // 当本轮没有交换发生，提前终止排序过程
      for (int j = 0; j < i; j++) {
        if (arr[j] > arr[j + 1]) {
          flag = true;
          swap(arr, j, j + 1);
        }
      }
      if (!flag) {
        break;
      }
    }
  }

  public static void swap(int[] arr, int i, int j) {
    int tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }

  public static void main(String[] args) {
    int[] arr = {8, 2, 1, 1, 4, 6, 7, 3, 5, 9, 6, 11, 19, 13, 55, 67, 32, 22};
    bubbleSort(arr);
    System.out.println(Arrays.toString(arr));
  }
}
