package sort;

import java.util.Arrays;

public class QuickSort {

  // 小 => 大， 非稳定算法
  public static int partition(int[] arr, int start, int end) {
    int tmp = arr[start];
    while (start < end) {
      while (start < end && arr[end] >= tmp) end--;
      arr[start] = arr[end];
      while (start < end && arr[start] <= tmp) start++;
      arr[end] = arr[start];
    }
    arr[start] = tmp;
    return start;
  }

  public static void quickSort(int[] arr, int start, int end) {
    if (start < end) {
      int partition = partition(arr, start, end);
      quickSort(arr, start, partition - 1);
      quickSort(arr, partition + 1, end);
    }
  }

  public static void main(String[] args) {
    int[] arr = {8, 2, 1, 1, 4, 6, 7, 3, 5, 9, 6, 11, 19, 13, 55, 67, 32, 22};
    quickSort(arr, 0, arr.length - 1);
    System.out.println(Arrays.toString(arr));
  }
}
