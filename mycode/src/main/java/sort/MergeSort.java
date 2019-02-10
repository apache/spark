package sort;

import java.util.Arrays;

public class MergeSort {

  // 小 => 大， 稳定算法
  private static void mergeSort(int[] arr, int start, int end) {
    if (start < end) {
      int mid = (start + end) / 2;
      mergeSort(arr, start, mid);
      mergeSort(arr, mid + 1, end);
      merge(arr, start, mid, end);
    }
  }

  private static void merge(int[] arr, int start, int mid, int end) {
    int r1 = mid + 1;
    int[] tmp = new int[arr.length];
    int tempIndex = start;
    int arrIndex = start;

    while (start <= mid && r1 <= end) {
      if (arr[start] <= arr[r1]) {
        tmp[tempIndex++] = arr[start++];
      } else {
        tmp[tempIndex++] = arr[r1++];
      }
    }

    while (start <= mid) { // arr2还有剩余元素
      tmp[tempIndex++] = arr[start++];
    }

    while (r1 <= end) { // arr1还有剩余元素
      tmp[tempIndex++] = arr[r1++];
    }
    for (int i = arrIndex; i <= end; i++) {
      arr[i] = tmp[i];
    }
    return;
  }

  public static void main(String[] args) {
    int[] arr = {8, 2, 1, 1, 4, 6, 7, 3, 5, 9, 6, 11, 19, 13, 55, 67, 32, 22};
    mergeSort(arr, 0, arr.length - 1);
    System.out.println(Arrays.toString(arr));
  }
}
