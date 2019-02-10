package sort;

import java.util.Arrays;

// siftDown: prefer to heap building or sorting
// siftUp: prefer to insert
public class HeapSort {

  // 小 => 大， 非稳定算法
  public static void heapSort(int[] arr) {
    int len = arr.length;
    for (int i = len / 2; i > 0; i--) { // 对所有非叶子节点进行下浮操作
      siftDown(arr, i, len - 1);
    }
  }

  public static void siftDown(int[] arr, int index, int maxIndex) {
    int tmp = arr[index]; // 下浮的元素
    int child;

    for (; index * 2 <= maxIndex; index = child) {
      child = 2 * index;
      if (child != maxIndex && arr[child + 1] < arr[child]) { // 获取较小的元素
        child = child + 1;
      }
      if (arr[child] < tmp) {
        arr[index] = arr[child];
      } else {
        break;
      }
    }
    arr[index] = tmp;
  }

  public static void main(String[] args) {
    // 数组下标从1 => n-1
    int[] arr = new int[]{Integer.MIN_VALUE, 1, 1, 8, 2, 1, 4, 6, 7,
        3, 5, 9, 6, 11, 19, 13, 55, 67, 32, 22};
    heapSort(arr); // 最小堆

    // 输出从大到小
    for (int i = 1; i < arr.length; i++) {
      int tmp = arr[1];
      arr[1] = arr[arr.length - i];
      arr[arr.length - i] = tmp;
      siftDown(arr, 1, arr.length - i - 1);
    }

    System.out.println(Arrays.toString(arr));
  }
}
