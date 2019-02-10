package others;

import java.util.Arrays;

// 215. Kth Largest Element in an Array
public class FindKthLargest {

  public static int findKthLargest(int[] nums, int left, int right, int k) {
    int pivot = partition(nums, left, right);
    if (pivot + 1 == k) {
      return nums[pivot];
    } else if (pivot + 1 < k) {  // 在右边
      return findKthLargest(nums, pivot + 1, right, k);
    } else {  // 在左边
      return findKthLargest(nums, left, pivot - 1, k);
    }
  }

  public static int findKthLargest(int[] nums, int k) {
    return findKthLargest(nums, 0, nums.length - 1, k);
  }

  // 取最左边元素为pivot, 如4, 从大到小
  // [], 7, 2, 1, 5, 3
  // 5, 7, 2, 1, [], 3
  // 5, 7, [], 1, 2, 3
  public static int partition(int[] nums, int left, int right) {
    int pivotPoint = nums[left];
    while (left < right) {
      while (left < right && nums[right] <= pivotPoint) {
        right--;
      }
      nums[left] = nums[right];
      while (left < right && nums[left] >= pivotPoint) {
        left++;
      }
      nums[right] = nums[left];
    }
    nums[left] = pivotPoint;    //基准元素归位
    return left;
  }

  public static void quickSort(int[] nums, int left, int right) {
    if (left >= right) {
      return;
    }
    int index = partition(nums, left, right);
    quickSort(nums, left, index - 1);
    quickSort(nums, index + 1, right);
  }

  public static void main(String[] args) {
    System.out.println(" = " +
        findKthLargest(new int[]{3, 2, 1, 5, 6, 4}, 2));
    System.out.println(" = " +
        findKthLargest(new int[]{3, 2, 3, 1, 2, 4, 5, 5, 6}, 4));

    int[] nums = new int[]{-1, 3, 2, 1, 7, 5, 6, 4};
    quickSort(nums, 0, 7);
    System.out.println(" = " + Arrays.toString(nums));
  }
}
