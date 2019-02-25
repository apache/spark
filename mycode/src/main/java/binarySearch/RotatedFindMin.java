package binarySearch;

/*
153. Find Minimum in Rotated Sorted Array
https://leetcode.com/problems/find-minimum-in-rotated-sorted-array/
 */

// [->Max,Min->]
public class RotatedFindMin {

  public static int findMin(int[] nums) {
    if (nums == null || nums.length == 0) {
      return -1;
    }

    int left = 0;
    int right = nums.length - 1;

    while (left + 1 < right) {
      int mid = left + (right - left) / 2;
      if (nums[mid] < nums[right]) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    return Math.min(nums[left], nums[right]);
  }

  public static void main(String[] args) {
//    int[] arr = {3,4,5,1,2};
    int[] arr = {1, 2, 3};
//    int[] arr = {4, 5, 6, 7, 0, 1, 2};
    System.out.println("res = " + findMin(arr));
  }
}
