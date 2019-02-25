package binarySearch;

/*
154. Find Minimum in Rotated Sorted Array
maybe duplicate exists in the array.
https://leetcode.com/problems/find-minimum-in-rotated-sorted-array/
 */

public class RotatedFindMin2 {

  // [2,2,2,2,0,1,2],  1
  public static int findMin(int[] nums) {
    if (nums == null || nums.length == 0) {
      return -1;
    }

    int left = 0;
    int right = nums.length - 1;

    while (left + 1 < right) {
      int mid = left + (right - left) / 2;
      if (nums[mid] < nums[right]) { // 后半段递增, 最小值在左边
        right = mid;
      } else if (nums[mid] > nums[right]) { // 前半段递增，最小值在右边
        left = mid + 1;
      } else { // 当mid与right值相等
        right--;
      }
    }
    return Math.min(nums[left], nums[right]);
  }

  public static void main(String[] args) {
//    int[] arr = {3,4,5,1,2};
    int[] arr = {1, 3, 3};
//    int[] arr = {4, 5, 6, 7, 0, 1, 2};
    System.out.println("res = " + findMin(arr));
  }
}
