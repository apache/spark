package binarySearch;

/*
162. Find Peak Element
https://leetcode.com/problems/find-peak-element/
You may imagine that nums[-1] = nums[n] = -∞.

A peak element is an element that is greater than its neighbors.
The array may contain multiple peaks,
in that case return the index to any one of the peaks is fine.
Your solution should be in logarithmic complexity.
 */

public class FindPeakElement {

  // 考虑边界条件，
  public static int findPeakElement(int[] nums) {
    int left = 0;
    int right = nums.length - 1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      if ((mid == nums.length - 1 || nums[mid] > nums[mid + 1]) &&
          (mid == 0 || nums[mid] > nums[mid - 1])) {
        return mid;
      } else if (mid > 0 && nums[mid - 1] > nums[mid]) {
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }
    return -1;
  }

  // nums = [1,2,3,1], 2
  // [1,2,1,3,5,6,4], 2|5
  public static void main(String[] args) {
    int[] arr = {1,2,1,3,5,6,4};
//    int[] arr = {1,2,3,1};
    System.out.println("res = " + findPeakElement(arr));
  }
}
