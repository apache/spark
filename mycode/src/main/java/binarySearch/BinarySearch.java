package binarySearch;

/*
704. Binary Search
https://leetcode.com/problems/binary-search/
 */
public class BinarySearch {

  // Input: nums = [-1,0,3,5,9,12], target = 9
  public static int search(int[] nums, int target) {
    int left = 0;
    int right = nums.length - 1;
    while (right >= left) { // 边界条件
      int mid = left + (right - left) / 2; // mid更靠近左边
      if (nums[mid] == target) {
        return mid;
      } else if (nums[mid] < target) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
    return -1;
  }

  public static void main(String[] args) {
    int[] arr = {-1, 0, 3, 5, 9, 12};
    System.out.println("res = " + search(arr, 9));
  }
}
