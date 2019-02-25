package binarySearch;

/*
33. Search in Rotated Sorted Array
 no duplicate exists in the array.
https://leetcode.com/problems/search-in-rotated-sorted-array/
 */
public class RotatedSearch {

  // Input: nums = [4,5,6,7,0,1,2], target = 0
  // Output index: 4
  public static int search(int[] nums, int target) {
    int left = 0;
    int right = nums.length - 1;
    while (left <= left) {
      int mid = left + (right - left) / 2;
      if (nums[mid] == target) {
        return mid;
      }

      if (nums[left] <= nums[mid]){ // 左边有序
        if (target < nums[mid] && target >= nums[left]) {
          right = mid -1;
        } else {
          left = mid +1;
        }
      } else { // mid两边的数组，必然有一个子数组存在有序，因此有 nums[mid] <= nums[right]
        if (target > nums[mid] && target <= nums[right]) {
          left = mid +1;
        } else {
          right = mid -1;
        }
      }
    }
    return -1;
  }

  public static void main(String[] args) {
//    int[] arr = {4, 5, 6, 7, 0, 1, 2};
//    int[] arr = {5, 1, 3};
    int[] arr = {4, 5, 6, 7, 8, 1, 2, 3};
    System.out.println("res = " + search(arr, 1));
  }
}
