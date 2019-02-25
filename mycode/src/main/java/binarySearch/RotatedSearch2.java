package binarySearch;

/*
  81. Search in Rotated Sorted Array II
  maybe duplicate exists in the array.
  https://leetcode.com/problems/search-in-rotated-sorted-array-ii
*/

public class RotatedSearch2 {
  // 特许的case 1，1，1，3，1
  public static boolean search(int[] nums, int target) {
    int left = 0;
    int right = nums.length - 1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      if (nums[mid] == target) {
        return true;
      }
      // 处理特殊的case
      if (nums[left] == nums[mid] && nums[right] == nums[mid]) {
        left++;
        right--;
        continue;
      }

      if (nums[left] <= nums[mid]) { // 左边有序
        if (nums[left] <= target && target < nums[mid]) { // 左边中间
          right = mid - 1;
        } else {
          left = mid + 1;
        }
      } else { // mid两边的数组，必然有一个子数组存在有序，因此有 nums[mid] <= nums[right]
        if (nums[mid] < target && target <= nums[right]) { // 右边中间
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
    }
    return false;
  }

  public static void main(String[] args) {
//    int[] arr = {4, 5, 6, 7, 0, 1, 2};
    int[] arr = {1, 1, 1, 3, 1};
//    int[] arr = {2,5,6,0,0,1,2};
//    System.out.println("res = " + search(arr, 0));
    System.out.println("res = " + search(arr, 3));
  }
}
