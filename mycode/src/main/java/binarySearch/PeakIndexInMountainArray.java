package binarySearch;

/*
852. Peak Index in a Mountain Array
https://leetcode.com/problems/peak-index-in-a-mountain-array/
注意这题不是rotate array，无其性质
 */
public class PeakIndexInMountainArray {

  public static int peakIndexInMountainArray(int[] nums) {
    int left = 0;
    int right = nums.length - 1;

    while (left < right) { // 此处不能left <= right
      int mid = left + (right - left) / 2;
      if (nums[mid] < nums[mid + 1])
        left = mid + 1;
      else
        right = mid;
    }
    return left;
  }

  public static void main(String[] args) {
//    int[] arr = {4, 5, 6, 7, 0, 1, 2};
//    int[] arr = {0,1,0};
//    int[] arr = {0,2,1,0};
    int[] arr = {24, 69, 100, 99, 79, 78, 67, 36, 26, 19};
    System.out.println("res = " + peakIndexInMountainArray(arr));
  }
}
