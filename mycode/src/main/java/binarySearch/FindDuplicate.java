package binarySearch;

/*
287. Find the Duplicate Number
https://leetcode.com/problems/find-the-duplicate-number/
Given an array nums containing n + 1 integers
where each integer is between 1 and n (inclusive)
You must use only constant, O(1) extra space.
Your runtime complexity should be less than O(n2).
 */
public class FindDuplicate {
  public static int findDuplicate(int[] nums) {
    int left = 0;
    int right = nums.length - 1;

    // 1,3,4,2,2
    while (left < right) {
      int mid = left + (right - left) / 2;
      int cnt = 0; // 表示len(nums <= mid)个数
      for (int num : nums) {
        if (num <= mid) cnt++;
      }
      if (cnt <= mid) { // 表示小于mid的元素不多，因此重复元素大于mid
        left = mid + 1;
      } else { // 表示小于mid的元素较多，因此重复元素小于或者等于mid
        right = mid;
      }
    }
    return left;
  }

  public static void main(String[] args) {
    int[] arr = {1,3,4,2,2};
    System.out.println("res = " + findDuplicate(arr));
  }
}
