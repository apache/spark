package linkedlist;

// 61. Rotate List
// https://leetcode.com/problems/rotate-list/
public class RotateRight {

  /*
    Input: 0->1->2->NULL, k = 4
    Output: 2->0->1->NULL
    Explanation:
    rotate 1 steps to the right: 2->0->1->NULL
    rotate 2 steps to the right: 1->2->0->NULL
    rotate 3 steps to the right: 0->1->2->NULL
    rotate 4 steps to the right: 2->0->1->NULL

    Input: 1->2->3->4->5->NULL, k = 2
    rotate 1 steps to the right: 5->1->2->3->4->NULL
    rotate 2 steps to the right: 4->5->1->2->3->NULL
    Output: 4->5->1->2->3->NULL
   */
  public static ListNode rotateRight(ListNode head, int k) {
    if (head == null || head.next == null) {
      return head;
    }

    int len = 1;
    ListNode end = head;
    while (end.next != null) {
      end = end.next;
      len++;
    }
    end.next = head;

    //  0->1->2->NULL, k = 4
    for (int i = 1; i < len - k % len; i++) {
      head = head.next;
    }
    ListNode res = head.next;
    head.next = null;
    return res;
  }
}
