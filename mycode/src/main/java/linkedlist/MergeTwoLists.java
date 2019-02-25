package linkedlist;

/*
  21, https://leetcode.com/problems/merge-two-sorted-lists/
  Input: 1->2->4, 1->3->4
  Output: 1->1->2->3->4->4
  // 从小到大
 */
public class MergeTwoLists {

  public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
    ListNode dummy = new ListNode(-1);
    ListNode current = dummy;
    while (l1 != null && l2 != null) {
      if (l1.val > l2.val) {
        current.next = l2;
        l2 = l2.next;
      } else {
        current.next = l1;
        l1 = l1.next;
      }
      current = current.next;
    }

    if (l1 != null) {
      current.next = l1;
    }

    if (l2 != null) {
      current.next = l2;
    }
    return dummy.next;
  }
}

