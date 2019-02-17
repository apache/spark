package linkedlist;

public class HasCycle {
  /*
   141，https://leetcode.com/problems/linked-list-cycle/
 */
  public static boolean hasCycle(ListNode head) {
    if (head == null || head.next == null) {
      return false;
    }

    ListNode slow = head;
    ListNode fast = head;
    while (fast != null && fast.next != null) {
      slow = slow.next;
      fast = fast.next.next; // 走2步
      if (slow == fast) {
        return true;
      }
    }
    return false;
  }

  /*
    142, https://leetcode.com/problems/linked-list-cycle-ii/
    Given a linked list, return the node where the cycle begins.
    If there is no cycle, return null.
   */
  public static ListNode detectCycle(ListNode head) {
    if (head == null || head.next == null) {
      return null;
    }

    ListNode slow = head;
    ListNode fast = head;
    boolean hasCycle = false;
    while (fast != null && fast.next != null) {
      slow = slow.next;
      fast = fast.next.next; // 走2步
      if (slow == fast) {
        hasCycle = true;
        break;
      }
    }

    if (!hasCycle) {
      return null;
    } else {
      slow = head;
      while (slow != fast) {
        slow = slow.next;
        fast = fast.next;
      }
      return slow;
    }
  }
}
