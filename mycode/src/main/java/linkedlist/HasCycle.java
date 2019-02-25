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
    设定慢速指针与快速指针相交点距离环起点的距离为k，环周长为n，指针起点到环起点的距离为m，
    则慢速指针走过的距离为a = m+k+xn ；快速指针走过的距离为2a = m+k+yn，
    两者做差可以得到a = (y-x)n，是环长度的整数倍，
    如果将快速指针重置到起点，且将快速指针的移动距离改为1，
    那么当快速指针移动到圆环起点时，慢速指针移动距离为a+m，因为a是圆环长度的整数倍，
    所以慢速指针的位置也是在圆环起点，这样两者的相遇点即为圆环起点。
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
