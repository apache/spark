package linkedlist;

import java.util.Stack;

/*
  160. Intersection of Two Linked Lists
  https://leetcode.com/problems/intersection-of-two-linked-lists/

     A:          a1 → a2
                         ↘
                         c1 → c2 → c3
                         ↗
     B:     b1 → b2 → b3
     begin to intersect at node c1.
 */
public class GetIntersectionNode {

  public ListNode getIntersectionNode(ListNode headA, ListNode headB) {
    Stack<ListNode> s1 = new Stack<>();
    Stack<ListNode> s2 = new Stack<>();
    while (headA != null) {
      s1.push(headA);
      headA = headA.next;
    }
    while (headB != null) {
      s2.push(headB);
      headB = headB.next;
    }

    ListNode res = null;
    while (!s1.isEmpty() && !s2.isEmpty() && s1.peek() == s2.peek()) {
      s1.pop();
      res = s2.pop();
    }
    return res;
  }

  public ListNode getIntersectionNode2(ListNode headA, ListNode headB) {
    if (headA == null || headB == null) {
      return null;
    }

    ListNode p1 = headA;
    ListNode p2 = headB;
    while (p1 != p2) {
      p1 = (p1 == null) ? headB : p1.next;
      p2 = (p2 == null) ? headA : p2.next;
    }
    return p1;
  }
}
