package linkedlist;

/*
  148 https://leetcode.com/problems/sort-list/
  Sort a linked list in O(n log n) time using constant space complexity.
  Input: 4->2->1->3
  Output: 1->2->3->4
  Input: -1->5->3->4->0
  Output: -1->0->3->4->5
 */
public class SortList {

  public static ListNode sortList(ListNode head) {
    if (head == null || head.next == null) {
      // 这里注意判断head是否为单个节点
      return head;
    }
    ListNode middle = getMiddle(head);
    ListNode next = middle.next;
    middle.next = null; // 断开两个子集之间的联系
    System.out.println("head = " + head.val);
    System.out.println("middle = " + middle.val);
    return merge(sortList(head), sortList(next));
  }

  private static ListNode merge(ListNode l1, ListNode l2) {
    ListNode fake = new ListNode(0);
    ListNode cut = fake;

//    ListNode fake = new ListNode(0);
    while (l1 != null && l2 != null) {
      if (l1.val < l2.val) {
        cut.next = l1;
        l1 = l1.next;
      } else {
        cut.next = l2;
        l2 = l2.next;
      }
      cut = cut.next;
    }

    if (l1 != null) {
      cut.next = l1;
    }

    if (l2 != null) {
      cut.next = l2;
    }
    return fake.next;
  }

  private static ListNode getMiddle(ListNode head) {
    if (head == null || head.next == null) {
      return head;
    }

    ListNode slow = head;
    ListNode fast = head;
    while (fast.next != null && fast.next.next != null) {
      slow = slow.next;
      fast = fast.next.next;
    }
    return slow;
  }

  //  -1->5->3->4->0
  public static void main(String[] args){
    ListNode listNode1 = new ListNode(-1);
    ListNode listNode2 = new ListNode(5);
    ListNode listNode3 = new ListNode(3);
    ListNode listNode4 = new ListNode(4);
    ListNode listNode5 = new ListNode(0);
    listNode1.next = listNode2;
    listNode2.next = listNode3;
    listNode3.next = listNode4;
    listNode4.next = listNode5;
    listNode5.next = null;

    ListNode res = sortList(listNode1);
    while (res != null) {
      System.out.println(res.val);
      res = res.next;
    }
  }

}
