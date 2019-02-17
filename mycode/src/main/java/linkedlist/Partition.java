package linkedlist;

// 86
// https://leetcode.com/problems/partition-list/submissions/
public class Partition {

  /*
  Input: head = 1->4->3->2->5->2, x = 3
  Output: 1->2->2->4->3->5
   */
  // 重建链表
  public static ListNode partition(ListNode head, int x) {
    ListNode fl = new ListNode(-1);
    ListNode fr = new ListNode(-1);

    ListNode left = fl;
    ListNode right = fr;
    while (head != null) {
      if (head.val < x) {
        left.next = head;
        left = head;
      } else {
        right.next = head;
        right = head;
      }
      head = head.next;
    }
    left.next = fr.next;
    right.next = null;
    return fl.next;
  }

  public static void main(String[] args) {
    ListNode listNode1 = new ListNode(1);
    ListNode listNode2 = new ListNode(4);
    ListNode listNode3 = new ListNode(3);
    ListNode listNode4 = new ListNode(2);
    ListNode listNode5 = new ListNode(5);
    ListNode listNode6 = new ListNode(2);
    listNode1.next = listNode2;
    listNode2.next = listNode3;
    listNode3.next = listNode4;
    listNode4.next = listNode5;
    listNode5.next = listNode6;
    listNode6.next = null;

    // 1->2->3->4->5->NULL => 5->4->3->2->1->NULL
    ListNode res = partition(listNode1, 3);
    while (res != null) {
      System.out.println(res.val);
      res = res.next;
    }
  }
}
