package linkedlist;

// 206. Reverse Linked List
public class ReverseList {

  // 递归：r(1->2->3->4->5->NULL) = (r(2->3->4->5->NULL) = 5->4->3->2) +
//  public static ListNode reverseList(ListNode head) {
//    if (head == null || head.next == null) {
//      return head;
//    }
//
//    ListNode newHead = reverseList(head.next);
//    head.next.next = head;
//    head.next = null;
//
//    return newHead;
//  }

  // 非递归，1->2->3->4->5->NULL
  // Detail: 2->1, 3->2, 4->3, 5->4,
  // => 5->4->3->2->1->NULL
  public static ListNode reverseList(ListNode head) {
    if (head == null || head.next == null) {
      return head;
    }
    ListNode cut = head;
    ListNode next = head.next;
    ListNode nextNext = null;
    while (next != null) {
      nextNext = next.next;
      next.next = cut;
      cut = next;
      next = nextNext;
    }
    // 注意此处head的next需要设置为null。
    head.next = null;
    return cut;
  }

  public static void main(String[] args) {
    ListNode listNode1 = new ListNode(1);
    ListNode listNode2 = new ListNode(2);
    ListNode listNode3 = new ListNode(3);
    ListNode listNode4 = new ListNode(4);
    ListNode listNode5 = new ListNode(5);
    listNode1.next = listNode2;
    listNode2.next = listNode3;
    listNode3.next = listNode4;
    listNode4.next = listNode5;
    listNode5.next = null;

    // 1->2->3->4->5->NULL => 5->4->3->2->1->NULL
    ListNode res = reverseList(listNode1);
    while (res != null) {
      System.out.println(res.val);
      res = res.next;
    }
  }
}
