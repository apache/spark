package linkedlist;


// 19, https://leetcode.com/problems/remove-nth-node-from-end-of-list/
public class RemoveNthFromEnd {


  /*
   Given linked list: 1->2->3->4->5, and n = 2.
   After removing the second node from the end,
   the linked list becomes 1->2->3->5.
   1->2->3->4->5
   l     h
         l     h
   */
  public static ListNode removeNthFromEnd(ListNode head, int n) {
    if (head == null) {
      return head;
    }

    ListNode low = head;
    ListNode high = head;
    while (n-- > 0) {
      high = high.next;
    }
    if(high == null) { // 表示删除最后一个节点
      return low.next;
    } else {
      while (high.next != null) {
        low = low.next;
        high = high.next;
      }
      low.next = low.next.next;
      return head;
    }
  }

  public static void main(String[] args) {

  }
}
