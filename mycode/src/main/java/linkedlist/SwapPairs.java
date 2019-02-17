package linkedlist;


// 24. Swap Nodes in Pairs
// https://leetcode.com/problems/swap-nodes-in-pairs
public class SwapPairs {

  public static ListNode swapPairs(ListNode head) {
    if (head == null) {
      return head;
    }
    ListNode res = head;
    if (res.next != null) {
      res = res.next;
    }

    // 1->2->3->4->5  => 2->1->4->3->5，奇数
    // 1->2->3->4  => 2->1->4->3， 偶数
    ListNode prev = head;
    ListNode current = head;
    while (current != null && current.next != null) {
      prev.next = current.next; // 用于两两之间的连接
      ListNode tmp = current.next.next; // 3, 5
      current.next.next = current;  // 2-> 1, 4->3
      current.next = tmp; // 1->3, 3->5

      prev = current;
      current = current.next;
    }

    return res;
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
    listNode4.next = null;

    ListNode res = swapPairs(listNode1);
    while (res != null) {
      System.out.println(res.val);
      res = res.next;
    }
  }
}

