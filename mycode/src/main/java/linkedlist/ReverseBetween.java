package linkedlist;

// 92. Reverse Linked List II
public class ReverseBetween {
  // reverse from m to n [1<=m<=n<=len(list)]
  // Input: 1->2->3->4->5->NULL, m = 2, n = 4
  // 1->2->3->4->5->NULL
  // => 1->3->2->4->5->NULL
  // => 1->4->3->2->5->NULL
  // Output: 1->4->3->2->5->NULL
  public static ListNode reverseBetween(ListNode head, int m, int n) {
    if (head == null) {
      return head;
    }

    // fake出一个节点否则需要考虑m=1与m!=1情况
    ListNode fake = new ListNode(-1);
    fake.next = head;

    ListNode prev = fake;
    int j = m;
    while (--j > 0) {
      prev = prev.next;
    }

    /*
         f 1 [2 3 4] 5 => f 1 3 2 4 5
           p  c t
     */
    ListNode cut = prev.next;
    for (int i = 0; i < n - m; i++) {
      ListNode tmp = cut.next; // 3
      cut.next = tmp.next; // 2->4
      tmp.next = prev.next;  // 3->2
      prev.next= tmp; // 1->3
    }
    return fake.next;
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

    // Input: 1->2->3->4->5->NULL, m = 2, n = 4
    // Output: 1->4->3->2->5->NULL
    ListNode res = reverseBetween(listNode1, 2, 4);
    while (res != null) {
      System.out.println(res.val);
      res = res.next;
    }
  }
}
