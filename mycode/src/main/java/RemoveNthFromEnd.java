public class RemoveNthFromEnd {
  //  1, and n =1 => null;
  //  1,2, and n =1 => 1|2,  1 -> null;
  //  1,2, and n =2 => 1|null,  1 -> null;
  //  1->2->3->4->5, and n = 2. =>  1->2->3->5.
  //  12,23,34,45;13,24,35,
  //  1->2->3->4->5, and n = 3. =>  1->2->4->5.
  //  13,24,35;14,25
  //  快慢指针，快指针夺走n步, 注意需要考虑删除的是第一个节点
  public ListNode removeNthFromEnd(ListNode head, int n) {
    ListNode slow = head;
    ListNode fast = head;
    int step = 0;
    while (step++ < n) {
      fast = fast.next;
    }
    if (fast == null) {
      return slow.next;
    }
    while (fast != null && fast.next != null) {
      slow = slow.next;
      fast = fast.next;
    }
    slow.next = slow.next.next;
    return head;
  }


}

class ListNode {

  int val;
  ListNode next;

  ListNode(int x) {
    val = x;
  }
}
