package linkedlist;

public class AddTwoNumbers {

  /*
  Input: (2 -> 4 -> 3) + (5 -> 6 -> 4)
  Output: 7 -> 0 -> 8
   Explanation: 342 + 465 = 807.
   */
  public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
    ListNode sentinel = new ListNode(0);
    ListNode d = sentinel;

    int sum = 0;
    while (l1 != null || l2 != null) {
      if (l1 != null) {
        sum += l1.val;
        l1 = l1.next;
      }
      if (l2 != null) {
        sum += l2.val;
        l2 = l2.next;
      }
      d.next = new ListNode(sum % 10);
      d = d.next;
      sum = sum / 10;
    }
    if (sum != 0 ) {
      d.next = new ListNode(1);
    }
    return sentinel.next;
  }

  public static void main(String[] args) {

  }
}
