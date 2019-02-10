package recursive;

// 687. Longest Univalue Path
public class LongestUnivaluePath {
  public int max = 0;

  public int longestUnivaluePath(TreeNode root) {
    if (root == null) {
      return 0;
    }
    univaluePath(root);
    return this.max;
  }

  // 基于假设返回路径最长肯定包含root
  public int univaluePath(TreeNode root) {
    if (root == null) {
      return 0;
    }
    int l = univaluePath(root.left);
    int r = univaluePath(root.right);
    int pl = 0;
    int pr = 0;
    if (root.left != null && root.val == root.left.val) {
      pl = l + 1;
    }
    if (root.right != null && root.val == root.right.val) {
      pr = r + 1;
    }

    // 最大值可以包含左右子节点
    this.max = Math.max(max, pl + pr);
    // 返回值是包含根节点的最长路径
    return Math.max(pl, pr);
  }
}

class TreeNode {
  int val;
  TreeNode left;
  TreeNode right;

  TreeNode(int x) {
    val = x;
  }
}
