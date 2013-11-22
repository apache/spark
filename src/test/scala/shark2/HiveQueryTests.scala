package catalyst
package shark2

class HiveQueryTests extends HiveComaparisionTest {
  createQueryTest("Simple Average",
    "SELECT AVG(key) FROM src")
}