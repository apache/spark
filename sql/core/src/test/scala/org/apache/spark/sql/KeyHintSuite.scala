/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.TestSQLContext

private object KeyHintSuite {
  case class Customer(id: Int, name: String)
  case class Employee(id: Int, name: String)
  case class Order(id: Int, customerId: Int, employeeId: Option[Int])
  case class Manager(managerId: Int, subordinateId: Int)
  case class BestFriend(id: Int, friendId: Int)
  case class BannedCustomer(name: String)
}

class KeyHintSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  import KeyHintSuite._

  lazy val customer = sqlContext.sparkContext.parallelize(Seq(
    Customer(0, "alice"),
    Customer(1, "bob"),
    Customer(2, "alice"))).toDF()
    .uniqueKey("id")
  lazy val employee = sqlContext.sparkContext.parallelize(Seq(
    Employee(0, "charlie"),
    Employee(1, "dan"))).toDF()
    .uniqueKey("id")
  lazy val order = sqlContext.sparkContext.parallelize(Seq(
    Order(0, 0, Some(0)),
    Order(1, 1, None))).toDF()
    .foreignKey("customerId", customer, "id")
    .foreignKey("employeeId", employee, "id")
  lazy val manager = sqlContext.sparkContext.parallelize(Seq(
    Manager(0, 1))).toDF()
    .foreignKey("managerId", employee, "id")
    .foreignKey("subordinateId", employee, "id")
  lazy val bestFriend = {
    val tmp = sqlContext.sparkContext.parallelize(Seq(
      BestFriend(0, 1),
      BestFriend(1, 2),
      BestFriend(2, 0))).toDF()
      .uniqueKey("id")
    tmp.foreignKey("friendId", tmp, "id")
  }
  lazy val bannedCustomer = sqlContext.sparkContext.parallelize(Seq(
    BannedCustomer("alice"),
    BannedCustomer("eve"))).toDF()
    .uniqueKey("name")

  // Joins involving referential integrity (a foreign key referencing a unique key)
  lazy val orderInnerJoinView = order
    .join(customer, order("customerId") === customer("id"))
    .join(employee, order("employeeId") === employee("id"))

  lazy val orderLeftOuterJoinView = order
    .join(customer, order("customerId") === customer("id"), "left_outer")
    .join(employee, order("employeeId") === employee("id"), "left_outer")

  lazy val orderRightOuterJoinView = employee.join(
    customer.join(order, order("customerId") === customer("id"), "right_outer"),
    order("employeeId") === employee("id"), "right_outer")

  lazy val orderCustomerFullOuterJoinView = order
    .join(customer, order("customerId") === customer("id"), "full_outer")

  lazy val orderEmployeeFullOuterJoinView = order
    .join(employee, order("employeeId") === employee("id"), "full_outer")

  lazy val managerInnerJoinView = manager
    .join(employee.as("emp_manager"), manager("managerId") === $"emp_manager.id")
    .join(employee.as("emp_subordinate"), manager("subordinateId") === $"emp_subordinate.id")

  lazy val bestFriendInnerJoinView = bestFriend
    .join(bestFriend.as("bestFriend2"), bestFriend("friendId") === $"bestFriend2.id")

  // Joins involving only a unique key
  lazy val bannedCustomerInnerJoinView = customer
    .join(bannedCustomer, bannedCustomer("name") === customer("name"))

  lazy val bannedCustomerLeftOuterJoinView = customer
    .join(bannedCustomer, bannedCustomer("name") === customer("name"), "left_outer")

  lazy val bannedCustomerFullOuterJoinView = customer
    .join(bannedCustomer, bannedCustomer("name") === customer("name"), "full_outer")

  def checkJoinCount(df: DataFrame, joinCount: Int): Unit = {
    val joins = df.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }
    assert(joins.size == joinCount)
  }

  def checkJoinsEliminated(df: DataFrame): Unit = checkJoinCount(df, 0)

  test("no elimination") {
    val orderInnerJoin = orderInnerJoinView
      .select(order("id"), order("customerId"), customer("name"),
        order("employeeId"), employee("name"))
    checkAnswer(orderInnerJoin, Seq(
      Row(0, 0, "alice", 0, "charlie")))

    val orderLeftOuterJoin = orderLeftOuterJoinView
      .select(order("id"), order("customerId"), customer("name"),
        order("employeeId"), employee("name"))
    checkAnswer(orderLeftOuterJoin, Seq(
      Row(0, 0, "alice", 0, "charlie"),
      Row(1, 1, "bob", null, null)))

    val orderRightOuterJoin = orderRightOuterJoinView
      .select(order("id"), order("customerId"), customer("name"),
        order("employeeId"), employee("name"))
    checkAnswer(orderRightOuterJoin, Seq(
      Row(0, 0, "alice", 0, "charlie"),
      Row(1, 1, "bob", null, null)))

    val orderCustomerFullOuterJoin = orderCustomerFullOuterJoinView
      .select(order("id"), customer("id"), customer("name"))
    checkAnswer(orderCustomerFullOuterJoin, Seq(
      Row(0, 0, "alice"),
      Row(1, 1, "bob"),
      Row(null, 2, "alice")))

    val orderEmployeeFullOuterJoin = orderEmployeeFullOuterJoinView
      .select(order("id"), employee("id"), employee("name"))
    checkAnswer(orderEmployeeFullOuterJoin, Seq(
      Row(0, 0, "charlie"),
      Row(1, null, null),
      Row(null, 1, "dan")))

    val managerInnerJoin = managerInnerJoinView
      .select(manager("managerId"), $"emp_manager.name",
        manager("subordinateId"), $"emp_subordinate.name")
    checkAnswer(managerInnerJoin, Seq(
      Row(0, "charlie", 1, "dan")))

    val bestFriendInnerJoin = bestFriendInnerJoinView
      .select(bestFriend("id"), $"bestFriend2.id", $"bestFriend2.friendId")
    checkAnswer(bestFriendInnerJoin, Seq(
      Row(0, 1, 2),
      Row(1, 2, 0),
      Row(2, 0, 1)))

    val bannedCustomerInnerJoin = bannedCustomerInnerJoinView
      .select(customer("id"), bannedCustomer("name"))
    checkAnswer(bannedCustomerInnerJoin, Seq(
      Row(0, "alice"),
      Row(2, "alice")))

    val bannedCustomerLeftOuterJoin = bannedCustomerLeftOuterJoinView
      .select(customer("id"), bannedCustomer("name"))
    checkAnswer(bannedCustomerLeftOuterJoin, Seq(
      Row(0, "alice"),
      Row(1, null),
      Row(2, "alice")))

    val bannedCustomerFullOuterJoin = bannedCustomerFullOuterJoinView
      .select(customer("id"), bannedCustomer("name"))
    checkAnswer(bannedCustomerFullOuterJoin, Seq(
      Row(0, "alice"),
      Row(1, null),
      Row(2, "alice"),
      Row(null, "eve")))
  }

  test("can't create foreign key referencing non-unique column") {
    intercept[AnalysisException] {
      bannedCustomer.foreignKey("name", customer, "name")
    }
  }

  test("eliminate unique key left outer join") {
    val bannedCustomerJoinEliminated = bannedCustomerLeftOuterJoinView
      .select(customer("id"), customer("name"))
    checkAnswer(bannedCustomerJoinEliminated, customer)
    checkJoinsEliminated(bannedCustomerJoinEliminated)
  }

  test("do not eliminate unique key inner/full outer join") {
    val bannedCustomerInnerJoinNotEliminated = bannedCustomerInnerJoinView
      .select(customer("id"), customer("name"))
    checkAnswer(bannedCustomerInnerJoinNotEliminated, Seq(
      Row(0, "alice"),
      Row(2, "alice")))

    val bannedCustomerFullOuterJoinNotEliminated = bannedCustomerFullOuterJoinView
      .select(customer("id"), customer("name"))
    checkAnswer(bannedCustomerFullOuterJoinNotEliminated, Seq(
      Row(0, "alice"),
      Row(1, "bob"),
      Row(2, "alice"),
      Row(null, null)))
  }

  test("do not eliminate referential integrity inner join where foreign key is nullable") {
    val orderInnerJoin = orderInnerJoinView
      .select(order("id"), customer("id"), employee("id"))
    checkAnswer(orderInnerJoin, Seq(
      Row(0, 0, 0)))
    // Only the customer join should be eliminated
    checkJoinCount(orderInnerJoinView, 2)
    checkJoinCount(orderInnerJoin, 1)
  }

  test("eliminate referential integrity join") {
    val orderLeftOuterJoinEliminated = orderLeftOuterJoinView
      .select(order("id"), customer("id"), employee("id"))
    checkAnswer(orderLeftOuterJoinEliminated, Seq(
      Row(0, 0, 0),
      Row(1, 1, null)))
    checkJoinsEliminated(orderLeftOuterJoinEliminated)

    val orderRightOuterJoinEliminated = orderRightOuterJoinView
      .select(order("id"), customer("id"), employee("id"))
    checkAnswer(orderRightOuterJoinEliminated, Seq(
      Row(0, 0, 0),
      Row(1, 1, null)))
    checkJoinsEliminated(orderRightOuterJoinEliminated)
  }

  test("do not eliminate referential integrity full outer join") {
    val orderCustomerFullOuterJoinNotEliminated = orderCustomerFullOuterJoinView
      .select(order("id"), order("customerId"), customer("id"))
    checkAnswer(orderCustomerFullOuterJoinNotEliminated, Seq(
      Row(0, 0, 0),
      Row(1, 1, 1),
      Row(null, null, 2)))

    val orderEmployeeFullOuterJoinNotEliminated = orderEmployeeFullOuterJoinView
      .select(order("id"), order("employeeId"), employee("id"))
    checkAnswer(orderEmployeeFullOuterJoinNotEliminated, Seq(
      Row(0, 0, 0),
      Row(1, null, null),
      Row(null, null, 1)))
  }

  test("eliminate referential integrity join despite multiple foreign keys with same referent") {
    val managerInnerJoinEliminated = managerInnerJoinView
      .select($"emp_manager.id", $"emp_subordinate.id")
    checkAnswer(managerInnerJoinEliminated, manager)
    checkJoinsEliminated(managerInnerJoinEliminated)
  }

  test("eliminate referential integrity self-join") {
    val bestFriendInnerJoinEliminated = bestFriendInnerJoinView
      .select(bestFriend("id"), $"bestFriend2.id")
    checkAnswer(bestFriendInnerJoinEliminated, Seq(
      Row(0, 1),
      Row(1, 2),
      Row(2, 0)))
    checkJoinsEliminated(bestFriendInnerJoinEliminated)
  }
}
