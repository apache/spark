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

package org.apache.spark.sql.execution.datasources.v2

import java.util.OptionalLong

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{BranchAlreadyExistsException, BranchNotFoundException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.SupportsBranching
import org.apache.spark.unsafe.types.UTF8String

/**
 * Physical plan node for ALTER TABLE ... CREATE BRANCH.
 */
case class CreateBranchExec(
    table: SupportsBranching,
    branchName: String,
    sourceSnapshotId: Option[Long],
    ifNotExists: Boolean,
    replace: Boolean) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val source = sourceSnapshotId match {
      case Some(id) => OptionalLong.of(id)
      case None => OptionalLong.empty()
    }
    if (replace) {
      table.replaceBranch(branchName, source)
    } else {
      try {
        table.createBranch(branchName, source)
      } catch {
        case _: BranchAlreadyExistsException if ifNotExists =>
          // ignored: branch already exists and IF NOT EXISTS was requested
      }
    }
    Seq.empty
  }
}

/**
 * Physical plan node for ALTER TABLE ... DROP BRANCH.
 */
case class DropBranchExec(
    table: SupportsBranching,
    branchName: String,
    ifExists: Boolean) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val removed = table.dropBranch(branchName)
    if (!removed && !ifExists) {
      throw new BranchNotFoundException(branchName, table.name())
    }
    Seq.empty
  }
}

/**
 * Physical plan node for ALTER TABLE ... FASTFORWARD BRANCH, which advances {@code branchName}
 * to the head snapshot of {@code targetBranchName}.
 */
case class FastForwardBranchExec(
    table: SupportsBranching,
    branchName: String,
    targetBranchName: String) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    table.fastForwardBranch(branchName, targetBranchName)
    Seq.empty
  }
}

/**
 * Physical plan node for SHOW BRANCHES.
 */
case class ShowBranchesExec(
    output: Seq[Attribute],
    table: SupportsBranching) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    table.listBranches().toSeq.map { branch =>
      val snapshot: Any = if (branch.snapshotId().isPresent) {
        branch.snapshotId().getAsLong
      } else {
        null
      }
      // Internal TimestampType representation is microseconds since the epoch.
      val creationTimeMicros = branch.creationTimeMs() * 1000L
      InternalRow(
        UTF8String.fromString(branch.name()),
        snapshot,
        creationTimeMicros)
    }
  }
}
