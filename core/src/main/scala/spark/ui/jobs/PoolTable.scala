package spark.ui.jobs

import scala.xml.Node
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark.scheduler.Stage
import spark.scheduler.cluster.Schedulable

/** Table showing list of pools */
private[spark] class PoolTable(pools: Seq[Schedulable], listener: JobProgressListener) {

  var poolToActiveStages: HashMap[String, HashSet[Stage]] = listener.poolToActiveStages

  def toNodeSeq(): Seq[Node] = {
    poolTable(poolRow, pools)
  }

  // pool tables
  def poolTable(makeRow: (Schedulable, HashMap[String, HashSet[Stage]]) => Seq[Node],
    rows: Seq[Schedulable]
    ): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <th>Pool Name</th>
        <th>Minimum Share</th>
        <th>Pool Weight</th>
        <td>Active Stages</td>
        <td>Running Tasks</td>
        <td>SchedulingMode</td>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r, poolToActiveStages))}
      </tbody>
    </table>
  }

  def poolRow(p: Schedulable, poolToActiveStages: HashMap[String, HashSet[Stage]]): Seq[Node] = {
    <tr>
      <td><a href={"/stages/pool?poolname=%s".format(p.name)}>{p.name}</a></td>
      <td>{p.minShare}</td>
      <td>{p.weight}</td>
      <td>{poolToActiveStages.getOrElseUpdate(p.name, new HashSet[Stage]()).size}</td>
      <td>{p.runningTasks}</td>
      <td>{p.schedulingMode}</td>
    </tr>
  }
}

