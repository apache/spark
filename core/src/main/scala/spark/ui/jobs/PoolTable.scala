package spark.ui.jobs

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.xml.Node

import spark.scheduler.Stage
import spark.scheduler.cluster.Schedulable
import spark.ui.UIUtils

/** Table showing list of pools */
private[spark] class PoolTable(pools: Seq[Schedulable], listener: JobProgressListener) {

  var poolToActiveStages: HashMap[String, HashSet[Stage]] = listener.poolToActiveStages

  def toNodeSeq(): Seq[Node] = {
    listener.synchronized {
      poolTable(poolRow, pools)
    }
  }

  private def poolTable(makeRow: (Schedulable, HashMap[String, HashSet[Stage]]) => Seq[Node],
    rows: Seq[Schedulable]
    ): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable table-fixed">
      <thead>
        <th>Pool Name</th>
        <th>Minimum Share</th>
        <th>Pool Weight</th>
        <th>Active Stages</th>
        <th>Running Tasks</th>
        <th>SchedulingMode</th>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r, poolToActiveStages))}
      </tbody>
    </table>
  }

  private def poolRow(p: Schedulable, poolToActiveStages: HashMap[String, HashSet[Stage]])
    : Seq[Node] = {
    val activeStages = poolToActiveStages.get(p.name) match {
      case Some(stages) => stages.size
      case None => 0
    }
    <tr>
      <td><a href={"%s/stages/pool?poolname=%s".format(UIUtils.addBaseUri(),p.name)}>{p.name}</a></td>
      <td>{p.minShare}</td>
      <td>{p.weight}</td>
      <td>{activeStages}</td>
      <td>{p.runningTasks}</td>
      <td>{p.schedulingMode}</td>
    </tr>
  }
}

