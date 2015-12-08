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

package org.apache.spark.deploy.history.yarn.server

import scala.collection.mutable

import org.apache.hadoop.yarn.api.records.{ApplicationReport, YarnApplicationState}
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._

/**
 * Utils on the history server-side.
 */
private[spark] object YarnProviderUtils extends Logging {

  /**
   * What should the value be for start/end times which are not set.
   */
  val UNSET_TIME_VALUE = 0L

  /**
   * Build an [[TimelineApplicationHistoryInfo]] instance from a [[TimelineEntity]].
   *
   * @param en the entity
   * @return an history info structure. The completed bit is true if the entity has an end time.
   * @throws Exception if the conversion failed
   */
  def toApplicationHistoryInfo(en: TimelineEntity): TimelineApplicationHistoryInfo = {
    val endTime = numberField(en, FIELD_END_TIME, UNSET_TIME_VALUE).longValue
    val startTime = numberField(en, FIELD_START_TIME, UNSET_TIME_VALUE).longValue
    val lastTimestamp = Math.max(startTime, endTime)
    var lastUpdated = numberField(en, FIELD_LAST_UPDATED).longValue
    if (lastUpdated < lastTimestamp) {
      logDebug(s"lastUpdated field $lastUpdated < latest timestamp $lastTimestamp; overwriting")
      lastUpdated = lastTimestamp
    }
    val name = field(en, FIELD_APP_NAME).asInstanceOf[String]
    val sparkUser = field(en, FIELD_APP_USER).asInstanceOf[String]
    val completed = endTime > 0

    val entityId = en.getEntityId()
    val appId = fieldOption(en, FIELD_APPLICATION_ID) match {
      case Some(value) => value.asInstanceOf[String]
      case None => entityId
    }
    val version = numberField(en, FIELD_ENTITY_VERSION, 0).longValue

    // the spark attempt ID; only unique amongst entities
    val sparkAttemptId = stringFieldOption(en, FIELD_ATTEMPT_ID)

    val attemptId = if (sparkAttemptId.isDefined) {
      sparkAttemptId
    } else {
      Some(entityId)
    }

    val attempt = new TimelineApplicationAttemptInfo(
      attemptId,
      startTime,
      endTime,
      lastUpdated,
      sparkUser,
      completed,
      entityId,
      sparkAttemptId,
      version)

    // return the single attempt which can be built from this entity
    new TimelineApplicationHistoryInfo(appId, name, attempt :: Nil)
  }

  /**
   * Describe the application history, including timestamps and completed flag.
   *
   * @param info history info to describe
   * @return a string description
   */
  def describeApplicationHistoryInfo(info: TimelineApplicationHistoryInfo): String = {
    val core = s"TimelineApplicationHistoryInfo [${info.id}] ${info.name}"
    if (info.attempts.isEmpty) {
      s"$core : no attempts"
    } else {
      val attempt = info.attempts.head
      s"$core : " + info.attempts.map(describeAttempt).mkString("[{", "}, {", "}]")
    }
  }

  /**
   * Describe an individual attempt.
   * @param attempt attempt
   * @return a string description
   */
  def describeAttempt(attempt: TimelineApplicationAttemptInfo): String = {
    val never = "-"

    s"Attempt ID ${attempt.attemptId }" +
        s" started ${timeShort(attempt.startTime, never) }," +
        s" ended ${timeShort(attempt.endTime, never) }" +
        s" updated ${timeShort(attempt.lastUpdated, never) }" +
        s" completed = ${attempt.completed }"
  }

  /**
   * Build a combined list with the policy of all original values come first,
   * followed by the later ones. Unless there is a later entry of the same ID...
   * In which case, that later entry appears.
   *
   * @param original original list of entries
   * @param latest later list of entries
   * @return a combined list.
   */
  def combineResults(original: Seq[TimelineApplicationHistoryInfo],
      latest: Seq[TimelineApplicationHistoryInfo]): Seq[TimelineApplicationHistoryInfo] = {
    // build map of original
    val results = new scala.collection.mutable.HashMap[String, TimelineApplicationHistoryInfo]
    original.map((elt) => results.put(elt.id, elt))
    // then merge in the new values, a combination of appending and adding
    latest.foreach(history => {
      val id = history.id
      results.put(id, results.get(id) match {
        case Some(old) => mergeAttempts(old, history)
        case None => history
      })
    })
    results.values.toList
  }

  /**
   * Merge two attempts. It is critical that the older event comes first,
   * so that the ordering is consistent.
   *
   * It's not enough to simply append values, as if the same attempt were merged
   * the result would be a history with duplicate attempts. Every attempt needs to
   * be unique.
   *
   * Note that `None` may be an attempt. Two entries with that as their attempt ID are
   * treated as equal and merged
   * @param old the older history
   * @param latest the latest attempt
   * @return the merged set
   */
  def mergeAttempts(old: TimelineApplicationHistoryInfo, latest: TimelineApplicationHistoryInfo):
  TimelineApplicationHistoryInfo = {
    val oldAttempts = old.attempts
    val latestAttempts = latest.attempts
    new TimelineApplicationHistoryInfo(old.id, old.name,
      mergeAttemptInfoLists(oldAttempts, latestAttempts))
  }

  /**
   * Merge the lists of two attempts. Factored out for ease of testing.
   *
   * @param oldAttempts list of old attempts -assuming no duplicate attempts
   * @param latestAttempts list of later attempts; this may include duplicate attempt entries.
   * @return an ordered list of attempts with original attempt entries removed if a later
   *         version updated the event information.
   */
  def mergeAttemptInfoLists(oldAttempts: List[TimelineApplicationAttemptInfo],
      latestAttempts: List[TimelineApplicationAttemptInfo])
    : List[TimelineApplicationAttemptInfo] = {

    // build map of id->attempt which will be updated during merge
    var attemptMap = mutable.Map[Option[String], TimelineApplicationAttemptInfo]()
    oldAttempts foreach {
      a => attemptMap += (a.attemptId -> a)
    }

    latestAttempts foreach (a => {
      val id = a.attemptId
      attemptMap get id match {
        case None =>
          // no match: insert into the map
          // and add to the map of attempts
          attemptMap += (id -> a)

        case Some(existing) =>
          // existing match, so merge.
          // this will also match Some(None), meaning there is an attempt ID with the of `None`,
          attemptMap += (id -> mostRecentAttempt(existing, a))
      }
    })
    val finalMapValues = attemptMap.values.toList
    val orderedAttemptList = sortAttempts(finalMapValues)
    orderedAttemptList
  }

  /**
   * Compare two entries of information about the same application attempt -and decide which
   * is the most recent one.
   *
   * @param attempt1 attempt 1
   * @param attempt2 attempt 2
   * @return the preferred outcome
   */
  def mostRecentAttempt(attempt1: TimelineApplicationAttemptInfo,
      attempt2: TimelineApplicationAttemptInfo): TimelineApplicationAttemptInfo = {
    (attempt1, attempt2) match {
      case (a1, a2) if a1.version > 0 && a2.version > 0 =>
        // use the version field if set
        if (a2.version > a1.version) a2 else a1
      case (a1, a2) if a1.completed => a1
      case (a1, a2) if a2.completed => a2
      case (a1, a2) if a2.lastUpdated >= a1.lastUpdated => a2
      case (a1, _) => a1
    }
  }

  /**
   * Comparator to find which attempt is newer than the other.
   *
   * @param attempt1 attempt 1
   * @param attempt2 attempt 2
   * @return true if attempt1 is considered newer than attempt2
   */
  def attemptNewerThan(attempt1: TimelineApplicationAttemptInfo,
      attempt2: TimelineApplicationAttemptInfo): Boolean = {
    if (attempt1.version > 0 && attempt2.version > 0) {
      attempt1.version > attempt2.version
    } else {
      attempt1.lastUpdated > attempt2.lastUpdated
    }
  }

  /**
   * Sort an attempt list using the such that newer attempts come first.
   *
   * It is critical for the web UI that completed events come before incomplete ones, so if
   * a completed one is found, it takes priority.
   * @param attempts attempt list to sort
   * @return a sorted list
   */
  def sortAttempts(attempts: List[TimelineApplicationAttemptInfo])
    : List[TimelineApplicationAttemptInfo] = {
    // sort attempts
    attempts.sortWith(attemptNewerThan)
  }

  /**
   * Get the start time of an application.
   *
   * For multiple attempts, the first attempt is chosen as the start time, so
   * that if sorting a list of application, the one attempted first is considered
   * the oldest.
   * @param info history info
   */
  def startTime(info: TimelineApplicationHistoryInfo): Long = {
    info.attempts match {
      case Nil => 0L
      case (h :: _) => h.startTime
    }
  }

  /**
   * Sort a list of applications by their start time.
   *
   * @param history history list
   * @return a new, sorted list
   */
  def sortApplicationsByStartTime(history: Seq[TimelineApplicationHistoryInfo]):
  Seq[TimelineApplicationHistoryInfo] = {
    history.sortBy(startTime)
  }

  /**
   * Find the latest application in the list. Scans the list once, so is O(n) even if
   * the list is already sorted.
   *
   * @param history history to scan (which can be an empty list
   * @return the latest element in the list
   */
  def findLatestApplication(history: Seq[TimelineApplicationHistoryInfo])
  : Option[TimelineApplicationHistoryInfo] = {
    history match {
      case Nil => None
      case l => Some(l.reduceLeft((x, y) => if (startTime(x) < startTime(y)) y else x))
    }
  }

  /**
   * Find the latest application in the list. Scans the list once, so is `O(n)` even if
   * the list is already sorted.
   *
   * @param history history to scan (which can be an empty list
   * @return the element in the list which started first
   */
  def findOldestApplication(history: Seq[TimelineApplicationHistoryInfo])
  : Option[TimelineApplicationHistoryInfo] = {
    history match {
      case Nil => None
      case l => Some(l.reduceLeft((x, y) => if (startTime(x) <= startTime(y)) x else y))
    }
  }

  /**
   * Find the application that represents the start of the update window.
   *
   * First it locates the oldest incomplete application in the list.
   * If there are no incomplete entries, then the latest completed entry is picked up.
   *
   * @param history history to scan (which can be an empty list)
   * @return the latest element in the list, or `None` for no match
   */
  def findStartOfWindow(history: Seq[TimelineApplicationHistoryInfo]):
    Option[TimelineApplicationHistoryInfo] = {
    findIncompleteApplications(history) match {
      // no incomplete apps; use latest
      case Nil => findLatestApplication(history)
      case incomplete => findOldestApplication(incomplete)
    }
  }

  /**
   * Is an application completed?
   *
   * @param info info to examine
   * @return true if its last attempt completed
   */
  def isCompleted(info: TimelineApplicationHistoryInfo): Boolean = {
    if (info.attempts.isEmpty) {
      // no events, implicitly not complete
      false
    } else {
      info.attempts.head.completed
    }
  }

  /**
   * Get the last update time; 0 if there are no attempts.
   *
   * @param info info to examine
   * @return the last update time of the attempt first in the list; 0 if none found
   */
  def lastUpdated(info: TimelineApplicationHistoryInfo): Long = {
    if (info.attempts.isEmpty) {
      // no events, implicitly not complete
      0
    } else {
      info.attempts.head.lastUpdated
    }
  }

  /**
   * Build the list of all incomplete applications.
   *
   * @param history history to scan (which can be an empty list)
   * @return a filtered list containing only those applications which are considered incompleted.
   */
  def findIncompleteApplications(history: Seq[TimelineApplicationHistoryInfo])
    : Seq[TimelineApplicationHistoryInfo] = {
    history.filter(x => !isCompleted(x))
  }

  /**
   * Count the number of incomplete applications in the sequence.
   *
   * @param history history to scan (which can be an empty list)
   * @return the number of incomplete applications found
   */
  def countIncompleteApplications(history: Seq[TimelineApplicationHistoryInfo]): Int = {
    findIncompleteApplications(history).size
  }

  /**
   * Find an application by its ID.
   *
   * @param history history to scan (which can be an empty list)
   * @param id app Id
   * @return the application or `None`
   */
  def findAppById(history: Seq[TimelineApplicationHistoryInfo], id: String)
    : Option[TimelineApplicationHistoryInfo] = {
    history.find(_.id == id)
  }

  /**
   * Get the finish time of the application, if it finished.
   *
   * @param appReport YARN application report
   * @return the finish time, or `None` if the application has not finished
   */
  def finishTime(appReport: ApplicationReport): Option[Long] = {
    if (appReport.getFinishTime > 0) Some(appReport.getFinishTime) else None
  }

  /**
   * Has an application finished?
   *
   * @param appReport application report
   * @return true if the application is in some terminated state
   */
  def isFinished(appReport: ApplicationReport): Boolean = {
    appReport.getYarnApplicationState match {
      case YarnApplicationState.FINISHED => true
      case YarnApplicationState.FAILED => true
      case YarnApplicationState.KILLED => true
      case _ => false
    }
  }

  /**
   * Given a sequence of application reports, create a map of them
   * mapped by the string value of their application ID.
   *
   * @param reports list of reports
   * @return mapped set
   */
  def reportsToMap(reports: Seq[ApplicationReport]): Map[String, ApplicationReport] = {
    var map: Map[String, ApplicationReport] = Map()
    reports.foreach(r => map = map + (r.getApplicationId.toString -> r))
    map
  }

  /**
   * Compare the list of applications with the YARN list and convert any that
   * are incomplete to completed state if they aren't found in the
   * list of running apps -or they are there, but failed.
   *
   * @param apps list of applications
   * @param recordMap map of app id to Application report entries
   * @param currentTime the current time in millis
   * @param livenessWindow the window in millis within which apps are considered automatically live
   * @return list of apps which are marked as incomplete but no longer running
   */
  private[yarn] def completeAppsFromYARN(apps: Seq[TimelineApplicationHistoryInfo],
      recordMap: Map[String, ApplicationReport],
      currentTime: Long,
      livenessWindow: Long): Seq[TimelineApplicationHistoryInfo] = {

    // complete an application, finishTime is optional finish time from report
    def complete(appInfo: TimelineApplicationHistoryInfo,
        finishTime: Option[Long]): TimelineApplicationHistoryInfo = {
      val incomplete = appInfo.attempts.head
      val endTime = finishTime match {
        case Some(t) => t
        case None => incomplete.lastUpdated
      }
      val updated = new TimelineApplicationAttemptInfo(
        incomplete.attemptId,
        incomplete.startTime,
        endTime,
        incomplete.lastUpdated,
        incomplete.sparkUser,
        true,
        incomplete.entityId,
        incomplete.sparkAttemptId)
      logDebug(s"Marking application ${appInfo.id} completed: ${describeAttempt(incomplete)}")
      new TimelineApplicationHistoryInfo(appInfo.id, appInfo.name, updated :: appInfo.attempts.tail)
    }

    // complete an application from a report
    def completeFromReport(appInfo: TimelineApplicationHistoryInfo, report: ApplicationReport):
      TimelineApplicationHistoryInfo = {

      complete(appInfo, finishTime(report))
    }

    // build and return an updated set of applications
    apps.map(app =>
      if (isCompleted(app)) {
        // app finished: return
        app
      } else {
        // app is incomplete, so look up the record.
        val id = app.id
        recordMap.get(id) match {
          case Some(report) if isFinished(report) =>
            // report found and app is actually finished
            logDebug(s"Incomplete app $id has halted as ${report.getYarnApplicationState}")
            completeFromReport(app, report)

          case Some(report) =>
            // in progress
            logDebug(s"App $id is in running state ${report.getYarnApplicationState}")
            app

          case None =>
            // app not found in the map. Outcome now depends on when it happened, to avoid
            // over-reacting to incomplete applications which are between AM instances and
            // briefly unlisted.
            val updated = lastUpdated(app)
            val humanTime = humanDateCurrentTZ(updated, "(never)")
            logDebug(s"Incomplete app $id updated at ${humanTime}is not in list of running apps")
            if ((currentTime - updated) > livenessWindow) {
              complete(app, None)
            } else {
              // complete it at the last update time
              app
            }
        }
      }
    )
  }

}
