import sbt._
import sbt.Keys._

/**
 * Suppresses the "N keys that are not used by any other settings/tasks" lint
 * that fires on project load. These are a side effect of applying broad shared
 * settings (e.g. Checkstyle, publishMavenStyle) to all projects, including
 * assembly and other modules that don't use all of them.
 */
object GlobalSettingsPlugin extends AutoPlugin {
  override def trigger = allRequirements
  override def globalSettings: Seq[Setting[_]] = Seq(
    lintUnusedKeysOnLoad := false
  )
}
