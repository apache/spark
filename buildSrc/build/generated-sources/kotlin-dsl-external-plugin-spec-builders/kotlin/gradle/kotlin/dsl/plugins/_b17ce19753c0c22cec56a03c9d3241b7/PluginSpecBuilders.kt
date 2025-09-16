/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@file:Suppress(
    "unused",
    "nothing_to_inline",
    "useless_cast",
    "unchecked_cast",
    "extension_shadowed_by_member",
    "redundant_projection",
    "RemoveRedundantBackticks",
    "ObjectPropertyName",
    "deprecation"
)
@file:org.gradle.api.Generated

/* ktlint-disable */

package gradle.kotlin.dsl.plugins._b17ce19753c0c22cec56a03c9d3241b7

import org.gradle.plugin.use.PluginDependenciesSpec
import org.gradle.plugin.use.PluginDependencySpec


/**
 * The `com` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com`.
 */
internal
val `PluginDependenciesSpec`.`com`: `ComPluginGroup`
    get() = `ComPluginGroup`(this)


/**
 * The `com.github` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComGithubPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com.github`.
 */
internal
val `ComPluginGroup`.`github`: `ComGithubPluginGroup`
    get() = `ComGithubPluginGroup`(plugins)


/**
 * The `com.github.johnrengelman` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComGithubJohnrengelmanPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com.github.johnrengelman`.
 */
internal
val `ComGithubPluginGroup`.`johnrengelman`: `ComGithubJohnrengelmanPluginGroup`
    get() = `ComGithubJohnrengelmanPluginGroup`(plugins)


/**
 * The `com.github.johnrengelman.shadow` plugin implemented by [com.github.jengelman.gradle.plugins.shadow.ShadowPlugin].
 */
internal
val `ComGithubJohnrengelmanPluginGroup`.`shadow`: PluginDependencySpec
    get() = plugins.id("com.github.johnrengelman.shadow")


/**
 * The `kotlin` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper].
 */
internal
val `PluginDependenciesSpec`.`kotlin`: PluginDependencySpec
    get() = this.id("kotlin")


/**
 * The `kotlin-android` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinAndroidPluginWrapper].
 */
internal
val `PluginDependenciesSpec`.`kotlin-android`: PluginDependencySpec
    get() = this.id("kotlin-android")


/**
 * The `kotlin-android-extensions` plugin implemented by [org.jetbrains.kotlin.gradle.internal.AndroidExtensionsSubpluginIndicator].
 */
internal
val `PluginDependenciesSpec`.`kotlin-android-extensions`: PluginDependencySpec
    get() = this.id("kotlin-android-extensions")


/**
 * The `kotlin-kapt` plugin implemented by [org.jetbrains.kotlin.gradle.internal.Kapt3GradleSubplugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-kapt`: PluginDependencySpec
    get() = this.id("kotlin-kapt")


/**
 * The `kotlin-multiplatform` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinMultiplatformPluginWrapper].
 */
internal
val `PluginDependenciesSpec`.`kotlin-multiplatform`: PluginDependencySpec
    get() = this.id("kotlin-multiplatform")


/**
 * The `kotlin-native-cocoapods` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.cocoapods.KotlinCocoapodsPlugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-native-cocoapods`: PluginDependencySpec
    get() = this.id("kotlin-native-cocoapods")


/**
 * The `kotlin-native-performance` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.performance.KotlinPerformancePlugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-native-performance`: PluginDependencySpec
    get() = this.id("kotlin-native-performance")


/**
 * The `kotlin-parcelize` plugin implemented by [org.jetbrains.kotlin.gradle.internal.ParcelizeSubplugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-parcelize`: PluginDependencySpec
    get() = this.id("kotlin-parcelize")


/**
 * The `kotlin-platform-android` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPlatformAndroidPlugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-platform-android`: PluginDependencySpec
    get() = this.id("kotlin-platform-android")


/**
 * The `kotlin-platform-common` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPlatformCommonPlugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-platform-common`: PluginDependencySpec
    get() = this.id("kotlin-platform-common")


/**
 * The `kotlin-platform-js` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPlatformJsPlugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-platform-js`: PluginDependencySpec
    get() = this.id("kotlin-platform-js")


/**
 * The `kotlin-platform-jvm` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPlatformJvmPlugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-platform-jvm`: PluginDependencySpec
    get() = this.id("kotlin-platform-jvm")


/**
 * The `kotlin-scripting` plugin implemented by [org.jetbrains.kotlin.gradle.scripting.internal.ScriptingGradleSubplugin].
 */
internal
val `PluginDependenciesSpec`.`kotlin-scripting`: PluginDependencySpec
    get() = this.id("kotlin-scripting")


/**
 * The `org` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org`.
 */
internal
val `PluginDependenciesSpec`.`org`: `OrgPluginGroup`
    get() = `OrgPluginGroup`(this)


/**
 * The `org.gradle` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgGradlePluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.gradle`.
 */
internal
val `OrgPluginGroup`.`gradle`: `OrgGradlePluginGroup`
    get() = `OrgGradlePluginGroup`(plugins)


/**
 * The `org.gradle.antlr` plugin implemented by [org.gradle.api.plugins.antlr.AntlrPlugin].
 */
internal
val `OrgGradlePluginGroup`.`antlr`: PluginDependencySpec
    get() = plugins.id("org.gradle.antlr")


/**
 * The `org.gradle.application` plugin implemented by [org.gradle.api.plugins.ApplicationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`application`: PluginDependencySpec
    get() = plugins.id("org.gradle.application")


/**
 * The `org.gradle.assembler` plugin implemented by [org.gradle.language.assembler.plugins.AssemblerPlugin].
 */
internal
val `OrgGradlePluginGroup`.`assembler`: PluginDependencySpec
    get() = plugins.id("org.gradle.assembler")


/**
 * The `org.gradle.assembler-lang` plugin implemented by [org.gradle.language.assembler.plugins.AssemblerLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`assembler-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.assembler-lang")


/**
 * The `org.gradle.base` plugin implemented by [org.gradle.api.plugins.BasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`base`: PluginDependencySpec
    get() = plugins.id("org.gradle.base")


/**
 * The `org.gradle.binary-base` plugin implemented by [org.gradle.platform.base.plugins.BinaryBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`binary-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.binary-base")


/**
 * The `org.gradle.build-dashboard` plugin implemented by [org.gradle.api.reporting.plugins.BuildDashboardPlugin].
 */
internal
val `OrgGradlePluginGroup`.`build-dashboard`: PluginDependencySpec
    get() = plugins.id("org.gradle.build-dashboard")


/**
 * The `org.gradle.build-init` plugin implemented by [org.gradle.buildinit.plugins.BuildInitPlugin].
 */
internal
val `OrgGradlePluginGroup`.`build-init`: PluginDependencySpec
    get() = plugins.id("org.gradle.build-init")


/**
 * The `org.gradle.c` plugin implemented by [org.gradle.language.c.plugins.CPlugin].
 */
internal
val `OrgGradlePluginGroup`.`c`: PluginDependencySpec
    get() = plugins.id("org.gradle.c")


/**
 * The `org.gradle.c-lang` plugin implemented by [org.gradle.language.c.plugins.CLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`c-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.c-lang")


/**
 * The `org.gradle.checkstyle` plugin implemented by [org.gradle.api.plugins.quality.CheckstylePlugin].
 */
internal
val `OrgGradlePluginGroup`.`checkstyle`: PluginDependencySpec
    get() = plugins.id("org.gradle.checkstyle")


/**
 * The `org.gradle.clang-compiler` plugin implemented by [org.gradle.nativeplatform.toolchain.plugins.ClangCompilerPlugin].
 */
internal
val `OrgGradlePluginGroup`.`clang-compiler`: PluginDependencySpec
    get() = plugins.id("org.gradle.clang-compiler")


/**
 * The `org.gradle.codenarc` plugin implemented by [org.gradle.api.plugins.quality.CodeNarcPlugin].
 */
internal
val `OrgGradlePluginGroup`.`codenarc`: PluginDependencySpec
    get() = plugins.id("org.gradle.codenarc")


/**
 * The `org.gradle.component-base` plugin implemented by [org.gradle.platform.base.plugins.ComponentBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`component-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.component-base")


/**
 * The `org.gradle.component-model-base` plugin implemented by [org.gradle.language.base.plugins.ComponentModelBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`component-model-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.component-model-base")


/**
 * The `org.gradle.cpp` plugin implemented by [org.gradle.language.cpp.plugins.CppPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp")


/**
 * The `org.gradle.cpp-application` plugin implemented by [org.gradle.language.cpp.plugins.CppApplicationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp-application`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp-application")


/**
 * The `org.gradle.cpp-lang` plugin implemented by [org.gradle.language.cpp.plugins.CppLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp-lang")


/**
 * The `org.gradle.cpp-library` plugin implemented by [org.gradle.language.cpp.plugins.CppLibraryPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp-library`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp-library")


/**
 * The `org.gradle.cpp-unit-test` plugin implemented by [org.gradle.nativeplatform.test.cpp.plugins.CppUnitTestPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp-unit-test`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp-unit-test")


/**
 * The `org.gradle.cunit` plugin implemented by [org.gradle.nativeplatform.test.cunit.plugins.CUnitConventionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cunit`: PluginDependencySpec
    get() = plugins.id("org.gradle.cunit")


/**
 * The `org.gradle.cunit-test-suite` plugin implemented by [org.gradle.nativeplatform.test.cunit.plugins.CUnitPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cunit-test-suite`: PluginDependencySpec
    get() = plugins.id("org.gradle.cunit-test-suite")


/**
 * The `org.gradle.distribution` plugin implemented by [org.gradle.api.distribution.plugins.DistributionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`distribution`: PluginDependencySpec
    get() = plugins.id("org.gradle.distribution")


/**
 * The `org.gradle.ear` plugin implemented by [org.gradle.plugins.ear.EarPlugin].
 */
internal
val `OrgGradlePluginGroup`.`ear`: PluginDependencySpec
    get() = plugins.id("org.gradle.ear")


/**
 * The `org.gradle.eclipse` plugin implemented by [org.gradle.plugins.ide.eclipse.EclipsePlugin].
 */
internal
val `OrgGradlePluginGroup`.`eclipse`: PluginDependencySpec
    get() = plugins.id("org.gradle.eclipse")


/**
 * The `org.gradle.eclipse-wtp` plugin implemented by [org.gradle.plugins.ide.eclipse.EclipseWtpPlugin].
 */
internal
val `OrgGradlePluginGroup`.`eclipse-wtp`: PluginDependencySpec
    get() = plugins.id("org.gradle.eclipse-wtp")


/**
 * The `org.gradle.gcc-compiler` plugin implemented by [org.gradle.nativeplatform.toolchain.plugins.GccCompilerPlugin].
 */
internal
val `OrgGradlePluginGroup`.`gcc-compiler`: PluginDependencySpec
    get() = plugins.id("org.gradle.gcc-compiler")


/**
 * The `org.gradle.google-test` plugin implemented by [org.gradle.nativeplatform.test.googletest.plugins.GoogleTestConventionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`google-test`: PluginDependencySpec
    get() = plugins.id("org.gradle.google-test")


/**
 * The `org.gradle.google-test-test-suite` plugin implemented by [org.gradle.nativeplatform.test.googletest.plugins.GoogleTestPlugin].
 */
internal
val `OrgGradlePluginGroup`.`google-test-test-suite`: PluginDependencySpec
    get() = plugins.id("org.gradle.google-test-test-suite")


/**
 * The `org.gradle.groovy` plugin implemented by [org.gradle.api.plugins.GroovyPlugin].
 */
internal
val `OrgGradlePluginGroup`.`groovy`: PluginDependencySpec
    get() = plugins.id("org.gradle.groovy")


/**
 * The `org.gradle.groovy-base` plugin implemented by [org.gradle.api.plugins.GroovyBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`groovy-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.groovy-base")


/**
 * The `org.gradle.groovy-gradle-plugin` plugin implemented by [org.gradle.plugin.devel.internal.precompiled.PrecompiledGroovyPluginsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`groovy-gradle-plugin`: PluginDependencySpec
    get() = plugins.id("org.gradle.groovy-gradle-plugin")


/**
 * The `org.gradle.help-tasks` plugin implemented by [org.gradle.api.plugins.HelpTasksPlugin].
 */
internal
val `OrgGradlePluginGroup`.`help-tasks`: PluginDependencySpec
    get() = plugins.id("org.gradle.help-tasks")


/**
 * The `org.gradle.idea` plugin implemented by [org.gradle.plugins.ide.idea.IdeaPlugin].
 */
internal
val `OrgGradlePluginGroup`.`idea`: PluginDependencySpec
    get() = plugins.id("org.gradle.idea")


/**
 * The `org.gradle.ivy-publish` plugin implemented by [org.gradle.api.publish.ivy.plugins.IvyPublishPlugin].
 */
internal
val `OrgGradlePluginGroup`.`ivy-publish`: PluginDependencySpec
    get() = plugins.id("org.gradle.ivy-publish")


/**
 * The `org.gradle.jacoco` plugin implemented by [org.gradle.testing.jacoco.plugins.JacocoPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jacoco`: PluginDependencySpec
    get() = plugins.id("org.gradle.jacoco")


/**
 * The `org.gradle.jacoco-report-aggregation` plugin implemented by [org.gradle.testing.jacoco.plugins.JacocoReportAggregationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jacoco-report-aggregation`: PluginDependencySpec
    get() = plugins.id("org.gradle.jacoco-report-aggregation")


/**
 * The `org.gradle.java` plugin implemented by [org.gradle.api.plugins.JavaPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java`: PluginDependencySpec
    get() = plugins.id("org.gradle.java")


/**
 * The `org.gradle.java-base` plugin implemented by [org.gradle.api.plugins.JavaBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-base")


/**
 * The `org.gradle.java-gradle-plugin` plugin implemented by [org.gradle.plugin.devel.plugins.JavaGradlePluginPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-gradle-plugin`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-gradle-plugin")


/**
 * The `org.gradle.java-library` plugin implemented by [org.gradle.api.plugins.JavaLibraryPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-library`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-library")


/**
 * The `org.gradle.java-library-distribution` plugin implemented by [org.gradle.api.plugins.JavaLibraryDistributionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-library-distribution`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-library-distribution")


/**
 * The `org.gradle.java-platform` plugin implemented by [org.gradle.api.plugins.JavaPlatformPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-platform`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-platform")


/**
 * The `org.gradle.java-test-fixtures` plugin implemented by [org.gradle.api.plugins.JavaTestFixturesPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-test-fixtures`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-test-fixtures")


/**
 * The `org.gradle.jdk-toolchains` plugin implemented by [org.gradle.api.plugins.JdkToolchainsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jdk-toolchains`: PluginDependencySpec
    get() = plugins.id("org.gradle.jdk-toolchains")


/**
 * The `org.gradle.jvm-ecosystem` plugin implemented by [org.gradle.api.plugins.JvmEcosystemPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jvm-ecosystem`: PluginDependencySpec
    get() = plugins.id("org.gradle.jvm-ecosystem")


/**
 * The `org.gradle.jvm-test-suite` plugin implemented by [org.gradle.api.plugins.JvmTestSuitePlugin].
 */
internal
val `OrgGradlePluginGroup`.`jvm-test-suite`: PluginDependencySpec
    get() = plugins.id("org.gradle.jvm-test-suite")


/**
 * The `org.gradle.jvm-toolchain-management` plugin implemented by [org.gradle.api.plugins.JvmToolchainManagementPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jvm-toolchain-management`: PluginDependencySpec
    get() = plugins.id("org.gradle.jvm-toolchain-management")


/**
 * The `org.gradle.jvm-toolchains` plugin implemented by [org.gradle.api.plugins.JvmToolchainsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jvm-toolchains`: PluginDependencySpec
    get() = plugins.id("org.gradle.jvm-toolchains")


/**
 * The `org.gradle.language-base` plugin implemented by [org.gradle.language.base.plugins.LanguageBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`language-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.language-base")


/**
 * The `org.gradle.lifecycle-base` plugin implemented by [org.gradle.language.base.plugins.LifecycleBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`lifecycle-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.lifecycle-base")


/**
 * The `org.gradle.maven-publish` plugin implemented by [org.gradle.api.publish.maven.plugins.MavenPublishPlugin].
 */
internal
val `OrgGradlePluginGroup`.`maven-publish`: PluginDependencySpec
    get() = plugins.id("org.gradle.maven-publish")


/**
 * The `org.gradle.microsoft-visual-cpp-compiler` plugin implemented by [org.gradle.nativeplatform.toolchain.plugins.MicrosoftVisualCppCompilerPlugin].
 */
internal
val `OrgGradlePluginGroup`.`microsoft-visual-cpp-compiler`: PluginDependencySpec
    get() = plugins.id("org.gradle.microsoft-visual-cpp-compiler")


/**
 * The `org.gradle.native-component` plugin implemented by [org.gradle.nativeplatform.plugins.NativeComponentPlugin].
 */
internal
val `OrgGradlePluginGroup`.`native-component`: PluginDependencySpec
    get() = plugins.id("org.gradle.native-component")


/**
 * The `org.gradle.native-component-model` plugin implemented by [org.gradle.nativeplatform.plugins.NativeComponentModelPlugin].
 */
internal
val `OrgGradlePluginGroup`.`native-component-model`: PluginDependencySpec
    get() = plugins.id("org.gradle.native-component-model")


/**
 * The `org.gradle.objective-c` plugin implemented by [org.gradle.language.objectivec.plugins.ObjectiveCPlugin].
 */
internal
val `OrgGradlePluginGroup`.`objective-c`: PluginDependencySpec
    get() = plugins.id("org.gradle.objective-c")


/**
 * The `org.gradle.objective-c-lang` plugin implemented by [org.gradle.language.objectivec.plugins.ObjectiveCLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`objective-c-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.objective-c-lang")


/**
 * The `org.gradle.objective-cpp` plugin implemented by [org.gradle.language.objectivecpp.plugins.ObjectiveCppPlugin].
 */
internal
val `OrgGradlePluginGroup`.`objective-cpp`: PluginDependencySpec
    get() = plugins.id("org.gradle.objective-cpp")


/**
 * The `org.gradle.objective-cpp-lang` plugin implemented by [org.gradle.language.objectivecpp.plugins.ObjectiveCppLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`objective-cpp-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.objective-cpp-lang")


/**
 * The `org.gradle.pmd` plugin implemented by [org.gradle.api.plugins.quality.PmdPlugin].
 */
internal
val `OrgGradlePluginGroup`.`pmd`: PluginDependencySpec
    get() = plugins.id("org.gradle.pmd")


/**
 * The `org.gradle.project-report` plugin implemented by [org.gradle.api.plugins.ProjectReportsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`project-report`: PluginDependencySpec
    get() = plugins.id("org.gradle.project-report")


/**
 * The `org.gradle.project-reports` plugin implemented by [org.gradle.api.plugins.ProjectReportsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`project-reports`: PluginDependencySpec
    get() = plugins.id("org.gradle.project-reports")


/**
 * The `org.gradle.publishing` plugin implemented by [org.gradle.api.publish.plugins.PublishingPlugin].
 */
internal
val `OrgGradlePluginGroup`.`publishing`: PluginDependencySpec
    get() = plugins.id("org.gradle.publishing")


/**
 * The `org.gradle.reporting-base` plugin implemented by [org.gradle.api.plugins.ReportingBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`reporting-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.reporting-base")


/**
 * The `org.gradle.scala` plugin implemented by [org.gradle.api.plugins.scala.ScalaPlugin].
 */
internal
val `OrgGradlePluginGroup`.`scala`: PluginDependencySpec
    get() = plugins.id("org.gradle.scala")


/**
 * The `org.gradle.scala-base` plugin implemented by [org.gradle.api.plugins.scala.ScalaBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`scala-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.scala-base")


/**
 * The `org.gradle.signing` plugin implemented by [org.gradle.plugins.signing.SigningPlugin].
 */
internal
val `OrgGradlePluginGroup`.`signing`: PluginDependencySpec
    get() = plugins.id("org.gradle.signing")


/**
 * The `org.gradle.standard-tool-chains` plugin implemented by [org.gradle.nativeplatform.toolchain.internal.plugins.StandardToolChainsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`standard-tool-chains`: PluginDependencySpec
    get() = plugins.id("org.gradle.standard-tool-chains")


/**
 * The `org.gradle.swift-application` plugin implemented by [org.gradle.language.swift.plugins.SwiftApplicationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`swift-application`: PluginDependencySpec
    get() = plugins.id("org.gradle.swift-application")


/**
 * The `org.gradle.swift-library` plugin implemented by [org.gradle.language.swift.plugins.SwiftLibraryPlugin].
 */
internal
val `OrgGradlePluginGroup`.`swift-library`: PluginDependencySpec
    get() = plugins.id("org.gradle.swift-library")


/**
 * The `org.gradle.swiftpm-export` plugin implemented by [org.gradle.swiftpm.plugins.SwiftPackageManagerExportPlugin].
 */
internal
val `OrgGradlePluginGroup`.`swiftpm-export`: PluginDependencySpec
    get() = plugins.id("org.gradle.swiftpm-export")


/**
 * The `org.gradle.test-report-aggregation` plugin implemented by [org.gradle.api.plugins.TestReportAggregationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`test-report-aggregation`: PluginDependencySpec
    get() = plugins.id("org.gradle.test-report-aggregation")


/**
 * The `org.gradle.test-suite-base` plugin implemented by [org.gradle.testing.base.plugins.TestSuiteBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`test-suite-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.test-suite-base")


/**
 * The `org.gradle.version-catalog` plugin implemented by [org.gradle.api.plugins.catalog.VersionCatalogPlugin].
 */
internal
val `OrgGradlePluginGroup`.`version-catalog`: PluginDependencySpec
    get() = plugins.id("org.gradle.version-catalog")


/**
 * The `org.gradle.visual-studio` plugin implemented by [org.gradle.ide.visualstudio.plugins.VisualStudioPlugin].
 */
internal
val `OrgGradlePluginGroup`.`visual-studio`: PluginDependencySpec
    get() = plugins.id("org.gradle.visual-studio")


/**
 * The `org.gradle.war` plugin implemented by [org.gradle.api.plugins.WarPlugin].
 */
internal
val `OrgGradlePluginGroup`.`war`: PluginDependencySpec
    get() = plugins.id("org.gradle.war")


/**
 * The `org.gradle.windows-resource-script` plugin implemented by [org.gradle.language.rc.plugins.WindowsResourceScriptPlugin].
 */
internal
val `OrgGradlePluginGroup`.`windows-resource-script`: PluginDependencySpec
    get() = plugins.id("org.gradle.windows-resource-script")


/**
 * The `org.gradle.windows-resources` plugin implemented by [org.gradle.language.rc.plugins.WindowsResourcesPlugin].
 */
internal
val `OrgGradlePluginGroup`.`windows-resources`: PluginDependencySpec
    get() = plugins.id("org.gradle.windows-resources")


/**
 * The `org.gradle.wrapper` plugin implemented by [org.gradle.buildinit.plugins.WrapperPlugin].
 */
internal
val `OrgGradlePluginGroup`.`wrapper`: PluginDependencySpec
    get() = plugins.id("org.gradle.wrapper")


/**
 * The `org.gradle.xcode` plugin implemented by [org.gradle.ide.xcode.plugins.XcodePlugin].
 */
internal
val `OrgGradlePluginGroup`.`xcode`: PluginDependencySpec
    get() = plugins.id("org.gradle.xcode")


/**
 * The `org.gradle.xctest` plugin implemented by [org.gradle.nativeplatform.test.xctest.plugins.XCTestConventionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`xctest`: PluginDependencySpec
    get() = plugins.id("org.gradle.xctest")


/**
 * The `org.jetbrains` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgJetbrainsPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.jetbrains`.
 */
internal
val `OrgPluginGroup`.`jetbrains`: `OrgJetbrainsPluginGroup`
    get() = `OrgJetbrainsPluginGroup`(plugins)


/**
 * The `org.jetbrains.kotlin` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgJetbrainsKotlinPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.jetbrains.kotlin`.
 */
internal
val `OrgJetbrainsPluginGroup`.`kotlin`: `OrgJetbrainsKotlinPluginGroup`
    get() = `OrgJetbrainsKotlinPluginGroup`(plugins)


/**
 * The `org.jetbrains.kotlin.android` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinAndroidPluginWrapper].
 */
internal
val `OrgJetbrainsKotlinPluginGroup`.`android`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.android")


/**
 * The `org.jetbrains.kotlin.js` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinJsPluginWrapper].
 */
internal
val `OrgJetbrainsKotlinPluginGroup`.`js`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.js")


/**
 * The `org.jetbrains.kotlin.jvm` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper].
 */
internal
val `OrgJetbrainsKotlinPluginGroup`.`jvm`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.jvm")


/**
 * The `org.jetbrains.kotlin.kapt` plugin implemented by [org.jetbrains.kotlin.gradle.internal.Kapt3GradleSubplugin].
 */
internal
val `OrgJetbrainsKotlinPluginGroup`.`kapt`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.kapt")


/**
 * The `org.jetbrains.kotlin.multiplatform` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinMultiplatformPluginWrapper].
 */
internal
val `OrgJetbrainsKotlinPluginGroup`.`multiplatform`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.multiplatform")


/**
 * The `org.jetbrains.kotlin.native` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgJetbrainsKotlinNativePluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.jetbrains.kotlin.native`.
 */
internal
val `OrgJetbrainsKotlinPluginGroup`.`native`: `OrgJetbrainsKotlinNativePluginGroup`
    get() = `OrgJetbrainsKotlinNativePluginGroup`(plugins)


/**
 * The `org.jetbrains.kotlin.native.cocoapods` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.cocoapods.KotlinCocoapodsPlugin].
 */
internal
val `OrgJetbrainsKotlinNativePluginGroup`.`cocoapods`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.native.cocoapods")


/**
 * The `org.jetbrains.kotlin.native.performance` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.performance.KotlinPerformancePlugin].
 */
internal
val `OrgJetbrainsKotlinNativePluginGroup`.`performance`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.native.performance")


/**
 * The `org.jetbrains.kotlin.platform` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgJetbrainsKotlinPlatformPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.jetbrains.kotlin.platform`.
 */
internal
val `OrgJetbrainsKotlinPluginGroup`.`platform`: `OrgJetbrainsKotlinPlatformPluginGroup`
    get() = `OrgJetbrainsKotlinPlatformPluginGroup`(plugins)


/**
 * The `org.jetbrains.kotlin.platform.android` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPlatformAndroidPlugin].
 */
internal
val `OrgJetbrainsKotlinPlatformPluginGroup`.`android`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.platform.android")


/**
 * The `org.jetbrains.kotlin.platform.common` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPlatformCommonPlugin].
 */
internal
val `OrgJetbrainsKotlinPlatformPluginGroup`.`common`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.platform.common")


/**
 * The `org.jetbrains.kotlin.platform.js` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPlatformJsPlugin].
 */
internal
val `OrgJetbrainsKotlinPlatformPluginGroup`.`js`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.platform.js")


/**
 * The `org.jetbrains.kotlin.platform.jvm` plugin implemented by [org.jetbrains.kotlin.gradle.plugin.KotlinPlatformJvmPlugin].
 */
internal
val `OrgJetbrainsKotlinPlatformPluginGroup`.`jvm`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.platform.jvm")


/**
 * The `org.jetbrains.kotlin.plugin` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgJetbrainsKotlinPluginPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.jetbrains.kotlin.plugin`.
 */
internal
val `OrgJetbrainsKotlinPluginGroup`.`plugin`: `OrgJetbrainsKotlinPluginPluginGroup`
    get() = `OrgJetbrainsKotlinPluginPluginGroup`(plugins)


/**
 * The `org.jetbrains.kotlin.plugin.parcelize` plugin implemented by [org.jetbrains.kotlin.gradle.internal.ParcelizeSubplugin].
 */
internal
val `OrgJetbrainsKotlinPluginPluginGroup`.`parcelize`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.plugin.parcelize")


/**
 * The `org.jetbrains.kotlin.plugin.scripting` plugin implemented by [org.jetbrains.kotlin.gradle.scripting.internal.ScriptingGradleSubplugin].
 */
internal
val `OrgJetbrainsKotlinPluginPluginGroup`.`scripting`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.kotlin.plugin.scripting")
