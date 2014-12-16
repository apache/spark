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
package org.apache.spark.examples.mllib

import scopt.OptionParser
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
import java.text.BreakIterator
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.topicmodel.LatentDirichletAllocation
import org.apache.spark.mllib.topicmodel.LatentDirichletAllocation.Document
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.log4j.{Level, Logger}

/**
 *
 *
 * @author dlwh
 */
object SimpleLatentDirichletAllocation {
    case class Params(
      input: Seq[String] = Seq.empty,
      numTopics: Int = 20,
      wordSmoothing: Double = 0.1,
      topicSmoothing: Double = 0.1,
      vocabSize: Int = 10000,
      minWordCount: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]) {

    val parser = new OptionParser[Params]("SimpleLatentDirichletAllocation") {
      head("SimpleLatentDirichletAllocation: an example LDA app for plain text data.")
      opt[Int]("numTopics")
        .text("number of topics")
        .action((x, c) => c.copy(numTopics = x))
      opt[Double]("wordSmoothing")
        .text("amount of word smoothing to use")
        .action((x, c) => c.copy(wordSmoothing = x))
      opt[Double]("topicSmoothing")
        .text(s"amount of topic smoothing to use")
        .action((x, c) => c.copy(topicSmoothing = x))
      opt[Int]("vocabSize")
        .text(s"number of distinct word types to use, chosen by frequency (after stopword removal)")
        .action((x, c) => c.copy(vocabSize = x))
      opt[Int]("minWordCount")
        .text(s"minimum number of times a word must appear to be included in vocab.")
        .action((x, c) => c.copy(minWordCount = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora")
        .unbounded()
        .required()
        .action((x, c) => c.copy(input = c.input :+ x))
    }

    val params = parser.parse(args, Params()).getOrElse{parser.showUsageAsError; sys.exit(1)}

    val conf = new SparkConf().setAppName(s"LDA with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val corpus = preprocess(sc, params.input, params.vocabSize, params.minWordCount)
    corpus.cache()

    val lda = new LatentDirichletAllocation(params.numTopics,
      100,
      params.topicSmoothing,
      params.wordSmoothing,
      0)

    for (state <- lda.iterations(corpus)) {
      println(state.logLikelihood)
    }
  }

  def preprocess(sc: SparkContext,
                 paths: Seq[String],
                 vocabSize: Int,
                 minWordCount: Int): RDD[Document] = {
    val files = for(p <- paths) yield {
      sc.wholeTextFiles(p)
    }

    val textRDD = files.reduce( _ ++ _ )

    val tokenized = textRDD.zipWithIndex.map { case ((name, content), id) =>
       id -> SimpleTokenizer.getWords(content)
    }

    val wordCounts: RDD[(String, Int)] = {
      tokenized
        .flatMap{ case (_, tokens) => tokens.map(_ -> 1)}
        .reduceByKey(_ + _)
        .filter(_._2 >= minWordCount)
    }

    // word -> id
    val vocab = (
      wordCounts
        .sortBy(_._2, ascending = false)
        .take(vocabSize)
        .map(_._1)
        .zipWithIndex
        .toMap
      )

    val documents = tokenized.map { case (id, toks) =>
      val counts = breeze.linalg.Counter.countTraversable(toks)

      val indexedCounts = counts.iterator.collect { case (k, v) if vocab.contains(k) =>
        vocab(k) -> v.toDouble
      }

      val sb = org.apache.spark.mllib.linalg.Vectors.sparse(vocab.size, indexedCounts.toSeq)
      // I do not know why .sparse doesn't return a SparseVector.
      LatentDirichletAllocation.Document(sb.asInstanceOf[SparseVector], id)
    }

    documents
  }

}

object SimpleTokenizer {

  val allWordRegex = "^(\\p{L}|\\p{M})*$".r

  def getWords(text: String): IndexedSeq[String] = {
    val words = new ArrayBuffer[String]()
    val wb = BreakIterator.getWordInstance
    wb.setText(text)

    var current = wb.first()
    var end = wb.next()
    while (end != BreakIterator.DONE) {
      val word: String = text.substring(current, end).toLowerCase

      // remove short words, things that aren't only letters, and stop words
      if (allWordRegex.unapplySeq(word).nonEmpty &&  !stopWords(word) && word.length >= 3) {
        words += word
      }

      current = end
      end = wb.next()
    }
    words
  }

  val stopWords =
    """
      |a
      |able
      |about
      |above
      |abst
      |accordance
      |according
      |accordingly
      |across
      |act
      |actually
      |added
      |adj
      |affected
      |affecting
      |affects
      |after
      |afterwards
      |again
      |against
      |ah
      |all
      |almost
      |alone
      |along
      |already
      |also
      |although
      |always
      |am
      |among
      |amongst
      |an
      |and
      |announce
      |another
      |any
      |anybody
      |anyhow
      |anymore
      |anyone
      |anything
      |anyway
      |anyways
      |anywhere
      |apparently
      |approximately
      |are
      |aren
      |arent
      |arise
      |around
      |as
      |aside
      |ask
      |asking
      |at
      |auth
      |available
      |away
      |awfully
      |b
      |back
      |be
      |became
      |because
      |become
      |becomes
      |becoming
      |been
      |before
      |beforehand
      |begin
      |beginning
      |beginnings
      |begins
      |behind
      |being
      |believe
      |below
      |beside
      |besides
      |between
      |beyond
      |biol
      |both
      |brief
      |briefly
      |but
      |by
      |c
      |ca
      |came
      |can
      |cannot
      |can't
      |cause
      |causes
      |certain
      |certainly
      |co
      |com
      |come
      |comes
      |contain
      |containing
      |contains
      |could
      |couldnt
      |d
      |date
      |did
      |didn't
      |different
      |do
      |does
      |doesn't
      |doing
      |done
      |don't
      |down
      |downwards
      |due
      |during
      |e
      |each
      |ed
      |edu
      |effect
      |eg
      |eight
      |eighty
      |either
      |else
      |elsewhere
      |end
      |ending
      |enough
      |especially
      |et
      |et-al
      |etc
      |even
      |ever
      |every
      |everybody
      |everyone
      |everything
      |everywhere
      |ex
      |except
      |f
      |far
      |few
      |ff
      |fifth
      |first
      |five
      |fix
      |followed
      |following
      |follows
      |for
      |former
      |formerly
      |forth
      |found
      |four
      |from
      |further
      |furthermore
      |g
      |gave
      |get
      |gets
      |getting
      |give
      |given
      |gives
      |giving
      |go
      |goes
      |gone
      |got
      |gotten
      |h
      |had
      |happens
      |hardly
      |has
      |hasn't
      |have
      |haven't
      |having
      |he
      |hed
      |hence
      |her
      |here
      |hereafter
      |hereby
      |herein
      |heres
      |hereupon
      |hers
      |herself
      |hes
      |hi
      |hid
      |him
      |himself
      |his
      |hither
      |home
      |how
      |howbeit
      |however
      |hundred
      |i
      |id
      |ie
      |if
      |i'll
      |im
      |immediate
      |immediately
      |importance
      |important
      |in
      |inc
      |indeed
      |index
      |information
      |instead
      |into
      |invention
      |inward
      |is
      |isn't
      |it
      |itd
      |it'll
      |its
      |itself
      |i've
      |j
      |just
      |k
      |keep    keeps
      |kept
      |kg
      |km
      |know
      |known
      |knows
      |l
      |largely
      |last
      |lately
      |later
      |latter
      |latterly
      |least
      |less
      |lest
      |let
      |lets
      |like
      |liked
      |likely
      |line
      |little
      |'ll
      |look
      |looking
      |looks
      |ltd
      |m
      |made
      |mainly
      |make
      |makes
      |many
      |may
      |maybe
      |me
      |mean
      |means
      |meantime
      |meanwhile
      |merely
      |mg
      |might
      |million
      |miss
      |ml
      |more
      |moreover
      |most
      |mostly
      |mr
      |mrs
      |much
      |mug
      |must
      |my
      |myself
      |n
      |na
      |name
      |namely
      |nay
      |nd
      |near
      |nearly
      |necessarily
      |necessary
      |need
      |needs
      |neither
      |never
      |nevertheless
      |new
      |next
      |nine
      |ninety
      |no
      |nobody
      |non
      |none
      |nonetheless
      |noone
      |nor
      |normally
      |nos
      |not
      |noted
      |nothing
      |now
      |nowhere
      |o
      |obtain
      |obtained
      |obviously
      |of
      |off
      |often
      |oh
      |ok
      |okay
      |old
      |omitted
      |on
      |once
      |one
      |ones
      |only
      |onto
      |or
      |ord
      |other
      |others
      |otherwise
      |ought
      |our
      |ours
      |ourselves
      |out
      |outside
      |over
      |overall
      |owing
      |own
      |p
      |page
      |pages
      |part
      |particular
      |particularly
      |past
      |per
      |perhaps
      |placed
      |please
      |plus
      |poorly
      |possible
      |possibly
      |potentially
      |pp
      |predominantly
      |present
      |previously
      |primarily
      |probably
      |promptly
      |proud
      |provides
      |put
      |q
      |que
      |quickly
      |quite
      |qv
      |r
      |ran
      |rather
      |rd
      |re
      |readily
      |really
      |recent
      |recently
      |ref
      |refs
      |regarding
      |regardless
      |regards
      |related
      |relatively
      |research
      |respectively
      |resulted
      |resulting
      |results
      |right
      |run
      |s
      |said
      |same
      |saw
      |say
      |saying
      |says
      |sec
      |section
      |see
      |seeing
      |seem
      |seemed
      |seeming
      |seems
      |seen
      |self
      |selves
      |sent
      |seven
      |several
      |shall
      |she
      |shed
      |she'll
      |shes
      |should
      |shouldn't
      |show
      |showed
      |shown
      |showns
      |shows
      |significant
      |significantly
      |similar
      |similarly
      |since
      |six
      |slightly
      |so
      |some
      |somebody
      |somehow
      |someone
      |somethan
      |something
      |sometime
      |sometimes
      |somewhat
      |somewhere
      |soon
      |sorry
      |specifically
      |specified
      |specify
      |specifying
      |still
      |stop
      |strongly
      |sub
      |substantially
      |successfully
      |such
      |sufficiently
      |suggest
      |sup
      |sure
      |t's    take    taken    tell    tends
      |th    than    thank    thanks    thanx
      |that    that's    thats    the    their
      |theirs    them    themselves    then    thence
      |there    there's    thereafter    thereby    therefore
      |therein    theres    thereupon    these    they
      |they'd    they'll    they're    they've    think
      |third    this    thorough    thoroughly    those
      |though    three    through    throughout    thru
      |thus    to    together    too    took
      |toward    towards    tried    tries    truly
      |try    trying    twice    two    un
      |under    unfortunately    unless    unlikely    until
      |unto    up    upon    us    use
      |used    useful    uses    using    usually
      |value    various    very    via    viz
      |vs    want    wants    was    wasn't
      |way    we    we'd    we'll    we're
      |we've    welcome    well    went    were
      |weren't    what    what's    whatever    when
      |whence    whenever    where    where's    whereafter
      |whereas    whereby    wherein    whereupon    wherever
      |whether    which    while    whither    who
      |who's    whoever    whole    whom    whose
      |why    will    willing    wish    with
      |within    without    won't    wonder    would
      |wouldn't    yes    yet    you    you'd
      |you'll    you're    you've    your    yours
      |yourself    yourselves    zero
    """.stripMargin.split("\\s+").toSet

}
