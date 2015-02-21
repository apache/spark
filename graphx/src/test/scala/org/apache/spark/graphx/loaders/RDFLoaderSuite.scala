/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.graphx.loaders
import java.io.{ File, OutputStreamWriter, FileOutputStream }
import org.apache.spark.util.Utils
import org.scalatest.FunSuite
import org.apache.spark.graphx.LocalSparkContext
import scala.io.Source

class RDFLoaderSuite extends FunSuite with LocalSparkContext {
	test("RDFLoader.loadNTriples") {
		withSpark { sc =>
			val file = getClass.getResource("/graph.nt").getFile
			val graph = RDFLoader.loadNTriples(sc, file)
			val edges = graph.edges.collect()
			assert(edges.length == 17)
			val vertices = graph.vertices.collect()
			assert(vertices.length == 19)
			val triples = graph.mapTriplets(triplet => {
					triplet.srcAttr + "<" + triplet.attr + ">" + triplet.dstAttr
				}
			).edges.collect
			val propregex = "<([^>]+)>\\s<([^>]+)>\\s(.+)\\s\\.".r
			val relregex = "<([^>]+)>\\s<([^>]+)>\\s<([^>]+)>\\s\\.".r
			
			
			// for (triple <- triples) Console.println(triple.attr)
			
			def assertline(a:String, b:String, c:String) = {
				// find corresponding line from triples from graph
				var found = false;
				for (triple <- triples) {
					if (triple.attr == a + "<" + b + ">" + c) {
						found = true
					}
				}
				if (!found) Console.println(a,b,c)
				assert(found)
			}
			
			for (line <- Source.fromFile(file).getLines) {
				line match {
					case relregex(a,b,c) => assertline(a,b,c)
					case propregex(a,b,c) => assertline(a,b,c)
					case _ =>
				}
			}
		}
	}
}