/**
  * Licensed to Big Data Genomics (BDG) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The BDG licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.bdgenomics.utils.cli

import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option, ParserProperties}

import scala.collection.JavaConversions._

class Args4jBase {
  @Option(name = "-h", aliases = Array("-help", "--help", "-?"), usage = "Print help")
  var doPrintUsage: Boolean = false
  @Option(name = "-print_metrics", usage = "Print metrics to the log on completion")
  var printMetrics: Boolean = false
}

object Args4j {
  val helpOptions = Array("-h", "-help", "--help", "-?")

  def apply[T](args: Array[String], ignoreCmdLineExceptions: Boolean = false)(implicit ev$1: T => Args4jBase, ev$2: Manifest[T]): T = {
    val args4j: T = manifest[T].runtimeClass.asInstanceOf[Class[T]].newInstance()
    val parser = new CmdLineParser(args4j, ParserProperties.defaults().withUsageWidth(150))

    def displayHelp(exitCode: Int = 0) = {
      parser.printUsage(System.out)
      System.exit(exitCode)
    }

    try {
      parser.parseArgument(args.toList)
      if (!ignoreCmdLineExceptions && args4j.doPrintUsage) {
        displayHelp()
      }
    } catch {
      case e: CmdLineException =>
        if (!ignoreCmdLineExceptions) {
          if (args4j.doPrintUsage) {
            displayHelp()
          } else {
            println(e.getMessage)
            displayHelp(1)
          }
        }
    }

    args4j
  }

}