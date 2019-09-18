/**
  * Copyright (C) 2015, Atgenomix Incorporated. All Rights Reserved.
  * This program is an unpublished copyrighted work which is proprietary to
  * Atgenomix Incorporated and contains confidential information that is not to
  * be reproduced or disclosed to any other person or entity without prior
  * written consent from Atgenomix, Inc. in each and every instance.
  * Unauthorized reproduction of this program as well as unauthorized
  * preparation of derivative works based upon the program or distribution of
  * copies by sale, rental, lease or lending are violations of federal copyright
  * laws and state trade secret laws, punishable by civil and criminal penalties.
  */

package com.atgenomix.connectedreads.cli

import javax.inject.Inject

object GraphSeqMain {
  val defaultCommandGroups =
    List(
      CommandGroup(
        "PREPROCESSING OPERATIONS",
        List(
//          Fasta2GDF,
          StringGraphDriver,
          ErrorCorrectionDriver //,
//          SuffixIndexDriver
        )
      ),
      CommandGroup(
        "ASSEMBLY OPERATIONS",
        List(
          AssemblyDriver
        )
      )
    )

  def main(args: Array[String]) {
    new GraphSeqMain(defaultCommandGroups)(args)
  }
}

case class CommandGroup(name: String, commands: List[CommandCompanion])

class GraphSeqMain @Inject()(commandGroups: List[CommandGroup]) {
  def apply(args: Array[String]) {
    if (args.length < 1) {
      printCommands()
    } else {
      val commands =
        for {
          grp <- commandGroups
          cmd <- grp.commands
        } yield cmd

      commands.find(_.commandName == args(0)) match {
        case None => printCommands()
        case Some(cmd) => cmd(args drop 1).run()
      }
    }
  }

  private def printCommands() {
    println("\nUsage: connectedreads-submit [<spark-args> --] <annot-args>")
    println("\nChoose one of the following commands:")
    commandGroups.foreach { grp =>
      println("\n%s".format(grp.name))
      grp.commands.foreach(cmd =>
        println("%20s : %s".format(cmd.commandName, cmd.commandDescription)))
    }
    println("\n")
  }
}

trait GraphSeqArgsBase {
  // argument default value
  var assignN: Boolean
  var input: String
  var minlcp: Int
  var maxrlen: Int
  var maxNCount: Int
  var output: String
  var pl: Short
  var pl2: Short
  var packing_size: Int
  var stats: Boolean
  var profiling: Boolean
}