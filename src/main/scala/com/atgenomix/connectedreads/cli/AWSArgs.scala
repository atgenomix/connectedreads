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

import org.bdgenomics.utils.cli.Args4jBase
import org.kohsuke.args4j.Option

trait AWSArgs extends Args4jBase {
  @Option(required = false, name = "-aws_access_key_id", usage = "AWS access key")
  var awsAccessKeyId: String = _

  @Option(required = false, name = "-aws_secret_access_key", usage = "AWS secret key")
  var awsSecretAccessKey: String = _
}
