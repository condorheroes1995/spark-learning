package kit

import org.slf4j.LoggerFactory

trait Logging {
  @transient
  protected val logger = LoggerFactory.getLogger(getClass().getName())
}
