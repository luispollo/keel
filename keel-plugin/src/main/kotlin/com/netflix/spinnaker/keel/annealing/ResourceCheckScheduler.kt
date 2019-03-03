package com.netflix.spinnaker.keel.annealing

import com.netflix.spinnaker.keel.persistence.ResourceRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class ResourceCheckScheduler(
  private val resourceRepository: ResourceRepository,
  private val resourceCheckQueue: ResourceCheckQueue
) {

  @Scheduled(fixedDelayString = "\${keel.resource.monitoring.frequency.ms:60000}")
  fun checkManagedResources() {
    log.debug("Starting scheduled validation…")
    resourceRepository.allResources { (_, name, _, apiVersion, kind) ->
      resourceCheckQueue.scheduleCheck(name, apiVersion, kind)
    }
    log.debug("Scheduled validation complete")
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}
