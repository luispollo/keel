package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.parseUID
import com.netflix.spinnaker.keel.echo.model.EchoNotification
import com.netflix.spinnaker.keel.exceptions.InvalidConstraintException
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.yaml.APPLICATION_YAML_VALUE
import com.netflix.spinnaker.kork.web.exceptions.InvalidRequestException
import java.time.Instant
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RestController

@RestController
class InteractiveNotificationCallbackController(
  private val deliveryConfigRepository: DeliveryConfigRepository,
  private val artifactController: ArtifactController
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @PostMapping(
    path = ["/notifications/callback"],
    consumes = [MediaType.APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE],
    produces = [MediaType.APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  // TODO: This should be validated against write access to a service account. Service accounts should
  //  become a top-level property of either delivery configs or environments.
  fun handleInteractionCallback(
    @RequestHeader("X-SPINNAKER-USER") user: String,
    @RequestBody callback: EchoNotification.InteractiveActionCallback
  ) {
    val currentState = deliveryConfigRepository.getConstraintStateById(parseUID(callback.messageId))
      ?: throw InvalidConstraintException("constraint@callbackId=${callback.messageId}", "constraint not found")

    log.debug("Updating constraint status based on notification interaction: " +
      "user = $user, status = ${callback.actionPerformed.value}")

    deliveryConfigRepository.storeConstraintState(
      currentState.copy(
        status = ConstraintStatus.valueOf(callback.actionPerformed.value),
        judgedAt = Instant.now(),
        judgedBy = user
      )
    )
  }

  @PostMapping(
    path = ["/commands/callback"],
    consumes = [MediaType.APPLICATION_JSON_VALUE],
    produces = [MediaType.TEXT_PLAIN_VALUE]
  )
  fun handleCommandCallback(
    @RequestHeader("X-SPINNAKER-USER") user: String,
    @RequestBody callback: EchoNotification.CommandCallback
  ): String {
    val args = callback.arguments.split(" ")

    if (args.size < 2) {
      throw InvalidRequestException("Not enough arguments")
    }

    return when (args[1]) {
      "pin" -> handlePinCommand(user, args)
      else -> throw InvalidRequestException("Unrecognized command $args[1]")
    }
  }

  private fun handlePinCommand(user: String, args: List<String>): String {
    if (args.size < 7) {
      throw InvalidRequestException("Not enough arguments")
    }

    val pin = EnvironmentArtifactPin(
      deliveryConfigName = args[2],
      targetEnvironment = args[3],
      reference = args[4],
      type = args[5],
      version = args[6],
      pinnedBy = user,
      comment = "Pinned by $user via interactive command."
    )

    artifactController.pin(user, pin)

    return "User ${pin.pinnedBy} pinned artifact *${pin.type}:${pin.version}* in environment *${pin.targetEnvironment}* of delivery config *${pin.deliveryConfigName}*"
  }
}
