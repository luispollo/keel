package com.netflix.spinnaker.keel.artifact

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.BRANCH_JOB_COMMIT_BY_JOB
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_JOB_COMMIT_BY_JOB
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_JOB_COMMIT_BY_SEMVER
import com.netflix.spinnaker.keel.api.artifacts.VersioningStrategy
import com.netflix.spinnaker.keel.api.plugins.ArtifactPublisher
import com.netflix.spinnaker.keel.api.plugins.SupportedArtifact
import com.netflix.spinnaker.keel.api.plugins.SupportedVersioningStrategy
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.artifacts.DOCKER
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.artifacts.DockerVersioningStrategy
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import org.springframework.stereotype.Component

/**
 * Built-in keel implementation of [ArtifactPublisher] that does not itself receive/retrieve artifact information
 * but is used by keel's `POST /artifacts/events` API to notify the core of new Docker artifacts.
 */
@Component
class DockerArtifactPublisher(
  override val eventPublisher: EventPublisher,
  private val cloudDriverService: CloudDriverService
) : ArtifactPublisher<DockerArtifact> {
  override val supportedArtifact = SupportedArtifact("docker", DockerArtifact::class.java)

  override val supportedVersioningStrategies: List<SupportedVersioningStrategy<*>>
    get() = listOf(
      SupportedVersioningStrategy("docker", DockerVersioningStrategy::class.java)
    )

  override suspend fun getLatestArtifact(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact): PublishedArtifact? {
    if (artifact !is DockerArtifact) {
      throw IllegalArgumentException("Only Docker artifacts are supported by this implementation.")
    }

    val latestTag = cloudDriverService
      .findDockerTagsForImage("*", artifact.name, deliveryConfig.serviceAccount)
      .distinct()
      .sortedWith(artifact.versioningStrategy.comparator)
      .firstOrNull()

    return if (latestTag != null) {
      // TODO: do we need to look in a specific account? if so, then DockerArtifact should have an
      // `account` property. In retrospect, it feels weird that we specify images without a repository
      // URL (from which we could infer the account), or an account.
      cloudDriverService.findDockerImages(account = "*", repository = artifact.name, tag = latestTag)
        .firstOrNull()
        ?.let { dockerImage ->
          PublishedArtifact(
            name = dockerImage.repository,
            type = DOCKER,
            reference = dockerImage.repository.substringAfter(':', dockerImage.repository),
            version = dockerImage.tag
          )
        }
    } else {
      null
    }
  }

  override fun getBuildMetadata(artifact: PublishedArtifact, versioningStrategy: VersioningStrategy): BuildMetadata? {
    if (versioningStrategy.hasBuild()) {
      // todo eb: this could be less brittle
      val regex = Regex("""^.*-h(\d+).*$""")
      val result = regex.find(artifact.version)
      if (result != null && result.groupValues.size == 2) {
        return BuildMetadata(id = result.groupValues[1].toInt())
      }
    }
    return null
  }

  override fun getGitMetadata(artifact: PublishedArtifact, versioningStrategy: VersioningStrategy): GitMetadata? {
    if (versioningStrategy.hasCommit()) {
      // todo eb: this could be less brittle
      return GitMetadata(commit = artifact.version.substringAfterLast("."))
    }
    return null
  }

  private fun VersioningStrategy.hasBuild(): Boolean {
    return (this as? DockerVersioningStrategy)
      ?.let { it.strategy in listOf(BRANCH_JOB_COMMIT_BY_JOB, SEMVER_JOB_COMMIT_BY_JOB, SEMVER_JOB_COMMIT_BY_SEMVER) }
      ?: false
  }

  private fun VersioningStrategy.hasCommit(): Boolean {
    return (this as? DockerVersioningStrategy)
      ?.let { it.strategy in listOf(BRANCH_JOB_COMMIT_BY_JOB, SEMVER_JOB_COMMIT_BY_JOB, SEMVER_JOB_COMMIT_BY_SEMVER) }
      ?: false
  }
}
