package io.findify.s3mock

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.S3Settings
import better.files.File
import com.amazonaws.auth.{
  AWSStaticCredentialsProvider,
  AnonymousAWSCredentials
}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.transfer.{
  TransferManager,
  TransferManagerBuilder
}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.config.{Config, ConfigFactory}
import io.findify.s3mock.provider.{FileProvider, InMemoryProvider}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.Source

/**
  * Created by shutty on 8/9/16.
  */
trait S3MockTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val workDir = File.newTemporaryDirectory().pathAsString
  private val fileBasedPort = 8001
  private val fileSystemConfig = configFor("localhost", fileBasedPort)
  private val fileSystem = ActorSystem.create("testfile", fileSystemConfig)
  private val fileBasedS3 = clientFor("localhost", fileBasedPort)
  private val fileBasedServer =
    new S3Mock(fileBasedPort, new FileProvider(workDir))
  private val fileBasedTransferManager: TransferManager =
    TransferManagerBuilder.standard().withS3Client(fileBasedS3).build()
  private val fileBasedAlpakkaSettings =
    S3Settings(fileSystemConfig)

  private val inMemoryPort = 8002
  private val inMemoryConfig = configFor("localhost", inMemoryPort)
  private val inMemorySystem = ActorSystem.create("testram", inMemoryConfig)
  private val inMemoryS3 = clientFor("localhost", inMemoryPort)
  private val inMemoryServer = new S3Mock(inMemoryPort, new InMemoryProvider)
  private val inMemoryTransferManager: TransferManager =
    TransferManagerBuilder.standard().withS3Client(inMemoryS3).build()
  private val inMemoryBasedAlpakkaSettings = S3Settings(inMemoryConfig)

  case class Fixture(server: S3Mock,
                     client: AmazonS3,
                     tm: TransferManager,
                     name: String,
                     port: Int,
                     alpakka: S3Settings,
                     system: ActorSystem)

  val fixtures = List(
    Fixture(
      fileBasedServer,
      fileBasedS3,
      fileBasedTransferManager,
      "file based S3Mock",
      fileBasedPort,
      fileBasedAlpakkaSettings,
      fileSystem,
    ),
    Fixture(
      inMemoryServer,
      inMemoryS3,
      inMemoryTransferManager,
      "in-memory S3Mock",
      inMemoryPort,
      inMemoryBasedAlpakkaSettings,
      inMemorySystem,
    )
  )

  def behaviour(fixture: => Fixture): Unit

  for (fixture <- fixtures) {
    fixture.name should behave like behaviour(fixture)
  }

  override def beforeAll = {
    if (!File(workDir).exists) File(workDir).createDirectory()
    fileBasedServer.start
    inMemoryServer.start
    super.beforeAll
  }
  override def afterAll = {
    super.afterAll
    inMemoryServer.stop
    fileBasedServer.stop
    inMemoryTransferManager.shutdownNow()
    Await.result(fileSystem.terminate(), Duration.Inf)
    Await.result(inMemorySystem.terminate(), Duration.Inf)
    File(workDir).delete()
  }
  def getContent(s3Object: S3Object): String =
    Source.fromInputStream(s3Object.getObjectContent, "UTF-8").mkString

  def clientFor(host: String, port: Int): AmazonS3 = {
    val endpoint = new EndpointConfiguration(s"http://$host:$port", "us-east-1")
    AmazonS3ClientBuilder
      .standard()
      .withPathStyleAccessEnabled(true)
      .withCredentials(
        new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())
      )
      .withEndpointConfiguration(endpoint)
      .build()
  }

  def configFor(host: String, port: Int): Config = {
    ConfigFactory.parseMap(
      Map(
        "proxy.host" -> host,
        "proxy.port" -> port,
        "proxy.secure" -> false,
        "path-style-access" -> true,
        "aws.credentials.provider" -> "static",
        "aws.credentials.access-key-id" -> "foo",
        "aws.credentials.secret-access-key" -> "bar",
        "aws.region.provider" -> "static",
        "aws.region.default-region" -> "us-east-1",
        "buffer" -> "memory",
        "disk-buffer-path" -> ""
      ).asJava
    )

  }

}
