package io.findify.s3mock.alpakka

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.{S3Attributes, S3Ext}
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.jdk.CollectionConverters._

object AlpakkaExample {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseMap(
      Map(
        "alpakka.s3.proxy.host" -> "localhost",
        "alpakka.s3.proxy.port" -> 8001,
        "alpakka.s3.proxy.secure" -> false,
        "alpakka.s3.path-style-access" -> true,
        "alpakka.s3.aws.credentials.provider" -> "static",
        "alpakka.s3.aws.credentials.access-key-id" -> "foo",
        "alpakka.s3.aws.credentials.secret-access-key" -> "bar",
        "alpakka.s3.aws.region.provider" -> "static",
        "alpakka.s3.aws.region.default-region" -> "us-east-1"
      ).asJava
    )
    implicit val system = ActorSystem("test", config)
    import system.dispatcher
    val contents = S3
      .download("bucket", "key")
      .withAttributes(S3Attributes.settings(S3Ext(system).settings))
      .flatMapConcat(_.map(_._1).getOrElse(Source.empty))
      .runWith(Sink.reduce[ByteString](_ ++ _))
      .map(_.utf8String)
  }
}
