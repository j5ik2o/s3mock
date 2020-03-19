package io.findify.s3mock.alpakka

import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import io.findify.s3mock.S3MockTest

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by shutty on 5/19/17.
  */
class GetObjectTest extends S3MockTest {

  override def behaviour(fixture: => Fixture): Unit = {
    val s3 = fixture.client
    implicit val sys = fixture.system

    it should "get objects via alpakka" in {
      s3.createBucket("alpakka1")
      s3.putObject("alpakka1", "test1", "foobar")
      val result = Await.result(
        S3.download("alpakka1", "test1")
          .withAttributes(S3Attributes.settings(fixture.alpakka))
          .flatMapConcat(_.map(_._1).getOrElse(Source.empty))
          .runWith(Sink.seq),
        5.second
      )
      val str = result.fold(ByteString(""))(_ ++ _).utf8String
      str shouldBe "foobar"
    }

    it should "get by range" in {
      s3.createBucket("alpakka2")
      s3.putObject("alpakka2", "test2", "foobar")
      val result = Await.result(
        S3.download("alpakka2", "test2", Some(ByteRange(1, 4)))
          .withAttributes(S3Attributes.settings(fixture.alpakka))
          .flatMapConcat(_.map(_._1).getOrElse(Source.empty))
          .runWith(Sink.seq),
        5.second
      )
      val str = result.fold(ByteString(""))(_ ++ _).utf8String
      str shouldBe "ooba"
    }
  }
}
